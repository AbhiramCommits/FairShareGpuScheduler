/*
Copyright 2026 The FairShareGpuScheduler Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Command bench runs the deterministic multi-tenant GPU benchmark comparing
// fairshare against FIFO and default-binpack and writes results/.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/sim"
)

// Benchmark is the serialized result of a full comparison run.
type Benchmark struct {
	GeneratedBy string       `json:"generatedBy"`
	TraceFile   string       `json:"traceFile"`
	Seed        int64        `json:"seed"`
	Results     []sim.Result `json:"results"`
}

func main() {
	tracePath := "testdata/trace.json"
	raw, err := os.ReadFile(tracePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read trace: %v\n", err)
		os.Exit(1)
	}
	var trace sim.Trace
	if err := json.Unmarshal(raw, &trace); err != nil {
		fmt.Fprintf(os.Stderr, "parse trace: %v\n", err)
		os.Exit(1)
	}

	modes := []sim.Mode{sim.ModeFIFO, sim.ModeDefaultBinpack, sim.ModeFairShare}
	results := make([]sim.Result, 0, len(modes))
	for _, m := range modes {
		res := sim.Simulate(trace, sim.Options{Mode: m, StarvationThreshold: 1800})
		results = append(results, res)
	}

	bench := Benchmark{
		GeneratedBy: "cmd/bench",
		TraceFile:   tracePath,
		Seed:        trace.Seed,
		Results:     results,
	}

	if err := os.MkdirAll("results", 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir results: %v\n", err)
		os.Exit(1)
	}

	jsonBytes, err := json.MarshalIndent(bench, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal json: %v\n", err)
		os.Exit(1)
	}
	jsonBytes = append(jsonBytes, '\n')
	must(os.WriteFile(filepath.Join("results", "benchmark.json"), jsonBytes, 0o644))
	must(os.WriteFile(filepath.Join("results", "benchmark.md"), []byte(renderMarkdown(bench)), 0o644))
	must(os.WriteFile(filepath.Join("results", "summary.txt"), []byte(renderSummary(bench)), 0o644))

	fmt.Print(renderSummary(bench))
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func renderSummary(b Benchmark) string {
	out := "FairShare GPU scheduler benchmark (deterministic trace)\n"
	out += fmt.Sprintf("trace=%s seed=%d jobs=%d\n\n", b.TraceFile, b.Seed, b.Results[0].JobsTotal)
	out += fmt.Sprintf("%-18s %8s %8s %8s %8s %8s %8s %10s %7s %6s %6s\n",
		"scheduler", "alloc", "p50(s)", "p95(s)", "maxwait", "jain", "dev", "makespan", "preempt", "recl", "done")
	for _, r := range b.Results {
		out += fmt.Sprintf("%-18s %8.3f %8.1f %8.1f %8.1f %8.4f %8.4f %10.1f %7d %6d %6d\n",
			r.Scheduler, r.MeanAllocation, r.WaitP50, r.WaitP95, r.StarvationMaxWait,
			r.JainIndex, r.FairnessDeviation, r.Makespan, r.Preemptions, r.Reclaims, r.JobsCompleted)
	}
	return out
}

func renderMarkdown(b Benchmark) string {
	var fair, fifo, bin sim.Result
	for _, r := range b.Results {
		switch r.Scheduler {
		case "fairshare":
			fair = r
		case "fifo":
			fifo = r
		case "default-binpack":
			bin = r
		}
	}
	pct := func(base, v float64) string {
		if base == 0 {
			return "n/a"
		}
		return fmt.Sprintf("%+.1f%%", (v-base)/base*100)
	}

	out := "# FairShare GPU Scheduler Benchmark\n\n"
	out += fmt.Sprintf("Deterministic trace `%s` (seed `%d`), %d jobs, 32-GPU pool across 4 nodes, 24 simulated hours.\n\n",
		b.TraceFile, b.Seed, fair.JobsTotal)

	out += "## Headline metrics\n\n"
	out += "| Metric | FIFO | default-binpack | fairshare | fairshare vs FIFO | fairshare vs binpack |\n"
	out += "|---|---:|---:|---:|---:|---:|\n"
	row := func(name string, get func(sim.Result) float64, fmtv string) {
		out += fmt.Sprintf("| %s | "+fmtv+" | "+fmtv+" | "+fmtv+" | %s | %s |\n",
			name, get(fifo), get(bin), get(fair), pct(get(fifo), get(fair)), pct(get(bin), get(fair)))
	}
	row("GPU allocation ratio (mean)", func(r sim.Result) float64 { return r.MeanAllocation }, "%.3f")
	row("GPU allocation ratio (peak)", func(r sim.Result) float64 { return r.PeakAllocation }, "%.3f")
	row("Queue wait p50 (s)", func(r sim.Result) float64 { return r.WaitP50 }, "%.1f")
	row("Queue wait p95 (s)", func(r sim.Result) float64 { return r.WaitP95 }, "%.1f")
	row("Makespan (s)", func(r sim.Result) float64 { return r.Makespan }, "%.1f")
	row("Fairness deviation", func(r sim.Result) float64 { return r.FairnessDeviation }, "%.4f")
	row("Jain fairness index", func(r sim.Result) float64 { return r.JainIndex }, "%.4f")
	row("Preemptions", func(r sim.Result) float64 { return float64(r.Preemptions) }, "%.0f")
	row("Reclaims", func(r sim.Result) float64 { return float64(r.Reclaims) }, "%.0f")
	row("Starvation max wait (s)", func(r sim.Result) float64 { return r.StarvationMaxWait }, "%.1f")
	row("Jobs completed", func(r sim.Result) float64 { return float64(r.JobsCompleted) }, "%.0f")

	out += "\n## Per-tenant queue wait (fairshare)\n\n"
	out += "| Tenant | p50 (s) | p95 (s) | max (s) | jobs |\n|---|---:|---:|---:|---:|\n"
	for _, t := range []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"} {
		w, ok := fair.PerTenantWait[t]
		if !ok {
			continue
		}
		out += fmt.Sprintf("| %s | %.1f | %.1f | %.1f | %d |\n", t, w.P50, w.P95, w.Max, w.Jobs)
	}

	out += "\n## Tenant GPU-second share vs weighted entitlement\n\n"
	out += "| Tenant | entitled | FIFO | default-binpack | fairshare |\n|---|---:|---:|---:|---:|\n"
	ent := map[string]string{"tenant-a": "50.0%", "tenant-b": "25.0%", "tenant-c": "12.5%", "tenant-d": "12.5%"}
	for _, t := range []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"} {
		out += fmt.Sprintf("| %s | %s | %.1f%% | %.1f%% | %.1f%% |\n",
			t, ent[t], fifo.TenantShare[t]*100, bin.TenantShare[t]*100, fair.TenantShare[t]*100)
	}
	return out
}
