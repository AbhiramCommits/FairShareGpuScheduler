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

//go:build ignore

// Command trace-gen deterministically generates testdata/trace.json.
// Run with: go run hack/trace-gen.go
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"

	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/sim"
)

func main() {
	const seed = 20240117
	rng := rand.New(rand.NewSource(seed))

	// Entitlement weights are 4:2:1:1. tenant-d is a greedy high-priority
	// tenant that submits far more demand than its share, which is exactly the
	// scenario weighted fair sharing must contain.
	tenants := []sim.Tenant{
		{Name: "tenant-a", Weight: 4, Guaranteed: 16, BorrowLimit: 32, PriorityClass: 100, Preemptible: true, Reclaimable: true},
		{Name: "tenant-b", Weight: 2, Guaranteed: 8, BorrowLimit: 32, PriorityClass: 100, Preemptible: true, Reclaimable: true},
		{Name: "tenant-c", Weight: 1, Guaranteed: 4, BorrowLimit: 32, PriorityClass: 200, Preemptible: true, Reclaimable: true},
		{Name: "tenant-d", Weight: 1, Guaranteed: 4, BorrowLimit: 32, PriorityClass: 300, Preemptible: true, Reclaimable: true},
	}

	pool := sim.Pool{TotalGPUs: 32, Nodes: 4, GPUsPerNode: 8, NVLinkGroups: 2}
	horizon := 24.0 * 3600.0

	var jobs []sim.Job
	id := 0
	add := func(tenant string, gpus int, arrival, duration float64, prio int32) {
		jobs = append(jobs, sim.Job{
			ID:       fmt.Sprintf("job-%04d", id),
			Tenant:   tenant,
			GPUs:     gpus,
			Arrival:  arrival,
			Duration: duration,
			Priority: prio,
		})
		id++
	}

	windows := []int{1, 2, 4, 8}
	windowWeights := []int{40, 30, 20, 10}
	sizePool := []int{}
	for i, w := range windowWeights {
		for j := 0; j < w; j++ {
			sizePool = append(sizePool, windows[i])
		}
	}

	// tenant-a, tenant-b, tenant-c: steady, entitled-scale demand spread over
	// the whole day.
	steady := []struct {
		name  string
		count int
		maxSize int
	}{
		{"tenant-a", 220, 4},
		{"tenant-b", 150, 4},
		{"tenant-c", 120, 4},
	}
	for _, s := range steady {
		for i := 0; i < s.count; i++ {
			size := sizePool[rng.Intn(len(sizePool))]
			if size > s.maxSize {
				size = s.maxSize
			}
			add(s.name, size, rng.Float64()*horizon, 900+rng.Float64()*2700, priorityOf(tenants, s.name))
		}
	}

	// tenant-d: a burst of long, large, high-priority jobs concentrated in the
	// first four hours. Without quota this tenant monopolises the pool.
	for i := 0; i < 360; i++ {
		add("tenant-d", 8, rng.Float64()*4*3600, 1800+rng.Float64()*1800, 300)
	}

	trace := sim.Trace{
		Seed:          seed,
		DurationHours: 24,
		Pool:          pool,
		Tenants:       tenants,
		Jobs:          jobs,
	}

	data, err := json.MarshalIndent(trace, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("testdata/trace.json", append(data, '\n'), 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("wrote testdata/trace.json: %d jobs across %d tenants\n", len(jobs), len(tenants))
}

func priorityOf(tenants []sim.Tenant, name string) int32 {
	for _, t := range tenants {
		if t.Name == name {
			return t.PriorityClass
		}
	}
	return 100
}
