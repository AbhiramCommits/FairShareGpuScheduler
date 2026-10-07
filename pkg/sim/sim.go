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

package sim

import (
	"sort"

	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/fairshare"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/topology"
)

// Mode selects one of the benchmarked scheduling policies.
type Mode string

const (
	// ModeFIFO is the baseline: single queue, strict arrival order, no quota.
	ModeFIFO Mode = "fifo"
	// ModeDefaultBinpack approximates the default scheduler: no quota, no
	// preemption, pack tightly onto the fullest node that still fits.
	ModeDefaultBinpack Mode = "default-binpack"
	// ModeFairShare is the real policy: hierarchical quota, weighted DRF,
	// borrowing and guarantee-safe preemption/reclaim.
	ModeFairShare Mode = "fairshare"
)

// Options configures a simulation run.
type Options struct {
	Mode                Mode
	StarvationThreshold float64
	MaxEvents           int
}

type node struct {
	name        string
	total       int
	allocated   int
	nvlinkGroup string
	numaNode    string
}

func (n *node) free() int { return n.total - n.allocated }

type running struct {
	job    *Job
	node   *node
	start  float64
	finish float64
}

type pending struct {
	job     *Job
	arrival float64
}

// Result holds all metrics computed from a run.
type Result struct {
	Scheduler             string                `json:"scheduler"`
	JobsTotal             int                   `json:"jobsTotal"`
	JobsCompleted         int                   `json:"jobsCompleted"`
	MeanAllocation        float64               `json:"gpuAllocationRatioMean"`
	PeakAllocation        float64               `json:"gpuAllocationRatioPeak"`
	WaitP50               float64               `json:"waitP50Seconds"`
	WaitP95               float64               `json:"waitP95Seconds"`
	PerTenantWait         map[string]TenantWait `json:"perTenantWait"`
	Makespan              float64               `json:"makespanSeconds"`
	FairnessDeviation     float64               `json:"fairnessDeviation"`
	FairnessDeviationPeak float64               `json:"fairnessDeviationPeak"`
	JainIndex             float64               `json:"jainIndex"`
	TenantShare           map[string]float64    `json:"tenantShare"`
	Preemptions           int                   `json:"preemptions"`
	Reclaims              int                   `json:"reclaims"`
	StarvationMaxWait     float64               `json:"starvationMaxWaitSeconds"`
	LowestPriorityTenant  string                `json:"lowestPriorityTenant"`
}

// TenantWait reports per-tenant queue wait statistics.
type TenantWait struct {
	P50  float64 `json:"p50Seconds"`
	P95  float64 `json:"p95Seconds"`
	Max  float64 `json:"maxSeconds"`
	Jobs int     `json:"jobs"`
}

// Simulate runs the trace once under the given options and returns metrics.
func Simulate(trace Trace, opts Options) Result {
	if opts.MaxEvents <= 0 {
		opts.MaxEvents = 5_000_000
	}
	nodes := makeNodes(trace.Pool)
	tenantAlloc := map[string]float64{}
	tenantGpuSeconds := map[string]float64{}
	waitsByTenant := map[string][]float64{}

	arrivals := make([]Job, len(trace.Jobs))
	copy(arrivals, trace.Jobs)
	sort.SliceStable(arrivals, func(i, j int) bool {
		if arrivals[i].Arrival != arrivals[j].Arrival {
			return arrivals[i].Arrival < arrivals[j].Arrival
		}
		return arrivals[i].ID < arrivals[j].ID
	})

	var runnings []running
	var pendings []pending
	next := 0
	firstArrival := 0.0
	if len(arrivals) > 0 {
		firstArrival = arrivals[0].Arrival
	}
	now := firstArrival
	lastCompletion := firstArrival
	completedJobs := 0

	areaAlloc := 0.0
	areaTime := 0.0
	peakAlloc := 0.0
	lastSample := now
	totalGPUs := float64(trace.Pool.TotalGPUs)
	horizon := trace.DurationHours * 3600
	if horizon <= 0 {
		horizon = 1e18
	}

	// Instantaneous fairness: the peak deviation of a tenant's allocated share
	// from its weighted entitlement, sampled over the whole run. This captures
	// hogging during contention, which cumulative share hides.
	totalWeight := int64(0)
	for _, t := range trace.Tenants {
		totalWeight += t.Weight
	}
	entitled := map[string]float64{}
	if totalWeight > 0 {
		for _, t := range trace.Tenants {
			entitled[t.Name] = float64(t.Weight) / float64(totalWeight)
		}
	}
	peakDeviation := 0.0

	preemptions, reclaims := 0, 0
	events := 0

	sample := func(at float64) {
		dt := at - lastSample
		if dt <= 0 {
			return
		}
		alloc := 0
		for _, n := range nodes {
			alloc += n.allocated
		}
		ratio := 0.0
		if totalGPUs > 0 {
			ratio = float64(alloc) / totalGPUs
		}
		areaAlloc += ratio * dt
		areaTime += dt
		if ratio > peakAlloc {
			peakAlloc = ratio
		}
		for t, g := range tenantAlloc {
			tenantGpuSeconds[t] += g * dt
		}
		if totalGPUs > 0 {
			for t, e := range entitled {
				inst := tenantAlloc[t] / totalGPUs
				d := inst - e
				if d < 0 {
					d = -d
				}
				if d > peakDeviation {
					peakDeviation = d
				}
			}
		}
		lastSample = at
	}

	for events < opts.MaxEvents {
		events++
		sort.SliceStable(runnings, func(i, j int) bool { return runnings[i].finish < runnings[j].finish })

		nextArrival := -1.0
		if next < len(arrivals) {
			nextArrival = arrivals[next].Arrival
		}
		nextFinish := -1.0
		if len(runnings) > 0 {
			nextFinish = runnings[0].finish
		}
		if nextArrival < 0 && nextFinish < 0 {
			break
		}

		var nextTime float64
		switch {
		case nextArrival >= 0 && (nextFinish < 0 || nextArrival <= nextFinish):
			nextTime = nextArrival
		case nextFinish >= 0:
			nextTime = nextFinish
		default:
			nextTime = now
		}
		if nextTime > horizon {
			now = horizon
			sample(now)
			break
		}
		now = nextTime
		sample(now)

		remaining := runnings[:0]
		for _, r := range runnings {
			if r.finish <= now+1e-9 {
				r.node.allocated -= r.job.GPUs
				tenantAlloc[r.job.Tenant] -= float64(r.job.GPUs)
				completedJobs++
				if r.finish > lastCompletion {
					lastCompletion = r.finish
				}
			} else {
				remaining = append(remaining, r)
			}
		}
		runnings = remaining

		for next < len(arrivals) && arrivals[next].Arrival <= now+1e-9 {
			j := arrivals[next]
			pendings = append(pendings, pending{job: &j, arrival: now})
			next++
		}

		switch opts.Mode {
		case ModeFIFO:
			scheduleFIFO(&pendings, &runnings, nodes, now, tenantAlloc, waitsByTenant)
		case ModeDefaultBinpack:
			scheduleBinpack(&pendings, &runnings, nodes, now, tenantAlloc, waitsByTenant)
		case ModeFairShare:
			scheduleFairShare(&pendings, &runnings, nodes, now, trace, tenantAlloc, opts, waitsByTenant, &preemptions, &reclaims)
		}
	}

	for _, r := range runnings {
		if r.finish > lastCompletion {
			lastCompletion = r.finish
		}
	}
	makespan := lastCompletion - firstArrival
	if makespan < 0 {
		makespan = 0
	}

	waitP50, waitP95 := percentileOf(allWaits(waitsByTenant))
	meanAlloc := 0.0
	if areaTime > 0 {
		meanAlloc = areaAlloc / areaTime
	}
	dev, jain, shares := fairnessDetailed(trace, tenantGpuSeconds)
	lowestTenant, starveMax := starvation(trace, waitsByTenant, pendings, now)

	return Result{
		Scheduler:             string(opts.Mode),
		JobsTotal:             len(trace.Jobs),
		JobsCompleted:         completedJobs,
		MeanAllocation:        meanAlloc,
		PeakAllocation:        peakAlloc,
		WaitP50:               waitP50,
		WaitP95:               waitP95,
		PerTenantWait:         summarizeWaits(waitsByTenant),
		Makespan:              makespan,
		FairnessDeviation:     dev,
		FairnessDeviationPeak: peakDeviation,
		JainIndex:             jain,
		TenantShare:           shares,
		Preemptions:           preemptions,
		Reclaims:              reclaims,
		StarvationMaxWait:     starveMax,
		LowestPriorityTenant:  lowestTenant,
	}
}

func makeNodes(p Pool) []*node {
	if p.Nodes <= 0 || p.GPUsPerNode <= 0 {
		return nil
	}
	nodes := make([]*node, 0, p.Nodes)
	groups := p.NVLinkGroups
	if groups <= 0 {
		groups = 1
	}
	for i := 0; i < p.Nodes; i++ {
		nodes = append(nodes, &node{
			name:        nodeName(i),
			total:       p.GPUsPerNode,
			nvlinkGroup: "group-" + itoa(i%groups),
			numaNode:    itoa(i % 2),
		})
	}
	return nodes
}

func placeFirstFit(nodes []*node, gpus int) *node {
	for _, n := range nodes {
		if n.free() >= gpus {
			return n
		}
	}
	return nil
}

func placeBinpack(nodes []*node, gpus int) *node {
	var best *node
	for _, n := range nodes {
		if n.free() < gpus {
			continue
		}
		if best == nil || n.allocated > best.allocated ||
			(n.allocated == best.allocated && n.name < best.name) {
			best = n
		}
	}
	return best
}

func placeTopology(nodes []*node, gpus int) *node {
	var best *node
	bestScore := int64(-1)
	for _, n := range nodes {
		if n.free() < gpus {
			continue
		}
		s := topology.ScoreNode(gpus, topology.NodeGPUState{
			NodeName:    n.name,
			FreeGPUs:    n.free(),
			TotalGPUs:   n.total,
			NVLinkGroup: n.nvlinkGroup,
			NUMANode:    n.numaNode,
		})
		// Among equally-scored nodes, prefer the fullest (binpack) so that
		// whole nodes stay free for large multi-GPU jobs.
		if s > bestScore ||
			(s == bestScore && best != nil && n.allocated > best.allocated) ||
			(s == bestScore && best != nil && n.allocated == best.allocated && n.name < best.name) ||
			(s == bestScore && best == nil) {
			bestScore = s
			best = n
		}
	}
	return best
}

func startJob(p pending, n *node, now float64, runnings *[]running, tenantAlloc map[string]float64, waits map[string][]float64) {
	n.allocated += p.job.GPUs
	tenantAlloc[p.job.Tenant] += float64(p.job.GPUs)
	waits[p.job.Tenant] = append(waits[p.job.Tenant], now-p.arrival)
	*runnings = append(*runnings, running{job: p.job, node: n, start: now, finish: now + p.job.Duration})
}

func scheduleFIFO(pendings *[]pending, runnings *[]running, nodes []*node, now float64, tenantAlloc map[string]float64, waits map[string][]float64) {
	list := *pendings
	remaining := make([]pending, 0, len(list))
	for _, p := range list {
		n := placeFirstFit(nodes, p.job.GPUs)
		if n == nil {
			remaining = append(remaining, p)
			continue
		}
		startJob(p, n, now, runnings, tenantAlloc, waits)
	}
	*pendings = remaining
}

func scheduleBinpack(pendings *[]pending, runnings *[]running, nodes []*node, now float64, tenantAlloc map[string]float64, waits map[string][]float64) {
	list := *pendings
	sort.SliceStable(list, func(i, j int) bool {
		if list[i].job.Priority != list[j].job.Priority {
			return list[i].job.Priority > list[j].job.Priority
		}
		return list[i].arrival < list[j].arrival
	})
	remaining := make([]pending, 0, len(list))
	for _, p := range list {
		n := placeBinpack(nodes, p.job.GPUs)
		if n == nil {
			remaining = append(remaining, p)
			continue
		}
		startJob(p, n, now, runnings, tenantAlloc, waits)
	}
	*pendings = remaining
}

func scheduleFairShare(pendings *[]pending, runnings *[]running, nodes []*node, now float64, trace Trace, tenantAlloc map[string]float64, opts Options, waits map[string][]float64, preemptions, reclaims *int) {
	totalWeight := int64(0)
	for _, t := range trace.Tenants {
		totalWeight += t.Weight
	}
	specs := make([]fairshare.QueueSpec, 0, len(trace.Tenants)+1)
	specs = append(specs, fairshare.QueueSpec{
		Name:       "root",
		Weight:     totalWeight,
		Guaranteed: fairshare.ResourceVec{"nvidia.com/gpu": float64(trace.Pool.TotalGPUs)},
	})
	for _, t := range trace.Tenants {
		specs = append(specs, fairshare.QueueSpec{
			Name:        t.Name,
			Parent:      "root",
			Weight:      t.Weight,
			Guaranteed:  fairshare.ResourceVec{"nvidia.com/gpu": t.Guaranteed},
			BorrowLimit: fairshare.ResourceVec{"nvidia.com/gpu": t.BorrowLimit},
			Preemptible: t.Preemptible,
			Reclaimable: t.Reclaimable,
		})
	}
	tree, err := fairshare.BuildTree(specs)
	if err != nil {
		scheduleFIFO(pendings, runnings, nodes, now, tenantAlloc, waits)
		return
	}
	capacity := fairshare.ResourceVec{"nvidia.com/gpu": float64(trace.Pool.TotalGPUs)}

	refresh := func() {
		for _, t := range trace.Tenants {
			if q := fairshareFind(tree, t.Name); q != nil {
				q.Allocated = fairshare.ResourceVec{"nvidia.com/gpu": tenantAlloc[t.Name]}
			}
		}
	}
	refresh()
	ranked := fairshare.RankQueues(tree, capacity)

	progress := true
	for progress && len(*pendings) > 0 {
		progress = false
		for _, qName := range ranked {
			if qName == "root" {
				continue
			}
			q := fairshareFind(tree, qName)
			if q == nil {
				continue
			}
			// Walk the queue's pending jobs in arrival order and admit the
			// first one that fits, so an oversized head job never blocks the
			// rest of the queue (no head-of-line blocking).
			admitted := false
			for _, idx := range jobsOf(*pendings, qName) {
				p := (*pendings)[idx]
				q.Allocated = fairshare.ResourceVec{"nvidia.com/gpu": tenantAlloc[qName]}
				if ok, _, _ := fairshare.CanAdmit(q, fairshare.ResourceVec{"nvidia.com/gpu": float64(p.job.GPUs)}); !ok {
					continue
				}
				n := placeTopology(nodes, p.job.GPUs)
				if n == nil {
					continue
				}
				startJob(p, n, now, runnings, tenantAlloc, waits)
				*pendings = append((*pendings)[:idx], (*pendings)[idx+1:]...)
				refresh()
				ranked = fairshare.RankQueues(tree, capacity)
				progress = true
				admitted = true
				break
			}
			if admitted {
				break
			}
		}
	}

	preemptLoop(pendings, runnings, nodes, now, trace, tenantAlloc, opts, waits, preemptions, reclaims)
}

func preemptLoop(pendings *[]pending, runnings *[]running, nodes []*node, now float64, trace Trace, tenantAlloc map[string]float64, opts Options, waits map[string][]float64, preemptions, reclaims *int) {
	if len(*pendings) == 0 {
		return
	}
	threshold := opts.StarvationThreshold
	if threshold <= 0 {
		threshold = 300
	}
	best := 0
	for i := range *pendings {
		if (*pendings)[i].arrival < (*pendings)[best].arrival {
			best = i
		}
	}
	p := (*pendings)[best]
	if now-p.arrival < threshold {
		return
	}
	requester := tenantOf(trace, p.job.Tenant)
	if requester == nil {
		return
	}
	// Never preempt for a job that can never be admitted under its own quota.
	if float64(p.job.GPUs) > requester.Guaranteed+requester.BorrowLimit {
		return
	}

	type cand struct {
		r     running
		alloc float64
		guar  float64
	}
	var cands []cand
	for _, r := range *runnings {
		t := tenantOf(trace, r.job.Tenant)
		if t == nil || !t.Preemptible {
			continue
		}
		if tenantAlloc[r.job.Tenant] <= t.Guaranteed {
			continue
		}
		cands = append(cands, cand{r: r, alloc: tenantAlloc[r.job.Tenant], guar: t.Guaranteed})
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].r.job.Priority != cands[j].r.job.Priority {
			return cands[i].r.job.Priority < cands[j].r.job.Priority
		}
		if cands[i].r.start != cands[j].r.start {
			return cands[i].r.start > cands[j].r.start
		}
		return cands[i].r.job.GPUs > cands[j].r.job.GPUs
	})

	freed := 0
	var evict []running
	for _, c := range cands {
		if freed >= p.job.GPUs {
			break
		}
		if c.alloc-float64(c.r.job.GPUs) < c.guar {
			continue
		}
		evict = append(evict, c.r)
		freed += c.r.job.GPUs
	}
	if freed < p.job.GPUs {
		return
	}

	// Apply the eviction.
	evictSet := map[string]bool{}
	for _, e := range evict {
		evictSet[e.job.ID] = true
		e.node.allocated -= e.job.GPUs
		tenantAlloc[e.job.Tenant] -= float64(e.job.GPUs)
	}
	remaining := (*runnings)[:0]
	for _, r := range *runnings {
		if !evictSet[r.job.ID] {
			remaining = append(remaining, r)
		}
	}
	*runnings = remaining

	n := placeTopology(nodes, p.job.GPUs)
	if n == nil {
		// Could not place after all; requeue the victims and stop.
		for _, e := range evict {
			e.node.allocated += e.job.GPUs
			tenantAlloc[e.job.Tenant] += float64(e.job.GPUs)
			*runnings = append(*runnings, e)
		}
		return
	}

	// Count and requeue victims so they are rescheduled once capacity frees.
	for _, e := range evict {
		if tenantAlloc[p.job.Tenant] < requester.Guaranteed {
			*reclaims++
		} else {
			*preemptions++
		}
		victim := *e.job
		*pendings = append(*pendings, pending{job: &victim, arrival: now})
	}
	startJob(p, n, now, runnings, tenantAlloc, waits)
	*pendings = append((*pendings)[:best], (*pendings)[best+1:]...)
}

// jobsOf returns the indices of a tenant's pending jobs in arrival order.
func jobsOf(pendings []pending, tenant string) []int {
	var idx []int
	for i := range pendings {
		if pendings[i].job.Tenant == tenant {
			idx = append(idx, i)
		}
	}
	sort.SliceStable(idx, func(a, b int) bool {
		ia, ib := idx[a], idx[b]
		if pendings[ia].arrival != pendings[ib].arrival {
			return pendings[ia].arrival < pendings[ib].arrival
		}
		return pendings[ia].job.ID < pendings[ib].job.ID
	})
	return idx
}

func fairshareFind(n *fairshare.QueueNode, name string) *fairshare.QueueNode {
	if n == nil {
		return nil
	}
	if n.Name == name {
		return n
	}
	for _, c := range n.Children {
		if f := fairshareFind(c, name); f != nil {
			return f
		}
	}
	return nil
}

func tenantOf(trace Trace, name string) *Tenant {
	for i := range trace.Tenants {
		if trace.Tenants[i].Name == name {
			return &trace.Tenants[i]
		}
	}
	return nil
}
