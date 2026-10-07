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

package plugin

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/fairshare"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/topology"
)

// Name is the scheduler plugin name registered with the framework.
const Name = "FairShareGPU"

// GPUResourceName is the extended resource used for GPU accounting.
const GPUResourceName = "nvidia.com/gpu"

// FairShareGPU is the scheduler-framework plugin implementing hierarchical
// quota admission, topology-aware scoring and guarantee-safe preemption.
type FairShareGPU struct {
	handle   framework.Handle
	registry *Registry
}

var _ framework.PreFilterPlugin = &FairShareGPU{}
var _ framework.FilterPlugin = &FairShareGPU{}
var _ framework.ScorePlugin = &FairShareGPU{}
var _ framework.PostFilterPlugin = &FairShareGPU{}
var _ framework.ReservePlugin = &FairShareGPU{}

// defaultRegistry is the process-wide registry shared by all plugin instances
// and kept up to date from the Kubernetes API by the scheduler's sync loop.
var defaultRegistry = NewRegistry()

// DefaultRegistry returns the shared registry used by the plugin.
func DefaultRegistry() *Registry { return defaultRegistry }

// New constructs the plugin. It satisfies the framework.PluginFactory signature
// used by app.WithPlugin.
func New(_ context.Context, _ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	if defaultRegistry.Tree() == nil {
		root := fairshare.QueueSpec{
			Name:        "root",
			Weight:      1,
			Guaranteed:  fairshare.ResourceVec{GPUResourceName: 32},
			BorrowLimit: fairshare.ResourceVec{GPUResourceName: 32},
		}
		def := fairshare.QueueSpec{
			Name:        "default-queue",
			Parent:      "root",
			Weight:      1,
			Guaranteed:  fairshare.ResourceVec{GPUResourceName: 32},
			BorrowLimit: fairshare.ResourceVec{GPUResourceName: 32},
			Preemptible: true,
			Reclaimable: true,
		}
		_ = defaultRegistry.SetQueues([]fairshare.QueueSpec{root, def})
	}
	return &FairShareGPU{handle: handle, registry: defaultRegistry}, nil
}

// Registry exposes the plugin's shared state for external configuration and tests.
func (pl *FairShareGPU) Registry() *Registry { return pl.registry }

// Name returns the plugin name.
func (pl *FairShareGPU) Name() string { return Name }

// PreFilter resolves the pod's queue via QueueBinding and enforces CanAdmit.
func (pl *FairShareGPU) PreFilter(_ context.Context, _ *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	queueName := pl.registry.ResolveQueue(pod)
	if queueName == "" {
		queueName = "default-queue"
	}
	q := FindQueue(pl.registry.Tree(), queueName)
	if q == nil {
		return nil, framework.NewStatus(framework.Unschedulable, fmt.Sprintf("queue %q not found", queueName))
	}
	req := fairshare.ResourceVec{GPUResourceName: float64(podGPURequest(pod))}
	ok, _, reason := fairshare.CanAdmit(q, req)
	if !ok {
		return nil, framework.NewStatus(framework.Unschedulable, "queue admission denied: "+reason)
	}
	return nil, nil
}

// PreFilterExtensions returns nil; this plugin does not implement remove.
func (pl *FairShareGPU) PreFilterExtensions() framework.PreFilterExtensions { return nil }

// Filter rejects nodes without enough free GPU.
func (pl *FairShareGPU) Filter(_ context.Context, _ *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	if nodeInfo == nil || nodeInfo.Node() == nil {
		return framework.NewStatus(framework.Error, "node info missing")
	}
	node := nodeInfo.Node()
	if freeGPUsOnNode(node, nodeInfo) < podGPURequest(pod) {
		return framework.NewStatus(framework.Unschedulable, "insufficient free GPUs on node "+node.Name)
	}
	return nil
}

// Score delegates to topology-aware scoring.
func (pl *FairShareGPU) Score(_ context.Context, _ *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	ns, ok := pl.registry.NodeState(nodeName)
	if !ok {
		ns = topology.NodeGPUState{NodeName: nodeName, FreeGPUs: 8, TotalGPUs: 8, NVLinkGroup: "group-0", NUMANode: "0"}
	}
	return topology.ScoreNode(podGPURequest(pod), ns), nil
}

// ScoreExtensions returns this plugin to enable NormalizeScore.
func (pl *FairShareGPU) ScoreExtensions() framework.ScoreExtensions { return pl }

// NormalizeScore clamps scores into the framework's 0-100 range.
func (pl *FairShareGPU) NormalizeScore(_ context.Context, _ *framework.CycleState, _ *v1.Pod, scores framework.NodeScoreList) *framework.Status {
	for i := range scores {
		if scores[i].Score > framework.MaxNodeScore {
			scores[i].Score = framework.MaxNodeScore
		}
		if scores[i].Score < framework.MinNodeScore {
			scores[i].Score = framework.MinNodeScore
		}
	}
	return nil
}

// PostFilter performs preemption: it selects guarantee-safe victims from
// over-quota, preemptible queues and emits a nomination for the preemption node.
func (pl *FairShareGPU) PostFilter(_ context.Context, _ *framework.CycleState, pod *v1.Pod, filtered framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	needed := podGPURequest(pod)
	candidates := pl.gatherCandidates(filtered)
	victims, ok := SelectVictims(candidates, needed)
	if !ok {
		return nil, framework.NewStatus(framework.Unschedulable, "no guarantee-safe victim found")
	}
	node := victimNode(victims)
	if node == "" {
		return nil, framework.NewStatus(framework.Unschedulable, "no node fits after preemption")
	}
	return framework.NewPostFilterResultWithNominatedNode(node), nil
}

// Reserve updates in-memory per-queue allocation under a lock.
func (pl *FairShareGPU) Reserve(_ context.Context, _ *framework.CycleState, pod *v1.Pod, _ string) *framework.Status {
	queueName := pl.registry.ResolveQueue(pod)
	pl.registry.mu.Lock()
	defer pl.registry.mu.Unlock()
	q := FindQueue(pl.registry.tree, queueName)
	if q == nil {
		return nil
	}
	if q.Allocated == nil {
		q.Allocated = fairshare.ResourceVec{}
	}
	q.Allocated[GPUResourceName] += float64(podGPURequest(pod))
	return nil
}

// Unreserve rolls back a Reserve.
func (pl *FairShareGPU) Unreserve(_ context.Context, _ *framework.CycleState, pod *v1.Pod, _ string) {
	queueName := pl.registry.ResolveQueue(pod)
	pl.registry.mu.Lock()
	defer pl.registry.mu.Unlock()
	q := FindQueue(pl.registry.tree, queueName)
	if q == nil || q.Allocated == nil {
		return
	}
	q.Allocated[GPUResourceName] -= float64(podGPURequest(pod))
	if q.Allocated[GPUResourceName] < 0 {
		q.Allocated[GPUResourceName] = 0
	}
}

// gatherCandidates builds victim candidates from the nodes that failed filtering.
func (pl *FairShareGPU) gatherCandidates(filtered framework.NodeToStatusMap) []VictimCandidate {
	if pl.handle == nil {
		return nil
	}
	lister := pl.handle.SnapshotSharedLister()
	if lister == nil {
		return nil
	}
	infos, err := lister.NodeInfos().List()
	if err != nil {
		return nil
	}
	var out []VictimCandidate
	tree := pl.registry.Tree()
	for _, info := range infos {
		if info == nil || info.Node() == nil {
			continue
		}
		nodeName := info.Node().Name
		if len(filtered) > 0 {
			if _, failed := filtered[nodeName]; !failed {
				continue
			}
		}
		for _, pi := range info.Pods {
			if pi.Pod == nil {
				continue
			}
			out = append(out, pl.candidateFor(pi.Pod, nodeName, tree))
		}
	}
	return out
}

func (pl *FairShareGPU) candidateFor(p *v1.Pod, nodeName string, tree *fairshare.QueueNode) VictimCandidate {
	qName := pl.registry.ResolveQueue(p)
	q := FindQueue(tree, qName)
	alloc, guar := 0.0, 0.0
	preemptible := true
	if q != nil {
		alloc = q.Allocated[GPUResourceName]
		guar = q.Guaranteed[GPUResourceName]
		preemptible = q.Preemptible
	}
	var start time.Time
	if p.Status.StartTime != nil {
		start = p.Status.StartTime.Time
	}
	var prio int32
	if p.Spec.Priority != nil {
		prio = *p.Spec.Priority
	}
	return VictimCandidate{
		PodName:         p.Name,
		NodeName:        nodeName,
		Queue:           qName,
		Priority:        prio,
		StartTime:       start,
		GPUs:            podGPURequest(p),
		QueueAllocated:  alloc,
		QueueGuaranteed: guar,
		Preemptible:     preemptible,
	}
}

func victimNode(victims []VictimCandidate) string {
	if len(victims) == 0 {
		return ""
	}
	freed := map[string]int{}
	for _, v := range victims {
		freed[v.NodeName] += v.GPUs
	}
	best := ""
	bestFreed := -1
	for node, g := range freed {
		if g > bestFreed || (g == bestFreed && node < best) {
			best = node
			bestFreed = g
		}
	}
	return best
}

// podGPURequest sums the nvidia.com/gpu requests across a pod's containers.
// Pods that request no GPU return 0 so they do not consume extended resource
// capacity during filtering.
func podGPURequest(pod *v1.Pod) int {
	total := 0
	for _, c := range pod.Spec.Containers {
		if q, ok := c.Resources.Requests[v1.ResourceName(GPUResourceName)]; ok {
			total += int(q.Value())
		}
	}
	return total
}

// freeGPUsOnNode reports free GPU capacity considering already-assigned pods.
func freeGPUsOnNode(node *v1.Node, nodeInfo *framework.NodeInfo) int {
	total := 0
	if q, ok := node.Status.Allocatable[v1.ResourceName(GPUResourceName)]; ok {
		total = int(q.Value())
	}
	used := 0
	if nodeInfo != nil {
		for _, p := range nodeInfo.Pods {
			if p.Pod != nil {
				used += podGPURequest(p.Pod)
			}
		}
	}
	if total-used < 0 {
		return 0
	}
	return total - used
}
