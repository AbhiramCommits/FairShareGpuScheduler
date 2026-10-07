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
	"sort"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/fairshare"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/topology"
)

// BindingSpec is a Kubernetes-free representation of a QueueBinding.
type BindingSpec struct {
	Queue             string
	Namespace         string
	NamespaceSelector map[string]string
	PodSelector       map[string]string
}

// Registry holds the in-memory state shared by the scheduler plugin:
// the queue tree, cluster capacity and per-node GPU topology state.
// It is safe for concurrent use.
type Registry struct {
	mu         sync.RWMutex
	specs      []fairshare.QueueSpec
	bindings   []BindingSpec
	tree       *fairshare.QueueNode
	capacity   fairshare.ResourceVec
	nodeStates map[string]topology.NodeGPUState
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		capacity:   fairshare.ResourceVec{},
		nodeStates: map[string]topology.NodeGPUState{},
	}
}

// SetQueues replaces the queue tree from a list of specs.
func (r *Registry) SetQueues(specs []fairshare.QueueSpec) error {
	tree, err := fairshare.BuildTree(specs)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.specs = specs
	r.tree = tree
	return nil
}

// SetBindings replaces the queue bindings.
func (r *Registry) SetBindings(bindings []BindingSpec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bindings = bindings
}

// SetCapacity sets cluster GPU capacity for DRF ranking.
func (r *Registry) SetCapacity(cap fairshare.ResourceVec) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capacity = cap
}

// SetNodeStates sets observed per-node GPU topology state.
func (r *Registry) SetNodeStates(states map[string]topology.NodeGPUState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodeStates = states
}

// Tree returns the current queue tree (may be nil).
func (r *Registry) Tree() *fairshare.QueueNode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tree
}

// Capacity returns the current cluster capacity.
func (r *Registry) Capacity() fairshare.ResourceVec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.capacity
}

// NodeState returns GPU topology state for a node.
func (r *Registry) NodeState(nodeName string) (topology.NodeGPUState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ns, ok := r.nodeStates[nodeName]
	return ns, ok
}

// ResolveQueue returns the queue name for a pod using the registered bindings.
// Precedence: explicit pod label, then pod label selector binding, then namespace binding.
func (r *Registry) ResolveQueue(pod *v1.Pod) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if q := pod.Labels["fairshare.io/queue"]; q != "" {
		return q
	}

	for _, b := range r.bindings {
		if b.Namespace != "" && b.Namespace != pod.Namespace {
			continue
		}
		if len(b.NamespaceSelector) > 0 && !labelsMatch(b.NamespaceSelector, nsLabels(pod.Namespace)) {
			continue
		}
		if len(b.PodSelector) > 0 && !labelsMatch(b.PodSelector, pod.Labels) {
			continue
		}
		return b.Queue
	}
	return ""
}

// FindQueue locates a queue node by name in the tree.
func FindQueue(tree *fairshare.QueueNode, name string) *fairshare.QueueNode {
	if tree == nil {
		return nil
	}
	if tree.Name == name {
		return tree
	}
	for _, c := range tree.Children {
		if found := FindQueue(c, name); found != nil {
			return found
		}
	}
	return nil
}

func labelsMatch(selector, labels map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func nsLabels(ns string) map[string]string {
	return map[string]string{"kubernetes.io/metadata.name": ns}
}

// VictimCandidate describes a pod that may be preempted.
type VictimCandidate struct {
	PodName         string
	NodeName        string
	Queue           string
	Priority        int32
	StartTime       time.Time
	GPUs            int
	QueueAllocated  float64
	QueueGuaranteed float64
	Preemptible     bool
}

// EligibleForPreemption reports whether a candidate may be preempted:
// only queues strictly over their guaranteed allocation and marked preemptible.
func EligibleForPreemption(c VictimCandidate) bool {
	if !c.Preemptible {
		return false
	}
	return c.QueueAllocated > c.QueueGuaranteed
}

// SelectVictims chooses victims to free at least needed GPUs.
// Ordering: lowest priorityClass first, then newest pod first, then largest GPU
// request first (fewest victims to fit). Guarantee-safe: never selects a pod
// from a queue at or below its guaranteed allocation.
func SelectVictims(candidates []VictimCandidate, needed int) ([]VictimCandidate, bool) {
	eligible := make([]VictimCandidate, 0, len(candidates))
	for _, c := range candidates {
		if EligibleForPreemption(c) {
			eligible = append(eligible, c)
		}
	}

	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].Priority != eligible[j].Priority {
			return eligible[i].Priority < eligible[j].Priority
		}
		if !eligible[i].StartTime.Equal(eligible[j].StartTime) {
			return eligible[i].StartTime.After(eligible[j].StartTime)
		}
		if eligible[i].GPUs != eligible[j].GPUs {
			return eligible[i].GPUs > eligible[j].GPUs
		}
		return eligible[i].PodName < eligible[j].PodName
	})

	var victims []VictimCandidate
	freed := 0
	for _, c := range eligible {
		if freed >= needed {
			break
		}
		victims = append(victims, c)
		freed += c.GPUs
	}
	if freed < needed {
		return nil, false
	}
	return victims, true
}
