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
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/fairshare"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/topology"
)

const Name = "FairShareGPU"

type FairShareGPU struct {
	mu         sync.Mutex
	tree       *fairshare.QueueNode
	capacity   fairshare.ResourceVec
	nodeStates map[string]topology.NodeGPUState
}

func New(_ context.Context, _ runtime.Object, _ interface{}) (interface{}, error) {
	root := &fairshare.QueueNode{
		Name:          "root",
		Weight:        1,
		Guaranteed:    fairshare.ResourceVec{"nvidia.com/gpu": 32},
		BorrowLimit:   fairshare.ResourceVec{"nvidia.com/gpu": 32},
		Allocated:     make(fairshare.ResourceVec),
		Children: []*fairshare.QueueNode{
			{
				Name:          "default-queue",
				Parent:        "root",
				Weight:        1,
				Guaranteed:    fairshare.ResourceVec{"nvidia.com/gpu": 32},
				BorrowLimit:   fairshare.ResourceVec{"nvidia.com/gpu": 32},
				Allocated:     make(fairshare.ResourceVec),
				Preemptible:   true,
				Reclaimable:   true,
			},
		},
	}
	return &FairShareGPU{
		tree:       root,
		capacity:   fairshare.ResourceVec{"nvidia.com/gpu": 32},
		nodeStates: make(map[string]topology.NodeGPUState),
	}, nil
}

func (pl *FairShareGPU) Name() string {
	return Name
}

func (pl *FairShareGPU) PreFilter(ctx context.Context, pod *v1.Pod) (string, error) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	queueName := pod.Labels["fairshare.io/queue"]
	if queueName == "" {
		queueName = "default-queue"
	}

	queueNode := findQueueNode(pl.tree, queueName)
	if queueNode == nil {
		return "", fmt.Errorf("queue %s not found", queueName)
	}

	reqGPUs := getPodGPURequest(pod)
	req := fairshare.ResourceVec{"nvidia.com/gpu": float64(reqGPUs)}

	ok, _, reason := fairshare.CanAdmit(queueNode, req)
	if !ok {
		return "", fmt.Errorf("queue admission denied: %s", reason)
	}

	return "", nil
}

func (pl *FairShareGPU) Filter(ctx context.Context, pod *v1.Pod, nodeName string, freeGPUs int) bool {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	reqGPUs := getPodGPURequest(pod)
	return freeGPUs >= reqGPUs
}

func (pl *FairShareGPU) Score(ctx context.Context, pod *v1.Pod, nodeName string) int64 {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	reqGPUs := getPodGPURequest(pod)
	ns, exists := pl.nodeStates[nodeName]
	if !exists {
		ns = topology.NodeGPUState{
			NodeName:    nodeName,
			FreeGPUs:    8,
			TotalGPUs:   8,
			NVLinkGroup: "group-0",
			NUMANode:    "0",
		}
	}

	return topology.ScoreNode(reqGPUs, ns)
}

func findQueueNode(n *fairshare.QueueNode, name string) *fairshare.QueueNode {
	if n.Name == name {
		return n
	}
	for _, child := range n.Children {
		if found := findQueueNode(child, name); found != nil {
			return found
		}
	}
	return nil
}

func getPodGPURequest(pod *v1.Pod) int {
	total := 0
	for _, container := range pod.Spec.Containers {
		if val, ok := container.Resources.Requests[v1.ResourceName("nvidia.com/gpu")]; ok {
			total += int(val.Value())
		}
	}
	if total == 0 {
		return 1
	}
	return total
}
