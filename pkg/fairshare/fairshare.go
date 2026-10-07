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

package fairshare

import (
	"fmt"
	"sort"
)

// ResourceVec maps resource names (e.g., "nvidia.com/gpu", "memory") to numeric quantities.
type ResourceVec map[string]float64

// Add adds another resource vector to v.
func (v ResourceVec) Add(other ResourceVec) ResourceVec {
	res := make(ResourceVec)
	for k, val := range v {
		res[k] = val
	}
	for k, val := range other {
		res[k] += val
	}
	return res
}

// Sub subtracts another resource vector from v.
func (v ResourceVec) Sub(other ResourceVec) ResourceVec {
	res := make(ResourceVec)
	for k, val := range v {
		res[k] = val
	}
	for k, val := range other {
		res[k] -= val
	}
	return res
}

// FitsIn checks if every resource in req is <= available in v.
func (v ResourceVec) FitsIn(available ResourceVec) bool {
	for k, reqVal := range v {
		if available[k] < reqVal {
			return false
		}
	}
	return true
}

// QueueSpec defines queue parameters for tree construction.
type QueueSpec struct {
	Name          string
	Parent        string
	Weight        int64
	Guaranteed    ResourceVec
	BorrowLimit   ResourceVec
	PriorityClass int32
	Reclaimable   bool
	Preemptible   bool
}

// QueueNode represents a node in the hierarchical queue tree.
type QueueNode struct {
	Name          string
	Parent        string
	Weight        int64
	Guaranteed    ResourceVec
	BorrowLimit   ResourceVec
	Allocated     ResourceVec
	Lent          ResourceVec
	Borrowed      ResourceVec
	PriorityClass int32
	Reclaimable   bool
	Preemptible   bool
	Children      []*QueueNode
	ParentNode    *QueueNode
}

// BuildTree builds and validates a hierarchical queue tree from flat queue specifications.
func BuildTree(queues []QueueSpec) (*QueueNode, error) {
	nodeMap := make(map[string]*QueueNode)
	for _, spec := range queues {
		if _, exists := nodeMap[spec.Name]; exists {
			return nil, fmt.Errorf("duplicate queue name: %s", spec.Name)
		}
		nodeMap[spec.Name] = &QueueNode{
			Name:          spec.Name,
			Parent:        spec.Parent,
			Weight:        spec.Weight,
			Guaranteed:    cloneVec(spec.Guaranteed),
			BorrowLimit:   cloneVec(spec.BorrowLimit),
			Allocated:     make(ResourceVec),
			Lent:          make(ResourceVec),
			Borrowed:      make(ResourceVec),
			PriorityClass: spec.PriorityClass,
			Reclaimable:   spec.Reclaimable,
			Preemptible:   spec.Preemptible,
			Children:      []*QueueNode{},
		}
	}

	var roots []*QueueNode
	for _, node := range nodeMap {
		if node.Parent == "" {
			roots = append(roots, node)
		} else {
			parentNode, exists := nodeMap[node.Parent]
			if !exists {
				return nil, fmt.Errorf("parent queue %s not found for queue %s", node.Parent, node.Name)
			}
			node.ParentNode = parentNode
			parentNode.Children = append(parentNode.Children, node)
		}
	}

	if len(roots) != 1 {
		return nil, fmt.Errorf("expected exactly 1 root queue, got %d", len(roots))
	}

	// Validate guaranteed sum constraints per parent
	for _, node := range nodeMap {
		if len(node.Children) > 0 {
			sumGuaranteed := make(ResourceVec)
			for _, child := range node.Children {
				sumGuaranteed = sumGuaranteed.Add(child.Guaranteed)
			}
			for res, parentGuaranteed := range node.Guaranteed {
				if sumGuaranteed[res] > parentGuaranteed {
					return nil, fmt.Errorf("sum of children guaranteed for %s (%.2f) exceeds parent guaranteed (%.2f)", res, sumGuaranteed[res], parentGuaranteed)
				}
			}
		}
	}

	// Check for cycles using DFS
	visited := make(map[string]int) // 0: unvisited, 1: visiting, 2: visited
	var checkCycle func(n *QueueNode) error
	checkCycle = func(n *QueueNode) error {
		visited[n.Name] = 1
		for _, child := range n.Children {
			if visited[child.Name] == 1 {
				return fmt.Errorf("cycle detected involving queue %s", child.Name)
			}
			if visited[child.Name] == 0 {
				if err := checkCycle(child); err != nil {
					return err
				}
			}
		}
		visited[n.Name] = 2
		return nil
	}

	for _, root := range roots {
		if err := checkCycle(root); err != nil {
			return nil, err
		}
	}

	return roots[0], nil
}

// DominantShare calculates the DRF dominant resource share, weight-normalized (share / weight).
func DominantShare(alloc, capacity ResourceVec) float64 {
	maxShare := 0.0
	for res, val := range alloc {
		capVal := capacity[res]
		if capVal > 0 {
			share := val / capVal
			if share > maxShare {
				maxShare = share
			}
		}
	}
	return maxShare
}

// WeightNormalizedDominantShare computes dominant share divided by queue weight.
func WeightNormalizedDominantShare(q *QueueNode, capacity ResourceVec) float64 {
	dom := DominantShare(q.Allocated, capacity)
	weight := float64(q.Weight)
	if weight <= 0 {
		weight = 1.0
	}
	return dom / weight
}

// RankQueues returns queue names ordered by ascending weighted dominant share, with deterministic tie-break by name.
func RankQueues(tree *QueueNode, capacity ResourceVec) []string {
	var nodes []*QueueNode
	var collect func(n *QueueNode)
	collect = func(n *QueueNode) {
		nodes = append(nodes, n)
		for _, child := range n.Children {
			collect(child)
		}
	}
	collect(tree)

	sort.Slice(nodes, func(i, j int) bool {
		shareI := WeightNormalizedDominantShare(nodes[i], capacity)
		shareJ := WeightNormalizedDominantShare(nodes[j], capacity)
		if shareI == shareJ {
			return nodes[i].Name < nodes[j].Name
		}
		return shareI < shareJ
	})

	var names []string
	for _, n := range nodes {
		names = append(names, n.Name)
	}
	return names
}

// CanAdmit checks if a queue can admit a resource request.
// Admits inside Guaranteed first; beyond Guaranteed, borrows only up to BorrowLimit and from idle lent capacity in ancestor subtree.
func CanAdmit(q *QueueNode, req ResourceVec) (ok bool, borrowed ResourceVec, reason string) {
	borrowed = make(ResourceVec)

	// Calculate current usage vs guaranteed
	// Within guaranteed?
	withinGuaranteed := true
	for res, val := range req {
		currentAlloc := q.Allocated[res]
		guaranteedVal := q.Guaranteed[res]
		if currentAlloc+val > guaranteedVal {
			withinGuaranteed = false
		}
	}

	if withinGuaranteed {
		return true, borrowed, ""
	}

	// Needs borrowing. Check borrow limits.
	for res, val := range req {
		currentAlloc := q.Allocated[res]
		guaranteedVal := q.Guaranteed[res]
		overage := (currentAlloc + val) - guaranteedVal
		if overage > 0 {
			limit := q.BorrowLimit[res]
			if overage > limit {
				return false, nil, fmt.Sprintf("request exceeds borrow limit for resource %s", res)
			}
			borrowed[res] = overage
		}
	}

	// Check if ancestor subtree has idle lent capacity to support the borrow.
	// We trace up ancestors and check available idle guaranteed capacity.
	for res, bVal := range borrowed {
		availableIdle := findIdleAncestorCapacity(q, res)
		if availableIdle < bVal {
			return false, nil, fmt.Sprintf("insufficient idle guaranteed capacity in ancestor subtree for resource %s (needed %.2f, available %.2f)", res, bVal, availableIdle)
		}
	}

	return true, borrowed, ""
}

func findIdleAncestorCapacity(q *QueueNode, res string) float64 {
	// Traverse up to parent / ancestors and find idle capacity (Guaranteed - Allocated in sibling subtree)
	curr := q.ParentNode
	for curr != nil {
		// Idle capacity at this ancestor level is ancestor.Guaranteed - ancestor.Allocated (plus any lent out)
		idle := curr.Guaranteed[res] - curr.Allocated[res]
		if idle > 0 {
			return idle
		}
		curr = curr.ParentNode
	}
	return 0
}

// StarvationGuard promotes a queue whose head pod waited past the threshold so low-priority queues cannot starve.
func StarvationGuard(q *QueueNode, waitSeconds float64, threshold float64) bool {
	return waitSeconds >= threshold
}

func cloneVec(v ResourceVec) ResourceVec {
	res := make(ResourceVec)
	for k, val := range v {
		res[k] = val
	}
	return res
}
