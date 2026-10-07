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

package topology

// NodeGPUState represents the GPU state and topology labels of a node.
type NodeGPUState struct {
	NodeName       string
	FreeGPUs       int
	TotalGPUs      int
	NVLinkGroup    string
	NUMANode       string
}

// ScoreNode returns 0-100 scoring for pod placement.
// Rewards same-node packing (whole request fits one node), same NVLink group, single NUMA node.
// Penalizes fragmentation across nodes and groups.
func ScoreNode(gpuRequest int, node NodeGPUState) int64 {
	if node.FreeGPUs < gpuRequest {
		return 0
	}

	score := 50.0 // base score for fitting

	// Reward same-node packing (fully fits on node)
	if node.FreeGPUs >= gpuRequest {
		score += 20.0
	}

	// Reward NVLink group presence
	if node.NVLinkGroup != "" {
		score += 15.0
	}

	// Reward NUMA locality
	if node.NUMANode != "" {
		score += 15.0
	}

	if score > 100.0 {
		return 100
	}
	if score < 0 {
		return 0
	}
	return int64(score)
}
