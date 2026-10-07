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

// Package sim is a deterministic discrete-event simulator that schedules a
// multi-tenant GPU trace using the same fairshare and topology code the real
// scheduler plugin uses. It powers the reproducible benchmark in cmd/bench.
package sim

// Trace is a fully deterministic synthetic workload description.
type Trace struct {
	Seed          int64    `json:"seed"`
	DurationHours float64  `json:"durationHours"`
	Pool          Pool     `json:"pool"`
	Tenants       []Tenant `json:"tenants"`
	Jobs          []Job    `json:"jobs"`
}

// Pool describes the shared GPU capacity.
type Pool struct {
	TotalGPUs int `json:"totalGPUs"`
	Nodes     int `json:"nodes"`
	GPUsPerNode int `json:"gpusPerNode"`
	NVLinkGroups int `json:"nvlinkGroups"`
}

// Tenant is a scheduling tenant with quota and weight.
type Tenant struct {
	Name          string  `json:"name"`
	Weight        int64   `json:"weight"`
	Guaranteed    float64 `json:"guaranteed"`
	BorrowLimit   float64 `json:"borrowLimit"`
	PriorityClass int32   `json:"priorityClass"`
	Preemptible   bool    `json:"preemptible"`
	Reclaimable   bool    `json:"reclaimable"`
}

// Job is a single GPU workload.
type Job struct {
	ID        string  `json:"id"`
	Tenant    string  `json:"tenant"`
	GPUs      int     `json:"gpus"`
	Arrival   float64 `json:"arrival"`   // seconds from trace start
	Duration  float64 `json:"duration"`  // seconds of runtime
	Priority  int32   `json:"priority"`  // higher wins
}
