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
	"testing"
)

func TestBuildTree(t *testing.T) {
	specs := []QueueSpec{
		{Name: "root", Parent: "", Weight: 1, Guaranteed: ResourceVec{"nvidia.com/gpu": 8}},
		{Name: "tenant-a", Parent: "root", Weight: 4, Guaranteed: ResourceVec{"nvidia.com/gpu": 4}, BorrowLimit: ResourceVec{"nvidia.com/gpu": 4}},
		{Name: "tenant-b", Parent: "root", Weight: 2, Guaranteed: ResourceVec{"nvidia.com/gpu": 4}, BorrowLimit: ResourceVec{"nvidia.com/gpu": 2}},
	}

	root, err := BuildTree(specs)
	if err != nil {
		t.Fatalf("unexpected error building tree: %v", err)
	}
	if root.Name != "root" {
		t.Errorf("expected root name 'root', got %s", root.Name)
	}
	if len(root.Children) != 2 {
		t.Errorf("expected 2 children, got %d", len(root.Children))
	}
}

func TestBuildTreeErrors(t *testing.T) {
	// Exceeding parent guaranteed
	specs := []QueueSpec{
		{Name: "root", Parent: "", Weight: 1, Guaranteed: ResourceVec{"nvidia.com/gpu": 4}},
		{Name: "tenant-a", Parent: "root", Weight: 1, Guaranteed: ResourceVec{"nvidia.com/gpu": 6}},
	}
	_, err := BuildTree(specs)
	if err == nil {
		t.Error("expected error when children guaranteed exceeds parent guaranteed, got nil")
	}

	// Cycle detection
	specsCycle := []QueueSpec{
		{Name: "q1", Parent: "q2", Weight: 1, Guaranteed: ResourceVec{"nvidia.com/gpu": 4}},
		{Name: "q2", Parent: "q1", Weight: 1, Guaranteed: ResourceVec{"nvidia.com/gpu": 4}},
	}
	_, err = BuildTree(specsCycle)
	if err == nil {
		t.Error("expected error on cycle detection, got nil")
	}
}

func TestDRFRankingAndShares(t *testing.T) {
	specs := []QueueSpec{
		{Name: "root", Parent: "", Weight: 1, Guaranteed: ResourceVec{"nvidia.com/gpu": 8, "memory": 64}},
		{Name: "a", Parent: "root", Weight: 4, Guaranteed: ResourceVec{"nvidia.com/gpu": 4}},
		{Name: "b", Parent: "root", Weight: 2, Guaranteed: ResourceVec{"nvidia.com/gpu": 4}},
	}
	root, err := BuildTree(specs)
	if err != nil {
		t.Fatalf("failed to build tree: %v", err)
	}

	// Allocate to a and b
	for _, child := range root.Children {
		if child.Name == "a" {
			child.Allocated = ResourceVec{"nvidia.com/gpu": 2, "memory": 16}
		} else if child.Name == "b" {
			child.Allocated = ResourceVec{"nvidia.com/gpu": 3, "memory": 32}
		}
	}

	capacity := ResourceVec{"nvidia.com/gpu": 8, "memory": 64}
	ranked := RankQueues(root, capacity)
	if len(ranked) != 3 {
		t.Fatalf("expected 3 ranked queues, got %d", len(ranked))
	}
}

func TestCanAdmitAndBorrowing(t *testing.T) {
	specs := []QueueSpec{
		{Name: "root", Parent: "", Weight: 1, Guaranteed: ResourceVec{"nvidia.com/gpu": 8}},
		{Name: "a", Parent: "root", Weight: 1, Guaranteed: ResourceVec{"nvidia.com/gpu": 4}, BorrowLimit: ResourceVec{"nvidia.com/gpu": 2}},
	}
	root, err := BuildTree(specs)
	if err != nil {
		t.Fatalf("failed to build tree: %v", err)
	}

	a := root.Children[0]

	// Admit within guaranteed
	ok, _, _ := CanAdmit(a, ResourceVec{"nvidia.com/gpu": 3})
	if !ok {
		t.Error("expected admission within guaranteed to succeed")
	}

	// Admit within borrow limit
	ok, borrowed, _ := CanAdmit(a, ResourceVec{"nvidia.com/gpu": 5})
	if !ok {
		t.Error("expected admission within borrow limit to succeed")
	}
	if borrowed["nvidia.com/gpu"] != 1.0 {
		t.Errorf("expected borrowed 1.0, got %.2f", borrowed["nvidia.com/gpu"])
	}

	// Exceed borrow limit
	ok, _, _ = CanAdmit(a, ResourceVec{"nvidia.com/gpu": 7})
	if ok {
		t.Error("expected admission exceeding borrow limit to fail")
	}
}

func TestStarvationGuard(t *testing.T) {
	specs := []QueueSpec{
		{Name: "root", Parent: "", Weight: 1, Guaranteed: ResourceVec{"nvidia.com/gpu": 8}},
		{Name: "low", Parent: "root", Weight: 1, Guaranteed: ResourceVec{"nvidia.com/gpu": 4}},
	}
	root, err := BuildTree(specs)
	if err != nil {
		t.Fatalf("failed to build tree: %v", err)
	}
	low := root.Children[0]

	promoted := StarvationGuard(low, 35.0, 30.0)
	if !promoted {
		t.Error("expected queue to be promoted by starvation guard after threshold")
	}
}
