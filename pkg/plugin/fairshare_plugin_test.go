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
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	framework "k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/fairshare"
)

// fakeNodeInfoLister implements framework.NodeInfoLister by embedding the
// interface and overriding only List.
type fakeNodeInfoLister struct {
	framework.NodeInfoLister
	infos []*framework.NodeInfo
}

func (f *fakeNodeInfoLister) List() ([]*framework.NodeInfo, error) { return f.infos, nil }

// fakeSharedLister implements framework.SharedLister.
type fakeSharedLister struct {
	nodeLister *fakeNodeInfoLister
}

func (f *fakeSharedLister) NodeInfos() framework.NodeInfoLister      { return f.nodeLister }
func (f *fakeSharedLister) StorageInfos() framework.StorageInfoLister { return nil }

// fakeHandle embeds framework.Handle and overrides only SnapshotSharedLister.
type fakeHandle struct {
	framework.Handle
	lister framework.SharedLister
}

func (f *fakeHandle) SnapshotSharedLister() framework.SharedLister { return f.lister }

func makePod(name, queue string, gpus int, priority int32, start time.Time) *v1.Pod {
	p := int32(priority)
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{"fairshare.io/queue": queue},
		},
		Spec: v1.PodSpec{
			Priority: &p,
			Containers: []v1.Container{{
				Name: "c",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceName(GPUResourceName): resource.MustParse(itoa(gpus)),
					},
				},
			}},
		},
		Status: v1.PodStatus{StartTime: &metav1.Time{Time: start}},
	}
}

func itoa(i int) string {
	switch i {
	case 0:
		return "0"
	case 1:
		return "1"
	case 2:
		return "2"
	case 3:
		return "3"
	case 4:
		return "4"
	case 6:
		return "6"
	case 8:
		return "8"
	default:
		return "1"
	}
}

func newTestPlugin(t *testing.T, queues []fairshare.QueueSpec, infos []*framework.NodeInfo) *FairShareGPU {
	t.Helper()
	p, err := New(context.TODO(), nil, &fakeHandle{lister: &fakeSharedLister{nodeLister: &fakeNodeInfoLister{infos: infos}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	pl := p.(*FairShareGPU)
	if err := pl.Registry().SetQueues(queues); err != nil {
		t.Fatalf("SetQueues: %v", err)
	}
	return pl
}

func TestSelectVictimsGuaranteeSafe(t *testing.T) {
	now := time.Now()
	candidates := []VictimCandidate{
		// queue over guarantee, preemptible -> eligible
		{PodName: "borrower-a", Queue: "b", Priority: 100, StartTime: now, GPUs: 2, QueueAllocated: 6, QueueGuaranteed: 4, Preemptible: true},
		// queue at guarantee -> must never be evicted
		{PodName: "owner", Queue: "a", Priority: 100, StartTime: now, GPUs: 4, QueueAllocated: 4, QueueGuaranteed: 4, Preemptible: true},
	}
	victims, ok := SelectVictims(candidates, 1)
	if !ok {
		t.Fatal("expected a victim")
	}
	if len(victims) != 1 || victims[0].PodName != "borrower-a" {
		t.Fatalf("expected borrower-a as victim, got %+v", victims)
	}
}

func TestSelectVictimsNoVictimAvailable(t *testing.T) {
	now := time.Now()
	candidates := []VictimCandidate{
		{PodName: "owner", Queue: "a", Priority: 100, StartTime: now, GPUs: 4, QueueAllocated: 4, QueueGuaranteed: 4, Preemptible: true},
		{PodName: "nonpreemptible", Queue: "b", Priority: 100, StartTime: now, GPUs: 4, QueueAllocated: 8, QueueGuaranteed: 4, Preemptible: false},
	}
	if _, ok := SelectVictims(candidates, 2); ok {
		t.Fatal("expected no victims available")
	}
}

func TestSelectVictimsPrefersFewestVictims(t *testing.T) {
	now := time.Now()
	candidates := []VictimCandidate{
		{PodName: "small-1", Queue: "b", Priority: 100, StartTime: now, GPUs: 1, QueueAllocated: 8, QueueGuaranteed: 4, Preemptible: true},
		{PodName: "small-2", Queue: "b", Priority: 100, StartTime: now, GPUs: 1, QueueAllocated: 8, QueueGuaranteed: 4, Preemptible: true},
		{PodName: "small-3", Queue: "b", Priority: 100, StartTime: now, GPUs: 1, QueueAllocated: 8, QueueGuaranteed: 4, Preemptible: true},
		{PodName: "big", Queue: "b", Priority: 100, StartTime: now, GPUs: 4, QueueAllocated: 8, QueueGuaranteed: 4, Preemptible: true},
	}
	victims, ok := SelectVictims(candidates, 3)
	if !ok {
		t.Fatal("expected victims")
	}
	if len(victims) != 1 || victims[0].PodName != "big" {
		t.Fatalf("expected single big victim, got %+v", victims)
	}
}

func TestSelectVictimsLowestPriorityFirst(t *testing.T) {
	now := time.Now()
	candidates := []VictimCandidate{
		{PodName: "high", Queue: "b", Priority: 200, StartTime: now.Add(-time.Minute), GPUs: 4, QueueAllocated: 8, QueueGuaranteed: 4, Preemptible: true},
		{PodName: "low", Queue: "b", Priority: 10, StartTime: now, GPUs: 4, QueueAllocated: 8, QueueGuaranteed: 4, Preemptible: true},
	}
	victims, ok := SelectVictims(candidates, 4)
	if !ok || len(victims) != 1 || victims[0].PodName != "low" {
		t.Fatalf("expected low priority victim, got %+v ok=%v", victims, ok)
	}
}

func TestSelectVictimsNewestFirstWithinPriority(t *testing.T) {
	now := time.Now()
	candidates := []VictimCandidate{
		{PodName: "old", Queue: "b", Priority: 100, StartTime: now.Add(-time.Hour), GPUs: 4, QueueAllocated: 8, QueueGuaranteed: 4, Preemptible: true},
		{PodName: "new", Queue: "b", Priority: 100, StartTime: now, GPUs: 4, QueueAllocated: 8, QueueGuaranteed: 4, Preemptible: true},
	}
	victims, ok := SelectVictims(candidates, 4)
	if !ok || victims[0].PodName != "new" {
		t.Fatalf("expected newest victim, got %+v", victims)
	}
}

func TestPostFilterPrefersBorrowerAndNominatesNode(t *testing.T) {
	guaranteed := 4.0
	_ = guaranteed
	queues := []fairshare.QueueSpec{
		{Name: "root", Weight: 1, Guaranteed: fairshare.ResourceVec{GPUResourceName: 16}},
		// tenant-b over its guarantee and preemptible; tenant-a exactly at guarantee.
		{Name: "tenant-a", Parent: "root", Weight: 1, Guaranteed: fairshare.ResourceVec{GPUResourceName: 4}, BorrowLimit: fairshare.ResourceVec{GPUResourceName: 4}, Preemptible: true},
		{Name: "tenant-b", Parent: "root", Weight: 1, Guaranteed: fairshare.ResourceVec{GPUResourceName: 4}, BorrowLimit: fairshare.ResourceVec{GPUResourceName: 4}, Preemptible: true},
	}

	nodeA := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: v1.NodeStatus{Allocatable: v1.ResourceList{v1.ResourceName(GPUResourceName): resource.MustParse("8")}}}
	nodeB := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}, Status: v1.NodeStatus{Allocatable: v1.ResourceList{v1.ResourceName(GPUResourceName): resource.MustParse("8")}}}

	now := time.Now()
	aPod := makePod("a-pod", "tenant-a", 4, 100, now.Add(-time.Minute))
	bPod := makePod("b-pod", "tenant-b", 4, 100, now)

	infoA := framework.NewNodeInfo(aPod)
	infoA.SetNode(nodeA)
	infoB := framework.NewNodeInfo(bPod)
	infoB.SetNode(nodeB)

	pl := newTestPlugin(t, queues, []*framework.NodeInfo{infoA, infoB})
	// Manually set allocations: tenant-a at guarantee (4), tenant-b over (8).
	tree := pl.Registry().Tree()
	FindQueue(tree, "tenant-a").Allocated = fairshare.ResourceVec{GPUResourceName: 4}
	FindQueue(tree, "tenant-b").Allocated = fairshare.ResourceVec{GPUResourceName: 8}

	incoming := makePod("incoming", "tenant-c", 4, 100, now)
	_ = incoming
	res, status := pl.PostFilter(context.TODO(), nil, makePod("incoming", "tenant-a", 4, 500, now), framework.NodeToStatusMap{"node-b": framework.NewStatus(framework.Unschedulable)})
	if status != nil && !status.IsSuccess() {
		t.Fatalf("expected success nomination, got %v", status.Message())
	}
	if res == nil || res.NominatedNodeName != "node-b" {
		t.Fatalf("expected nomination on node-b, got %+v", res)
	}
}

func TestPreFilterAdmissionAndReserve(t *testing.T) {
	queues := []fairshare.QueueSpec{
		{Name: "root", Weight: 1, Guaranteed: fairshare.ResourceVec{GPUResourceName: 8}},
		{Name: "tenant-a", Parent: "root", Weight: 1, Guaranteed: fairshare.ResourceVec{GPUResourceName: 4}, BorrowLimit: fairshare.ResourceVec{GPUResourceName: 2}, Preemptible: true},
	}
	pl := newTestPlugin(t, queues, nil)

	pod := makePod("p1", "tenant-a", 3, 100, time.Now())
	if _, st := pl.PreFilter(context.TODO(), nil, pod); st != nil && !st.IsSuccess() {
		t.Fatalf("expected admission within guarantee, got %v", st.Message())
	}

	if st := pl.Reserve(context.TODO(), nil, pod, "node-a"); st != nil && !st.IsSuccess() {
		t.Fatalf("reserve failed: %v", st.Message())
	}
	tree := pl.Registry().Tree()
	if got := FindQueue(tree, "tenant-a").Allocated[GPUResourceName]; got != 3 {
		t.Fatalf("expected allocated 3 after reserve, got %v", got)
	}
	pl.Unreserve(context.TODO(), nil, pod, "node-a")
	if got := FindQueue(tree, "tenant-a").Allocated[GPUResourceName]; got != 0 {
		t.Fatalf("expected allocated 0 after unreserve, got %v", got)
	}
}

func TestResolveQueueViaBinding(t *testing.T) {
	pl := newTestPlugin(t, []fairshare.QueueSpec{
		{Name: "root", Weight: 1, Guaranteed: fairshare.ResourceVec{GPUResourceName: 8}},
		{Name: "team-x", Parent: "root", Weight: 1, Guaranteed: fairshare.ResourceVec{GPUResourceName: 4}, BorrowLimit: fairshare.ResourceVec{GPUResourceName: 4}},
	}, nil)
	pl.Registry().SetBindings([]BindingSpec{{Queue: "team-x", Namespace: "team-x"}})

	bound := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "team-x"}}
	if got := pl.Registry().ResolveQueue(bound); got != "team-x" {
		t.Fatalf("expected team-x via namespace binding, got %q", got)
	}
	labeled := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "other", Labels: map[string]string{"fairshare.io/queue": "team-x"}}}
	if got := pl.Registry().ResolveQueue(labeled); got != "team-x" {
		t.Fatalf("expected explicit label to win, got %q", got)
	}
}

func TestScoreAndFilter(t *testing.T) {
	pl := newTestPlugin(t, []fairshare.QueueSpec{
		{Name: "root", Weight: 1, Guaranteed: fairshare.ResourceVec{GPUResourceName: 8}},
		{Name: "default-queue", Parent: "root", Weight: 1, Guaranteed: fairshare.ResourceVec{GPUResourceName: 8}, BorrowLimit: fairshare.ResourceVec{GPUResourceName: 8}},
	}, nil)

	pod := makePod("p1", "default-queue", 4, 100, time.Now())
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Status: v1.NodeStatus{Allocatable: v1.ResourceList{v1.ResourceName(GPUResourceName): resource.MustParse("8")}}}
	info := framework.NewNodeInfo()
	info.SetNode(node)

	if st := pl.Filter(context.TODO(), nil, pod, info); st != nil && !st.IsSuccess() {
		t.Fatalf("expected filter pass, got %v", st.Message())
	}
	score, _ := pl.Score(context.TODO(), nil, pod, "node-a")
	if score <= 0 {
		t.Fatalf("expected positive score, got %d", score)
	}
	scores := framework.NodeScoreList{{Name: "node-a", Score: 500}}
	if st := pl.NormalizeScore(context.TODO(), nil, pod, scores); st != nil && !st.IsSuccess() {
		t.Fatalf("normalize failed: %v", st.Message())
	}
	if scores[0].Score != framework.MaxNodeScore {
		t.Fatalf("expected score clamped to %d, got %d", framework.MaxNodeScore, scores[0].Score)
	}
}
