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

package controller

import (
	"context"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fairsharev1alpha1 "github.com/abhiramkasireddi/fairshare-gpu-scheduler/api/v1alpha1"
)

func gpuPod(name, queue string, gpus int, phase v1.PodPhase, start time.Time) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    map[string]string{"fairshare.io/queue": queue},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{
				Name: "c",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceName("nvidia.com/gpu"): resource.MustParse(intToStr(gpus)),
					},
				},
			}},
		},
		Status: v1.PodStatus{Phase: phase, StartTime: &metav1.Time{Time: start}},
	}
}

func intToStr(i int) string {
	if i == 0 {
		return "0"
	}
	digits := ""
	for i > 0 {
		digits = string(rune('0'+i%10)) + digits
		i /= 10
	}
	return digits
}

func TestQueueReconcilerStatusAndReclaim(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = fairsharev1alpha1.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	now := time.Now()
	// tenant-a owns 4 guaranteed GPUs and is idle, so it lends 4.
	owner := &fairsharev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-a"},
		Spec: fairsharev1alpha1.QueueSpec{
			Weight:      1,
			Guaranteed:  v1.ResourceList{v1.ResourceName("nvidia.com/gpu"): resource.MustParse("4")},
			BorrowLimit: v1.ResourceList{v1.ResourceName("nvidia.com/gpu"): resource.MustParse("2")},
			Reclaimable: true,
			Preemptible: true,
		},
	}
	// tenant-b borrows 6 GPUs (2 over its guarantee of 4).
	borrower := &fairsharev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-b"},
		Spec: fairsharev1alpha1.QueueSpec{
			Weight:      1,
			Guaranteed:  v1.ResourceList{v1.ResourceName("nvidia.com/gpu"): resource.MustParse("2")},
			BorrowLimit: v1.ResourceList{v1.ResourceName("nvidia.com/gpu"): resource.MustParse("8")},
			Reclaimable: true,
			Preemptible: true,
		},
	}

	objects := []runtime.Object{owner, borrower}
	// tenant-b runs 2 pods of 3 GPUs each, one old one new.
	objects = append(objects,
		gpuPod("b-old", "tenant-b", 3, v1.PodRunning, now.Add(-time.Hour)),
		gpuPod("b-new", "tenant-b", 3, v1.PodRunning, now),
		// owner has a pending pod -> triggers reclaim.
		gpuPod("a-pending", "tenant-a", 2, v1.PodPending, now),
	)

	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).WithStatusSubresource(owner, borrower).Build()
	r := &QueueReconciler{Client: c, Scheme: scheme, GpuHourlyRate: 2.50, ReclaimGrace: time.Second, ReconcilePeriod: time.Second}

	if _, err := r.Reconcile(context.TODO(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "tenant-a"}}); err != nil {
		t.Fatalf("reconcile tenant-a: %v", err)
	}

	var got fairsharev1alpha1.Queue
	if err := c.Get(context.TODO(), types.NamespacedName{Name: "tenant-a"}, &got); err != nil {
		t.Fatalf("get owner: %v", err)
	}
	// owner runs nothing, so allocated 0 and lent 4.
	allocQ := got.Status.Allocated[v1.ResourceName("nvidia.com/gpu")]
	if allocQ.Value() != 0 {
		t.Fatalf("expected owner allocated 0, got %v", got.Status.Allocated)
	}
	lentQ := got.Status.Lent[v1.ResourceName("nvidia.com/gpu")]
	if lentQ.Value() != 4 {
		t.Fatalf("expected owner lent 4, got %v", got.Status.Lent)
	}
	if got.Status.PendingPods != 1 {
		t.Fatalf("expected 1 pending pod, got %d", got.Status.PendingPods)
	}

	// The reclaim must have evicted exactly the newest borrowed pod (b-new),
	// leaving the guaranteed pods intact.
	var podList v1.PodList
	if err := c.List(context.TODO(), &podList); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	remaining := map[string]bool{}
	for _, p := range podList.Items {
		remaining[p.Name] = true
	}
	if remaining["b-new"] {
		t.Fatalf("expected b-new (newest borrowed) to be reclaimed, still present: %v", remaining)
	}
	if !remaining["b-old"] {
		t.Fatalf("expected b-old to remain, pods: %v", remaining)
	}
	if !remaining["a-pending"] {
		t.Fatalf("expected owner pending pod to remain, pods: %v", remaining)
	}
}

func TestSelectReclaimVictimsNeverDropsBelowGuarantee(t *testing.T) {
	now := time.Now()
	pods := []queuePod{
		{Name: "old", GPUs: 2, StartTime: now.Add(-time.Hour)},
		{Name: "new", GPUs: 2, StartTime: now},
	}
	// Queue uses 4, guaranteed 2 -> only 2 reclaimable. Need 3 but must stop at 2.
	victims := selectReclaimVictims(pods, 2, 3)
	freed := 0
	for _, v := range victims {
		freed += v.GPUs
	}
	if freed > 2 {
		t.Fatalf("reclaimed %d GPUs, would drop queue below guarantee", freed)
	}
	if len(victims) == 0 || victims[0].Name != "new" {
		t.Fatalf("expected newest pod first, got %+v", victims)
	}
}

func TestComputeBorrowLend(t *testing.T) {
	if b, l := computeBorrowLend(6, 4, true); b != 2 || l != 0 {
		t.Fatalf("expected borrowed 2 lent 0, got b=%v l=%v", b, l)
	}
	if b, l := computeBorrowLend(2, 4, true); b != 0 || l != 2 {
		t.Fatalf("expected borrowed 0 lent 2, got b=%v l=%v", b, l)
	}
	if b, l := computeBorrowLend(2, 4, false); b != 0 || l != 0 {
		t.Fatalf("expected no lend when not reclaimable, got b=%v l=%v", b, l)
	}
}
