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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fairsharev1alpha1 "github.com/abhiramkasireddi/fairshare-gpu-scheduler/api/v1alpha1"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/finops"
)

// TestMetricsFamiliesExported drives a reconcile and then scrapes the FinOps
// metrics registry to prove every documented metric family is present and that
// at least one queue reports non-zero values.
func TestMetricsFamiliesExported(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = fairsharev1alpha1.AddToScheme(scheme)
	_ = v1.AddToScheme(scheme)

	now := time.Now()
	q := &fairsharev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-a"},
		Spec: fairsharev1alpha1.QueueSpec{
			Weight:      1,
			Guaranteed:  v1.ResourceList{v1.ResourceName("nvidia.com/gpu"): resource.MustParse("4")},
			BorrowLimit: v1.ResourceList{v1.ResourceName("nvidia.com/gpu"): resource.MustParse("4")},
			Reclaimable: true,
			Preemptible: true,
		},
	}
	borrower := &fairsharev1alpha1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-b"},
		Spec: fairsharev1alpha1.QueueSpec{
			Weight:      1,
			Guaranteed:  v1.ResourceList{v1.ResourceName("nvidia.com/gpu"): resource.MustParse("1")},
			BorrowLimit: v1.ResourceList{v1.ResourceName("nvidia.com/gpu"): resource.MustParse("8")},
			Reclaimable: true,
			Preemptible: true,
		},
	}
	objects := []runtime.Object{
		q, borrower,
		gpuPod("a-run", "tenant-a", 3, v1.PodRunning, now),
		gpuPod("a-pending", "tenant-a", 2, v1.PodPending, now),
		gpuPod("b-old", "tenant-b", 2, v1.PodRunning, now.Add(-time.Minute)),
		gpuPod("b-new", "tenant-b", 2, v1.PodRunning, now),
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).WithStatusSubresource(q, borrower).Build()
	r := &QueueReconciler{Client: c, Scheme: scheme, GpuHourlyRate: 2.50, ReconcilePeriod: time.Hour}
	if _, err := r.Reconcile(context.TODO(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "tenant-a"}}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// Scrape the actual /metrics handler.
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	finops.MetricsHandler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics handler returned %d", rec.Code)
	}
	body := rec.Body.String()
	for _, name := range []string{
		"fairshare_queue_gpus_allocated",
		"fairshare_queue_gpus_guaranteed",
		"fairshare_queue_gpus_borrowed",
		"fairshare_queue_gpus_lent",
		"fairshare_queue_gpu_hours_total",
		"fairshare_queue_gpu_idle_hours_total",
		"fairshare_queue_cost_usd_total",
		"fairshare_queue_pending_pods",
		"fairshare_preemptions_total",
		"fairshare_reclaims_total",
		"fairshare_scheduling_latency_seconds",
	} {
		if !strings.Contains(body, name) {
			t.Errorf("metric family %s missing from /metrics output", name)
		}
	}

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	byName := map[string]*dto.MetricFamily{}
	for _, f := range families {
		byName[f.GetName()] = f
	}
	for _, name := range []string{"fairshare_queue_gpus_allocated", "fairshare_queue_gpus_guaranteed", "fairshare_queue_cost_usd_total"} {
		f, ok := byName[name]
		if !ok || len(f.GetMetric()) == 0 {
			t.Fatalf("expected non-empty family %s", name)
		}
	}
	// The running pod of 3 GPUs must be reflected as allocated.
	alloc := byName["fairshare_queue_gpus_allocated"].GetMetric()
	nonZero := false
	for _, m := range alloc {
		if m.GetGauge().GetValue() > 0 {
			nonZero = true
		}
	}
	if !nonZero {
		t.Fatalf("expected at least one non-zero allocated value, got %v", alloc)
	}
}
