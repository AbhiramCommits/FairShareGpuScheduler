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
	"sort"
	"time"

	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	fairsharev1alpha1 "github.com/abhiramkasireddi/fairshare-gpu-scheduler/api/v1alpha1"
	"github.com/abhiramkasireddi/fairshare-gpu-scheduler/pkg/finops"
)

// QueueReconciler reconciles Queue objects: it recomputes status, lends idle
// guaranteed GPUs to borrowing siblings, reclaims them when the owner needs
// capacity and emits FinOps metrics.
type QueueReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	GpuHourlyRate   float64
	ReclaimGrace    time.Duration
	ReconcilePeriod time.Duration
}

// queuePod is a pod accounted to a queue for reclaim decisions.
type queuePod struct {
	Name      string
	Namespace string
	Queue     string
	GPUs      int
	Running   bool
	StartTime time.Time
}

// +kubebuilder:rbac:groups=fairshare.io,resources=queues,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fairshare.io,resources=queues/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=policy,resources=pods/eviction,verbs=create;delete

// Reconcile implements the Queue reconciliation loop.
func (r *QueueReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	start := time.Now()
	defer func() {
		finops.SchedulingLatencySeconds.WithLabelValues("reconcile").Observe(time.Since(start).Seconds())
	}()

	var queue fairsharev1alpha1.Queue
	if err := r.Get(ctx, req.NamespacedName, &queue); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	var podList v1.PodList
	if err := r.List(ctx, &podList); err != nil {
		return ctrl.Result{}, err
	}

	allocatedGPUs := 0.0
	pendingCount := int32(0)
	pendingGPUs := 0.0
	var queuePods []queuePod

	for _, pod := range podList.Items {
		qName := pod.Labels["fairshare.io/queue"]
		if qName != queue.Name {
			continue
		}
		podGPUs := 0
		for _, c := range pod.Spec.Containers {
			if val, ok := c.Resources.Requests[v1.ResourceName("nvidia.com/gpu")]; ok {
				podGPUs += int(val.Value())
			}
		}
		switch pod.Status.Phase {
		case v1.PodRunning:
			allocatedGPUs += float64(podGPUs)
		case v1.PodPending:
			pendingCount++
			pendingGPUs += float64(podGPUs)
		}
		var startTime time.Time
		if pod.Status.StartTime != nil {
			startTime = pod.Status.StartTime.Time
		}
		queuePods = append(queuePods, queuePod{
			Name: pod.Name, Namespace: pod.Namespace, Queue: qName,
			GPUs: podGPUs, Running: pod.Status.Phase == v1.PodRunning, StartTime: startTime,
		})
	}

	guaranteedGPUs := 0.0
	if val, ok := queue.Spec.Guaranteed[v1.ResourceName("nvidia.com/gpu")]; ok {
		guaranteedGPUs = float64(val.Value())
	}

	borrowed, lent := computeBorrowLend(allocatedGPUs, guaranteedGPUs, queue.Spec.Reclaimable)

	queue.Status.Allocated = gpuList(allocatedGPUs)
	queue.Status.Borrowed = gpuList(borrowed)
	queue.Status.Lent = gpuList(lent)
	queue.Status.PendingPods = pendingCount
	queue.Status.ShareRatio = shareRatio(allocatedGPUs, guaranteedGPUs)

	// Reclaim: if this queue has pending pods, evict newest borrowed pods from
	// over-quota, reclaimable sibling queues until enough capacity is freed.
	if pendingCount > 0 {
		reclaimed, err := r.reclaimForQueue(ctx, log.FromContext(ctx), queue.Name, pendingGPUs)
		if err != nil {
			logger.Error(err, "reclaim failed")
		} else if reclaimed > 0 {
			now := metav1.Now()
			queue.Status.LastReclaimTime = &now
			finops.ReclaimsTotal.WithLabelValues(queue.Name).Add(float64(reclaimed))
		}
	}

	if err := r.Status().Update(ctx, &queue); err != nil && !apierrors.IsConflict(err) {
		logger.Error(err, "failed to update queue status")
		return ctrl.Result{}, err
	}

	r.publishMetrics(queue.Name, allocatedGPUs, guaranteedGPUs, borrowed, lent, pendingCount)

	period := r.ReconcilePeriod
	if period <= 0 {
		period = 10 * time.Second
	}
	return ctrl.Result{RequeueAfter: period}, nil
}

// reclaimForQueue evicts newest borrowed pods from over-quota queues.
func (r *QueueReconciler) reclaimForQueue(ctx context.Context, logger interface{ Error(error, string, ...interface{}) }, queueName string, needGPUs float64) (int, error) {
	var queues fairsharev1alpha1.QueueList
	if err := r.List(ctx, &queues); err != nil {
		return 0, err
	}

	var podList v1.PodList
	if err := r.List(ctx, &podList); err != nil {
		return 0, err
	}

	byQueue := map[string][]queuePod{}
	for _, pod := range podList.Items {
		q := pod.Labels["fairshare.io/queue"]
		if q == "" || q == queueName || pod.Status.Phase != v1.PodRunning {
			continue
		}
		g := 0
		for _, c := range pod.Spec.Containers {
			if val, ok := c.Resources.Requests[v1.ResourceName("nvidia.com/gpu")]; ok {
				g += int(val.Value())
			}
		}
		var st time.Time
		if pod.Status.StartTime != nil {
			st = pod.Status.StartTime.Time
		}
		byQueue[q] = append(byQueue[q], queuePod{Name: pod.Name, Namespace: pod.Namespace, Queue: q, GPUs: g, Running: true, StartTime: st})
	}

	reclaimed := 0
	for _, q := range queues.Items {
		if !q.Spec.Reclaimable || !q.Spec.Preemptible {
			continue
		}
		guaranteed := 0.0
		if val, ok := q.Spec.Guaranteed[v1.ResourceName("nvidia.com/gpu")]; ok {
			guaranteed = float64(val.Value())
		}
		pods := byQueue[q.Name]
		total := 0.0
		for _, p := range pods {
			total += float64(p.GPUs)
		}
		over := total - guaranteed // only resources above guarantee are reclaimable
		if over <= 0 {
			continue
		}
		victims := selectReclaimVictims(pods, guaranteed, needGPUs-float64(reclaimed))
		for _, v := range victims {
			if err := r.evictPod(ctx, v); err != nil {
				logger.Error(err, "failed to evict borrowed pod", "pod", v.Name)
				continue
			}
			reclaimed += v.GPUs
		}
		if float64(reclaimed) >= needGPUs {
			break
		}
	}
	return reclaimed, nil
}

// selectReclaimVictims returns newest-first pods from a borrower queue whose
// cumulative allocation is above the queue's guaranteed floor. This never
// selects a pod that would drop the queue below its guarantee.
func selectReclaimVictims(pods []queuePod, guaranteed, need float64) []queuePod {
	sorted := make([]queuePod, len(pods))
	copy(sorted, pods)
	sort.SliceStable(sorted, func(i, j int) bool {
		if !sorted[i].StartTime.Equal(sorted[j].StartTime) {
			return sorted[i].StartTime.After(sorted[j].StartTime)
		}
		return sorted[i].Name < sorted[j].Name
	})

	total := 0.0
	for _, p := range sorted {
		total += float64(p.GPUs)
	}
	reclaimable := total - guaranteed
	if reclaimable <= 0 {
		return nil
	}
	if need > reclaimable {
		need = reclaimable
	}

	var victims []queuePod
	freed := 0.0
	for _, p := range sorted {
		if freed >= need {
			break
		}
		// Stop before dropping the queue below its guarantee.
		if total-float64(p.GPUs) < guaranteed {
			continue
		}
		victims = append(victims, p)
		total -= float64(p.GPUs)
		freed += float64(p.GPUs)
	}
	return victims
}

func (r *QueueReconciler) evictPod(ctx context.Context, p queuePod) error {
	pod := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: p.Namespace}}
	grace := int64(0)
	if r.ReclaimGrace > 0 {
		grace = int64(r.ReclaimGrace.Seconds())
	}
	err := r.SubResource("eviction").Create(ctx, pod, &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: p.Namespace},
		DeleteOptions: &metav1.DeleteOptions{
			GracePeriodSeconds: &grace,
		},
	})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		// Fall back to deletion when the Eviction API is unavailable.
		if derr := r.Delete(ctx, pod); derr != nil {
			return derr
		}
	}
	finops.PreemptionsTotal.WithLabelValues(p.Queue, "reclaim").Inc()
	return nil
}

func (r *QueueReconciler) publishMetrics(queue string, allocated, guaranteed, borrowed, lent float64, pending int32) {
	finops.QueueGPUsAllocated.WithLabelValues(queue).Set(allocated)
	finops.QueueGPUsGuaranteed.WithLabelValues(queue).Set(guaranteed)
	finops.QueueGPUsBorrowed.WithLabelValues(queue).Set(borrowed)
	finops.QueueGPUsLent.WithLabelValues(queue).Set(lent)
	finops.QueuePendingPods.WithLabelValues(queue).Set(float64(pending))

	period := r.ReconcilePeriod
	if period <= 0 {
		period = 10 * time.Second
	}
	elapsedHours := period.Hours()
	finops.QueueGpuHoursTotal.WithLabelValues(queue).Add(allocated * elapsedHours)
	if lent > 0 {
		finops.QueueGpuIdleHoursTotal.WithLabelValues(queue).Add(lent * elapsedHours)
	}
	rate := r.GpuHourlyRate
	if rate <= 0 {
		rate = 2.50
	}
	finops.QueueCostUsdTotal.WithLabelValues(queue).Add(allocated * elapsedHours * rate)
}

func computeBorrowLend(allocated, guaranteed float64, reclaimable bool) (borrowed, lent float64) {
	if allocated > guaranteed {
		return allocated - guaranteed, 0
	}
	if reclaimable {
		return 0, guaranteed - allocated
	}
	return 0, 0
}

func shareRatio(allocated, guaranteed float64) string {
	if guaranteed <= 0 {
		return "0.00"
	}
	return resource.NewQuantity(int64((allocated/guaranteed)*100), resource.DecimalSI).String()
}

func gpuList(v float64) v1.ResourceList {
	return v1.ResourceList{v1.ResourceName("nvidia.com/gpu"): resource.MustParse(formatFloat(v))}
}

func formatFloat(v float64) string {
	if v < 0 {
		v = 0
	}
	return resource.NewMilliQuantity(int64(v*1000), resource.DecimalSI).String()
}

// SetupWithManager registers the reconciler with the controller-runtime manager.
func (r *QueueReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fairsharev1alpha1.Queue{}).
		Complete(r)
}
