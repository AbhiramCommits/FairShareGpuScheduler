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

package finops

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	QueueGPUsAllocated = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "fairshare_queue_gpus_allocated",
			Help: "Number of GPUs currently allocated to a queue.",
		},
		[]string{"queue"},
	)

	QueueGPUsGuaranteed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "fairshare_queue_gpus_guaranteed",
			Help: "Number of guaranteed GPUs for a queue.",
		},
		[]string{"queue"},
	)

	QueueGPUsBorrowed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "fairshare_queue_gpus_borrowed",
			Help: "Number of borrowed GPUs for a queue.",
		},
		[]string{"queue"},
	)

	QueueGPUsLent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "fairshare_queue_gpus_lent",
			Help: "Number of lent GPUs for a queue.",
		},
		[]string{"queue"},
	)

	QueueGpuHoursTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fairshare_queue_gpu_hours_total",
			Help: "Total cumulative GPU-hours consumed by a queue.",
		},
		[]string{"queue"},
	)

	QueueGpuIdleHoursTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fairshare_queue_gpu_idle_hours_total",
			Help: "Total cumulative idle guaranteed GPU-hours.",
		},
		[]string{"queue"},
	)

	QueueCostUsdTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fairshare_queue_cost_usd_total",
			Help: "Total estimated GPU cost in USD based on GPU-hours and hourly rate.",
		},
		[]string{"queue"},
	)

	QueuePendingPods = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "fairshare_queue_pending_pods",
			Help: "Number of pending pods in a queue.",
		},
		[]string{"queue"},
	)

	PreemptionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fairshare_preemptions_total",
			Help: "Total number of preemption events.",
		},
		[]string{"queue", "reason"},
	)

	ReclaimsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fairshare_reclaims_total",
			Help: "Total number of resource reclaim events.",
		},
		[]string{"queue"},
	)

	SchedulingLatencySeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "fairshare_scheduling_latency_seconds",
			Help:    "Scheduler scheduling decision latency in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
)

func init() {
	prometheus.MustRegister(
		QueueGPUsAllocated,
		QueueGPUsGuaranteed,
		QueueGPUsBorrowed,
		QueueGPUsLent,
		QueueGpuHoursTotal,
		QueueGpuIdleHoursTotal,
		QueueCostUsdTotal,
		QueuePendingPods,
		PreemptionsTotal,
		ReclaimsTotal,
		SchedulingLatencySeconds,
	)
}

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
