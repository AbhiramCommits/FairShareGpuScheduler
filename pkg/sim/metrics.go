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

package sim

import (
	"fmt"
	"sort"
)

func nodeName(i int) string { return fmt.Sprintf("node-%d", i) }

func itoa(i int) string { return fmt.Sprintf("%d", i) }

// percentileOf returns the p50 and p95 of a sorted-internally slice.
func percentileOf(vals []float64) (p50, p95 float64) {
	if len(vals) == 0 {
		return 0, 0
	}
	s := make([]float64, len(vals))
	copy(s, vals)
	sort.Float64s(s)
	return quantile(s, 0.50), quantile(s, 0.95)
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := q * float64(len(sorted)-1)
	lo := int(idx)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

func allWaits(byTenant map[string][]float64) []float64 {
	var out []float64
	keys := make([]string, 0, len(byTenant))
	for k := range byTenant {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, byTenant[k]...)
	}
	return out
}

func summarizeWaits(byTenant map[string][]float64) map[string]TenantWait {
	out := map[string]TenantWait{}
	for tenant, waits := range byTenant {
		p50, p95 := percentileOf(waits)
		max := 0.0
		for _, w := range waits {
			if w > max {
				max = w
			}
		}
		out[tenant] = TenantWait{P50: p50, P95: p95, Max: max, Jobs: len(waits)}
	}
	return out
}

// fairness computes the maximum absolute deviation between each tenant's actual
// share of consumed GPU-seconds and its weighted entitlement, plus Jain's index.
func fairness(trace Trace, tenantGpuSeconds map[string]float64) (deviation, jain float64) {
	deviation, jain, _ = fairnessDetailed(trace, tenantGpuSeconds)
	return deviation, jain
}

// fairnessDetailed additionally returns each tenant's actual GPU-second share.
func fairnessDetailed(trace Trace, tenantGpuSeconds map[string]float64) (deviation, jain float64, shares map[string]float64) {
	shares = map[string]float64{}
	total := 0.0
	for _, v := range tenantGpuSeconds {
		total += v
	}
	n := len(trace.Tenants)
	if total <= 0 || n == 0 {
		return 0, 0, shares
	}
	totalWeight := int64(0)
	for _, t := range trace.Tenants {
		totalWeight += t.Weight
	}
	if totalWeight == 0 {
		return 0, 0, shares
	}

	sum := 0.0
	sumSq := 0.0
	for _, t := range trace.Tenants {
		actual := tenantGpuSeconds[t.Name] / total
		shares[t.Name] = actual
		entitled := float64(t.Weight) / float64(totalWeight)
		if d := actual - entitled; d > 0 {
			if d > deviation {
				deviation = d
			}
		} else if -d > deviation {
			deviation = -d
		}
		sum += actual
		sumSq += actual * actual
	}
	if sumSq > 0 {
		jain = (sum * sum) / (float64(n) * sumSq)
	}
	return deviation, jain, shares
}

// starvation returns the lowest-priority tenant and its maximum wait, counting
// pods still pending at the end of the trace.
func starvation(trace Trace, waitsByTenant map[string][]float64, pendings []pending, now float64) (string, float64) {
	if len(trace.Tenants) == 0 {
		return "", 0
	}
	lowest := trace.Tenants[0]
	for _, t := range trace.Tenants {
		if t.PriorityClass < lowest.PriorityClass {
			lowest = t
		}
	}
	max := 0.0
	for _, w := range waitsByTenant[lowest.Name] {
		if w > max {
			max = w
		}
	}
	for _, p := range pendings {
		if p.job.Tenant == lowest.Name {
			if w := now - p.arrival; w > max {
				max = w
			}
		}
	}
	return lowest.Name, max
}
