# FairShare GPU Scheduler Benchmark

Deterministic trace `testdata/trace.json` (seed `20240117`), 850 jobs, 32-GPU pool across 4 nodes, 24 simulated hours.

## Headline metrics

| Metric | FIFO | default-binpack | fairshare | fairshare vs FIFO | fairshare vs binpack |
|---|---:|---:|---:|---:|---:|
| GPU allocation ratio (mean) | 0.999 | 0.999 | 0.989 | -1.0% | -1.0% |
| GPU allocation ratio (peak) | 1.000 | 1.000 | 1.000 | +0.0% | +0.0% |
| Queue wait p50 (s) | 14525.9 | 42445.1 | 1428.3 | -90.2% | -96.6% |
| Queue wait p95 (s) | 62197.0 | 76753.5 | 21735.2 | -65.1% | -71.7% |
| Makespan (s) | 88493.5 | 89451.1 | 89042.5 | +0.6% | -0.5% |
| Fairness deviation | 0.5109 | 0.8750 | 0.1460 | -71.4% | -83.3% |
| Jain fairness index | 0.5524 | 0.2500 | 0.9062 | +64.0% | +262.5% |
| Preemptions | 0 | 0 | 17 | n/a | n/a |
| Reclaims | 0 | 0 | 1 | n/a | n/a |
| Starvation max wait (s) | 61636.4 | 85909.9 | 18222.1 | -70.4% | -78.8% |
| Jobs completed | 331 | 123 | 456 | +37.8% | +270.7% |

## Per-tenant queue wait (fairshare)

| Tenant | p50 (s) | p95 (s) | max (s) | jobs |
|---|---:|---:|---:|---:|
| tenant-a | 784.3 | 10557.7 | 17688.3 | 212 |
| tenant-b | 1043.6 | 9601.4 | 30436.8 | 133 |
| tenant-c | 3606.7 | 19853.9 | 39908.3 | 94 |
| tenant-d | 10491.9 | 75363.9 | 82314.9 | 48 |

## Tenant GPU-second share vs weighted entitlement

| Tenant | entitled | FIFO | default-binpack | fairshare |
|---|---:|---:|---:|---:|
| tenant-a | 50.0% | 17.0% | 0.0% | 36.3% |
| tenant-b | 25.0% | 11.1% | 0.0% | 22.5% |
| tenant-c | 12.5% | 8.2% | 0.0% | 14.1% |
| tenant-d | 12.5% | 63.6% | 100.0% | 27.1% |
