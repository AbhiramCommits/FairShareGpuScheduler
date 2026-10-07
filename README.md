# FairShareGpuScheduler

FairShareGpuScheduler multiplexes a shared GPU pool across tenants in a single
Kubernetes cluster. It is a real out-of-tree `kube-scheduler` plugin plus a
controller-runtime quota controller. Every tenant gets a **guaranteed** floor of
GPUs, **weighted fair sharing** above that floor (weighted DRF), **elastic
borrowing** of idle capacity with **reclaim** when the owner needs it back,
**priority preemption** that never touches guaranteed capacity, and
**topology-aware placement** (same-node packing, NVLink group, NUMA node). A
FinOps layer converts per-tenant GPU-seconds into GPU-hours and dollars and
exports it to Prometheus for chargeback.

## Architecture

```
                       ┌───────────────────────────────────────────────┐
   kubectl / users ──▶ │ Kubernetes API: Queue, QueueBinding, Pods      │
                       └───────────────┬───────────────────────────────┘
                                       │ watch
             ┌─────────────────────────┴──────────────────────────┐
             ▼                                                     ▼
 ┌───────────────────────────────┐                   ┌───────────────────────────────┐
 │ Scheduler plugin              │                   │ Quota controller              │
 │ cmd/scheduler (2nd scheduler) │                   │ cmd/controller                │
 │                               │                   │                               │
 │ PreFilter  → resolve queue +  │                   │ reconcile Queue status        │
 │              CanAdmit         │                   │ lend idle guaranteed GPUs     │
 │ Filter     → free GPU check   │                   │ reclaim via Eviction API      │
 │ Score      → topology.Score   │                   │ expose FinOps metrics :9090   │
 │ PostFilter → victim selection │                   └──────────────┬────────────────┘
 │ Reserve    → allocate (mutex) │                                  │
 └───────────────┬───────────────┘                                  │
                 │ same pure core                                   │
                 ▼                                                  ▼
        ┌───────────────────────────────┐                ┌───────────────────────────┐
        │ pkg/fairshare                 │                │ pkg/finops                │
        │ BuildTree / CanAdmit /        │                │ Prometheus metrics        │
        │ RankQueues (weighted DRF) /   │                └───────────────────────────┘
        │ StarvationGuard               │
        ├───────────────────────────────┤
        │ pkg/topology ScoreNode        │◀── also used by pkg/sim (benchmark)
        ├───────────────────────────────┤
        │ pkg/sim deterministic engine  │──▶ results/benchmark.{json,md}
        └───────────────────────────────┘
```

Data flow for a pod:

```
Pod (schedulerName=fairshare-scheduler)
  └─ label fairshare.io/queue=tenant-a  ──or──▶ QueueBinding (namespace / selectors)
        └─ Queue node in the hierarchy ──▶ CanAdmit(guaranteed → borrow) ──▶ Filter/Score/Reserve
```

### CRDs

- `Queue` (cluster-scoped): `spec.parent`, `spec.weight`, `spec.guaranteed`,
  `spec.borrowLimit`, `spec.priorityClass`, `spec.reclaimable`,
  `spec.preemptible`; status: `allocated`, `borrowed`, `lent`, `pendingPods`,
  `shareRatio`, `lastReclaimTime`.
- `QueueBinding` (namespaced): maps a namespace and/or pod label selector to a
  `Queue`.

A pod resolves to a queue by, in order: the `fairshare.io/queue` pod label, then
a `QueueBinding` whose namespace/pod selectors match.

## Running it

```bash
# 1. Local cluster with fake GPUs (1 control-plane + 3 workers)
make kind-up                 # creates the cluster and runs hack/fake-gpus.sh

# 2. Build and load the scheduler + controller image
docker build -t fairshare-gpu-scheduler:latest .
kind load docker-image fairshare-gpu-scheduler:latest --name fairshare-cluster

# 3. Apply CRDs, RBAC, queues and demo workload
kubectl apply -f config/crd/ -f config/rbac/role.yaml
kubectl apply -f config/samples/namespaces.yaml -f config/samples/queues.yaml
kubectl apply -f config/samples/burst.yaml
kubectl apply -f deploy/scheduler-deployment.yaml -f deploy/controller-deployment.yaml

# 4. Benchmark (deterministic, no GPUs required)
make bench                   # writes results/benchmark.json, .md and summary.txt

# 5. Full end-to-end validation (creates and tears down its own kind cluster)
hack/e2e.sh
```

Metrics are served by the controller on `:9090/metrics`:

```bash
kubectl -n kube-system port-forward svc/fairshare-controller 9090:9090
curl localhost:9090/metrics
```

The Grafana chargeback dashboard is `dashboards/chargeback.json` (per-tenant
GPU-hours, guaranteed vs borrowed cost split, cluster allocation ratio, queue
wait-time p50/p95, fairness deviation, preemptions/reclaims, pending pods).

## Results

All figures below are copied from `results/benchmark.json`, produced by
`make bench` over the deterministic trace `testdata/trace.json` (seed
`20240117`): **850 jobs**, 4 tenants with entitlement weights **4:2:1:1**, three
priority classes, job sizes 1/2/4/8 GPUs, bursty arrivals, a **32-GPU pool
across 4 nodes**, and a fixed **24-hour** scheduling window.

| Metric | FIFO | default-binpack | fairshare | fairshare vs FIFO | fairshare vs binpack |
|---|---:|---:|---:|---:|---:|
| GPU allocation ratio (mean) | 0.999 | 0.999 | 0.989 | -1.0% | -1.0% |
| GPU allocation ratio (peak) | 1.000 | 1.000 | 1.000 | +0.0% | +0.0% |
| Queue wait p50 (s) | 14525.9 | 42445.1 | 1428.3 | **-90.2%** | **-96.6%** |
| Queue wait p95 (s) | 62197.0 | 76753.5 | 21735.2 | **-65.1%** | **-71.7%** |
| Makespan (s) | 88493.5 | 89451.1 | 89042.5 | +0.6% | -0.5% |
| Fairness deviation (max abs share - entitlement) | 0.5109 | 0.8750 | 0.1460 | **-71.4%** | **-83.3%** |
| Jain fairness index | 0.5524 | 0.2500 | 0.9062 | **+64.0%** | **+262.5%** |
| Preemptions | 0 | 0 | 17 | n/a | n/a |
| Reclaims | 0 | 0 | 1 | n/a | n/a |
| Starvation max wait, lowest-priority tenant (s) | 61636.4 | 85909.9 | 18222.1 | **-70.4%** | **-78.8%** |
| Jobs completed within 24 h | 331 | 123 | 456 | **+37.8%** | **+270.7%** |

Tenant GPU-second share vs weighted entitlement (fairshare converges toward the
4:2:1:1 target; FIFO lets the greedy high-demand tenant dominate):

| Tenant | entitled | FIFO | default-binpack | fairshare |
|---|---:|---:|---:|---:|
| tenant-a | 50.0% | 17.0% | 0.0% | 36.3% |
| tenant-b | 25.0% | 11.1% | 0.0% | 22.5% |
| tenant-c | 12.5% | 8.2% | 0.0% | 14.1% |
| tenant-d | 12.5% | 63.6% | 100.0% | 27.1% |

Reproduce with `make bench`; the harness is deterministic (a test asserts two
runs with the same seed produce byte-identical JSON).

### Test coverage and end-to-end validation

- `go test ./... -race -coverprofile=coverage.out`: total statement coverage
  **65.1%** (packages: `pkg/sim` 92.8%, `pkg/controller` 87.1%,
  `pkg/topology` 87.5%, `pkg/fairshare` 84.1%, `pkg/plugin` 62.1%).
- `hack/e2e.sh` exits `0`; **all 4 assertion groups passed** on a real kind
  cluster (1 control-plane + 3 workers, 24 fake GPUs):
  - (a) every tenant stayed within `guaranteed + borrowLimit`
    (`tenant-a=12`, `tenant-b=10`, `tenant-c=2`, `tenant-d=0`).
  - (b) high-priority `tenant-d` was scheduled (4 GPUs) by preempting/reclaiming
    a borrower: `tenant-b` fell from `10` to `6` GPUs.
  - (c) after the burst drained, `tenant-b` was restored to `10` GPUs.
  - (d) `fairshare_queue_cost_usd_total` was non-zero for every tenant
    (`tenant-a=0.25`, `tenant-b=0.375`, `tenant-c=0.0278`, `tenant-d=0.0556`).

## Limitations

- **Fake GPUs on kind.** Capacity is injected as an extended resource
  (`nvidia.com/gpu`) on node status plus topology labels; there is no real
  device plugin or GPU isolation.
- **Single cluster.** Quota, lending and reclaim operate within one cluster;
  there is no federation or multi-cluster admission.
- **Simulated trace benchmark.** The benchmark numbers come from the discrete
  event simulator in `pkg/sim`, which reuses the same `pkg/fairshare` and
  `pkg/topology` code as the plugin but does not model every kube-scheduler
  detail. The kind e2e validates the real scheduling path separately.

## License

MIT — see [LICENSE](./LICENSE).
