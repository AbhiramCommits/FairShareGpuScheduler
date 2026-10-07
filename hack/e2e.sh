#!/usr/bin/env bash
#
# End-to-end validation of the FairShare GPU scheduler on a local kind cluster.
# It exercises the real scheduler plugin and quota controller against fake GPUs
# and fails (non-zero exit) on any failed assertion.
#
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-fairshare-e2e}"
IMAGE="${IMAGE:-fairshare-gpu-scheduler:latest}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
POOL_GPUS="${POOL_GPUS:-24}"   # 3 workers x 8 fake GPUs

log()  { printf '\033[1;34m[e2e]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[e2e FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

cleanup() {
  if [[ "${KEEP_CLUSTER}" != "1" ]]; then
    log "tearing down kind cluster ${CLUSTER_NAME}"
    kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
  else
    log "KEEP_CLUSTER=1: leaving cluster ${CLUSTER_NAME} running"
  fi
}
trap cleanup EXIT

tenant_gpu() {
  # tenant_gpu <namespace>: sum of running pod GPU requests
  kubectl get pods -n "$1" -o json 2>/dev/null | python3 -c '
import json,sys
d=json.load(sys.stdin)
total=0
for p in d.get("items",[]):
    if p.get("status",{}).get("phase")!="Running":
        continue
    for c in p.get("spec",{}).get("containers",[]):
        req=c.get("resources",{}).get("requests",{}) or {}
        total+=int(req.get("nvidia.com/gpu","0"))
print(total)
'
}

wait_gpu_at_least() {
  local ns="$1" want="$2" timeout="${3:-180}" start
  start=$(date +%s)
  while true; do
    if (( $(tenant_gpu "$ns") >= want )); then return 0; fi
    if (( $(date +%s) - start > timeout )); then return 1; fi
    sleep 3
  done
}

log "creating kind cluster ${CLUSTER_NAME}"
kind delete cluster --name "${CLUSTER_NAME}" >/dev/null 2>&1 || true
kind create cluster --name "${CLUSTER_NAME}" --config "${ROOT_DIR}/hack/kind-cluster.yaml"

log "building and loading scheduler/controller image"
docker build -t "${IMAGE}" "${ROOT_DIR}"
kind load docker-image "${IMAGE}" --name "${CLUSTER_NAME}"

log "applying CRDs and RBAC"
kubectl apply -f "${ROOT_DIR}/config/crd/"
kubectl apply -f "${ROOT_DIR}/config/rbac/role.yaml"

log "patching fake GPUs and topology labels"
bash "${ROOT_DIR}/hack/fake-gpus.sh"

log "applying queues and bindings (before the scheduler starts)"
kubectl apply -f "${ROOT_DIR}/config/samples/namespaces.yaml"
kubectl apply -f "${ROOT_DIR}/config/samples/queues.yaml"

log "deploying scheduler and controller"
kubectl apply -f "${ROOT_DIR}/deploy/scheduler-deployment.yaml"
kubectl apply -f "${ROOT_DIR}/deploy/controller-deployment.yaml"

log "granting the scheduler/controller API permissions (dev cluster: cluster-admin)"
kubectl create clusterrolebinding fairshare-scheduler-admin \
  --clusterrole=cluster-admin --serviceaccount=kube-system:scheduler-sa --dry-run=client -o yaml | kubectl apply -f -
kubectl create clusterrolebinding fairshare-controller-admin \
  --clusterrole=cluster-admin --serviceaccount=kube-system:fairshare-controller --dry-run=client -o yaml | kubectl apply -f -

kubectl -n kube-system rollout status deploy/fairshare-controller --timeout=180s
kubectl -n kube-system rollout status deploy/fairshare-scheduler --timeout=180s

log "applying tenant baseline workload"
kubectl apply -f "${ROOT_DIR}/config/samples/burst.yaml"

log "waiting for tenant-a, tenant-b and tenant-c to schedule"
wait_gpu_at_least tenant-a 8 180 || fail "tenant-a did not schedule enough pods"
wait_gpu_at_least tenant-b 6 180 || fail "tenant-b did not schedule its guarantee"
wait_gpu_at_least tenant-c 1 180 || fail "tenant-c did not schedule any pods"

# ---- Assertion (a): per-tenant running GPU never exceeds guaranteed+borrowLimit
cap_for() {
  # guaranteed + borrowLimit per tenant (bash 3.2 compatible; no associative arrays)
  case "$1" in
    tenant-a) echo 36 ;;  # 12 + 24
    tenant-b) echo 30 ;;  #  6 + 24
    tenant-c) echo 27 ;;  #  3 + 24
    tenant-d) echo 27 ;;  #  3 + 24
    *) echo 0 ;;
  esac
}
for ns in tenant-a tenant-b tenant-c tenant-d; do
  running=$(tenant_gpu "$ns")
  cap=$(cap_for "$ns")
  if (( running > cap )); then
    fail "assertion (a): ${ns} runs ${running} GPUs > guaranteed+borrowLimit ${cap}"
  fi
  if (( running > POOL_GPUS )); then
    fail "assertion (a): ${ns} runs ${running} GPUs > pool ${POOL_GPUS}"
  fi
  log "assertion (a): ${ns} running=${running} <= ${cap}"
done

# Wait until tenant-b has borrowed idle capacity beyond its guarantee.
log "waiting for tenant-b to borrow idle capacity"
wait_gpu_at_least tenant-b 8 180 || fail "tenant-b did not borrow idle capacity"
b_before=$(tenant_gpu tenant-b)
log "tenant-b is borrowing: running=${b_before} GPUs (> its 6 guarantee)"

# ---- Assertion (b): high-priority tenant-d schedules by preempting a borrower
log "applying tenant-d high-priority burst"
kubectl apply -f "${ROOT_DIR}/config/samples/burst-d.yaml"
start=$(date +%s)
d_running=0
b_now="${b_before}"
while (( $(date +%s) - start < 240 )); do
  d_running=$(tenant_gpu tenant-d)
  b_now=$(tenant_gpu tenant-b)
  if (( d_running >= 4 )) && (( b_now < b_before )); then break; fi
  sleep 3
done
if (( d_running < 4 )); then
  kubectl -n tenant-d get pods -o wide || true
  fail "assertion (b): tenant-d did not get scheduled via preemption/reclaim"
fi
if (( b_now >= b_before )); then
  kubectl -n tenant-b get pods -o wide || true
  fail "assertion (b): no borrowing tenant was preempted (b stayed at ${b_now})"
fi
log "assertion (b): tenant-d running=${d_running}; tenant-b reduced ${b_before} -> ${b_now} via preemption"

# ---- Assertion (d): cost metric is non-zero for every tenant. Scraped while
# tenant-d is still running so every tenant has accrued GPU cost.
log "scraping FinOps metrics"
kubectl -n kube-system port-forward svc/fairshare-controller 19090:9090 >/dev/null 2>&1 &
PF_PID=$!
sleep 5
metrics=$(curl -sf http://127.0.0.1:19090/metrics || true)
kill "${PF_PID}" >/dev/null 2>&1 || true
if [[ -z "${metrics}" ]]; then
  fail "assertion (d): could not scrape /metrics from the controller"
fi
for ns in tenant-a tenant-b tenant-c tenant-d; do
  val=$(printf '%s\n' "${metrics}" | grep -E "^fairshare_queue_cost_usd_total\{queue=\"${ns}\"\}" | awk '{print $2}' | head -1)
  if [[ -z "${val}" || "${val}" == "0" || "${val}" == "0.0" ]]; then
    fail "assertion (d): fairshare_queue_cost_usd_total for ${ns} is ${val:-missing}"
  fi
  log "assertion (d): ${ns} cost_usd_total=${val}"
done

# ---- Assertion (c): preempted borrower pods come back after the burst drains
log "draining tenant-d burst"
kubectl delete deployment burst-d -n tenant-d --ignore-not-found
start=$(date +%s)
restored=0
while (( $(date +%s) - start < 300 )); do
  b_after=$(tenant_gpu tenant-b)
  if (( b_after >= b_before )); then restored=1; break; fi
  sleep 3
done
if (( restored != 1 )); then
  kubectl -n tenant-b get pods -o wide || true
  fail "assertion (c): tenant-b did not reclaim its borrowed GPUs after the burst drained"
fi
log "assertion (c): tenant-b restored to ${b_after} GPUs"

log "ALL E2E ASSERTIONS PASSED"
