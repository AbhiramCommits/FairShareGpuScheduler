#!/usr/bin/env bash
set -e

echo "Patching fake GPUs and topology labels on Kind worker nodes..."
WORKERS=$(kubectl get nodes --selector='node-role.kubernetes.io/control-plane!=' -o jsonpath='{.items[*].metadata.name}')

for node in $WORKERS; do
  echo "Patching node: $node"
  kubectl label node "$node" fairshare.io/nvlink-group="group-0" --overwrite
  kubectl label node "$node" topology.kubernetes.io/numa-node="0" --overwrite
  kubectl patch node "$node" --type=json -p='[
    {"op": "add", "path": "/status/allocatable/nvidia.com~1gpu", "value": "8"},
    {"op": "add", "path": "/status/capacity/nvidia.com~1gpu", "value": "8"}
  ]' || true
done
echo "Kind nodes patched successfully with fake GPUs and topology labels."
