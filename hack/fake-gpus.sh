#!/usr/bin/env bash
set -e

# Patch fake GPU capacity and topology labels onto Kind worker nodes so the
# scheduler path is exercised end to end without real GPUs.
echo "Patching fake GPUs and topology labels on Kind worker nodes..."
WORKERS=$(kubectl get nodes --selector='node-role.kubernetes.io/control-plane!=' -o jsonpath='{.items[*].metadata.name}')

for node in $WORKERS; do
  echo "Patching node: $node"
  kubectl label node "$node" fairshare.io/nvlink-group="group-0" --overwrite
  kubectl label node "$node" topology.kubernetes.io/numa-node="0" --overwrite
  # Extended resources live in the node status subresource; use --subresource=status
  # so the API server accepts the patch.
  kubectl patch node "$node" --subresource=status --type=json -p '[
    {"op": "add", "path": "/status/allocatable/nvidia.com~1gpu", "value": "8"},
    {"op": "add", "path": "/status/capacity/nvidia.com~1gpu", "value": "8"}
  ]' 2>/dev/null \
  || kubectl patch node "$node" --subresource=status --type=json -p '[
    {"op": "replace", "path": "/status/allocatable/nvidia.com~1gpu", "value": "8"},
    {"op": "replace", "path": "/status/capacity/nvidia.com~1gpu", "value": "8"}
  ]'
done
echo "Kind nodes patched successfully with fake GPUs and topology labels."
