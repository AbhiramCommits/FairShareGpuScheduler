# Contributing

Thanks for your interest in FairShareGpuScheduler.

## Local development loop

Prerequisites: Go (the version in `go.mod`, built with the same major toolchain),
Docker, `kind`, `kubectl`, and `golangci-lint`.

```bash
# Regenerate deepcopy code and CRD manifests (must be committed)
make generate manifests

# Build the scheduler, controller and bench binaries
make build

# Unit tests with the race detector and coverage
make test

# Lint
make lint

# Deterministic benchmark (no GPUs required)
make bench

# Full end-to-end validation on kind (creates and tears down the cluster)
hack/e2e.sh
```

`make kind-up` creates a 3-worker kind cluster and runs `hack/fake-gpus.sh`,
which labels the workers and injects `nvidia.com/gpu` into their status so the
scheduler path is exercised without real hardware.

## Guidelines

- Keep `pkg/fairshare` and `pkg/topology` free of Kubernetes imports; they are
  the pure, unit-tested core shared by the plugin and the simulator.
- Add table-driven tests for behavior changes to the scheduling math.
- Run `make generate manifests` and commit the result; CI fails if generated
  code is stale.
- Never weaken an end-to-end assertion to make CI pass; fix the code instead.
- Every exported symbol needs a doc comment.

## Commit style

Use short, imperative subject lines (e.g. `feat: ...`, `fix: ...`, `test: ...`,
`docs: ...`). Keep generated artifacts and their source in the same commit.
