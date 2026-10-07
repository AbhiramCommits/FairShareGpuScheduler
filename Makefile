# Image URL to use all building/pushing image targets
IMG ?= fairshare-gpu-scheduler:latest

# Get the currently used golang install (in CI, etc.)
GO ?= go
KUBECTL ?= kubectl
KIND ?= kind

.PHONY: all
all: build

.PHONY: generate
generate:
	# Running controller-gen object
	go run sigs.k8s.io/controller-tools/cmd/controller-gen object:headerFile="hack/boilerplate.go.txt" paths="./api/..."

.PHONY: manifests
manifests:
	# Running controller-gen crd
	go run sigs.k8s.io/controller-tools/cmd/controller-gen crd paths="./api/..." output:crd:artifacts:config=config/crd

.PHONY: build
build: generate manifests
	$(GO) build -o bin/scheduler cmd/scheduler/main.go
	$(GO) build -o bin/controller cmd/controller/main.go
	$(GO) build -o bin/bench cmd/bench/main.go

.PHONY: test
test: generate
	$(GO) test ./... -race -coverprofile=coverage.out

.PHONY: lint
lint:
	golangci-lint run || echo "golangci-lint not installed or found warnings"

.PHONY: kind-up
kind-up:
	$(KIND) create cluster --config hack/kind-cluster.yaml
	./hack/fake-gpus.sh

.PHONY: kind-down
kind-down:
	$(KIND) delete cluster --name fairshare-cluster

.PHONY: bench
bench:
	$(GO) run hack/trace-gen.go
	$(GO) run cmd/bench/main.go

.PHONY: trace
trace:
	$(GO) run hack/trace-gen.go
