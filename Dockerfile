# Build stage
FROM golang:1.26 AS builder
WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o bin/scheduler ./cmd/scheduler && \
    CGO_ENABLED=0 go build -o bin/controller ./cmd/controller && \
    CGO_ENABLED=0 go build -o bin/bench ./cmd/bench

# Runtime stage
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/bin/scheduler /bin/scheduler
COPY --from=builder /workspace/bin/controller /bin/controller
USER 65532:65532
