# buf and the protoc-gen-go* plugins are installed under $(go env GOPATH)/bin,
# which is not always on PATH in non-interactive shells. The proto targets put
# it on PATH so buf can find both itself and the plugins.
GOPATH_BIN := $(shell go env GOPATH)/bin

.PHONY: fmt test race vet check build image run proto proto-lint proto-check

fmt:
	gofmt -w .

build:
	go build -trimpath -o bin/feishu-botd ./cmd/feishu-botd

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

check: fmt test race vet

image:
	docker build -t feishu-botd:latest .

run:
	go run ./cmd/feishu-botd

# Regenerate Go bindings from proto/. The output under gen/ is committed, so
# this is only needed when the .proto files change.
proto:
	PATH="$(GOPATH_BIN):$$PATH" buf generate
	gofmt -w gen

proto-lint:
	PATH="$(GOPATH_BIN):$$PATH" buf lint

# Fail if the committed generated code is stale relative to the .proto files.
proto-check: proto
	git diff --exit-code gen
