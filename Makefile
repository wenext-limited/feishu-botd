.PHONY: fmt test race vet check build image run

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
