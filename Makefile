.PHONY: fmt test race vet check run

fmt:
	gofmt -w .

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

check: fmt test race vet

run:
	go run ./cmd/feishu-botd
