devsetup:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0

test:
	go test -race ./...

cover:
	go test -coverprofile=cover.out ./...

infra:
	docker compose up

lint:
	golangci-lint run -v

check: lint
	go test -cover -race ./...

.PHONY: all test devsetup lint cover check
