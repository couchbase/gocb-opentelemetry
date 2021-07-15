devsetup:
	go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.39.0

test:
	go test -race ./

cover:
	go test -coverprofile=cover.out ./

lint:
	golangci-lint run -v

check: lint
	go test -cover -race ./

.PHONY: all test devsetup lint cover check
