.PHONY: test
test:
	go test ./... -race -coverprofile=coverage.out -covermode=atomic