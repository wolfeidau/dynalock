ci: deps lint test ##=> Run all CI targets
.PHONY: ci

lint: ##=> Lint all the things
	@echo "--- lint all the things"
	@golangci-lint run
.PHONY: lint

clean: ##=> Clean all the things
	$(info [+] Clean all the things...")
.PHONY: clean

deps: ##=> Intall all the dependencies to build
	$(info [+] Installing deps...")
	@GO111MODULE=off go get -v -u github.com/golangci/golangci-lint/cmd/golangci-lint
.PHONY: deps

test: ##=> Run the tests
	$(info [+] Run tests...")
	@go test -v -cover ./...
.PHONY: test