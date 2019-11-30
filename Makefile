ci: deps lint test ##=> Run all CI targets
.PHONY: ci

lint: ##=> Lint all the things
	@echo "--- lint all the things"
	@$(shell pwd)/.bin/golangci-lint run
.PHONY: lint

clean: ##=> Clean all the things
	$(info [+] Clean all the things...")
.PHONY: clean

deps: ##=> Intall all the dependencies to build
	$(info [+] Installing deps...")
	@curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b $(shell pwd)/.bin v1.21.0
.PHONY: deps

test: ##=> Run the tests
	$(info [+] Run tests...")
	@go test -v -cover ./...
.PHONY: test