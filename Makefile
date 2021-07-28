GOLANGCI_VERSION = 1.34.0

ci: deps lint test ##=> Run all CI targets
.PHONY: ci

lint: bin/golangci-lint ##=> Lint all the things
	@echo "--- lint all the things"
	@bin/golangci-lint run
.PHONY: lint

clean: ##=> Clean all the things
	$(info [+] Clean all the things...")
.PHONY: clean

bin/golangci-lint: bin/golangci-lint-${GOLANGCI_VERSION}
	@ln -sf golangci-lint-${GOLANGCI_VERSION} bin/golangci-lint
bin/golangci-lint-${GOLANGCI_VERSION}:
	curl -sfL https://install.goreleaser.com/github.com/golangci/golangci-lint.sh | BINARY=golangci-lint bash -s -- v${GOLANGCI_VERSION}
	@mv bin/golangci-lint $@

test: ##=> Run the tests
	$(info [+] Run tests...")
	@go test -v -cover ./...
.PHONY: test

lintv2: bin/golangci-lint ##=> Lint all the v2 things
	@echo "--- lint all the things"
	@cd v2; ../bin/golangci-lint run
.PHONY: lintv2

testv2: ##=> Run the tests
	$(info [+] Run v2 tests...")
	@cd v2; go test -v -cover ./...
.PHONY: testv2
