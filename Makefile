.PHONY: test test-coverage test-unit test-integration test-e2e test-bench test-all lint security-check build build-all clean
.PHONY: test-performance test-race test-short test-verbose test-json ci-test

# Default test command
test: test-unit

# Testing commands
test-all:
	@echo "Running all tests..."
	@./scripts/test.sh all

test-unit:
	@echo "Running unit tests..."
	go test -v -short -coverprofile=unit-coverage.out ./cmd/... ./internal/... ./pkg/...

test-integration:
	@echo "Running integration tests..."
	go test -v -tags=integration -coverprofile=integration-coverage.out ./test/integration/...

test-e2e:
	@echo "Running end-to-end tests..."
	go test -v -tags=e2e -timeout=20m ./test/e2e/...

test-performance:
	@echo "Running performance tests..."
	go test -v -tags=performance -timeout=30m ./test/performance/...

test-bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem -run=^$$ ./... | tee benchmark-results.txt
	@echo "Benchmark results saved to benchmark-results.txt"

test-race:
	@echo "Running tests with race detector..."
	go test -race -timeout=10m ./...

test-short:
	@echo "Running short tests only..."
	go test -short ./...

test-verbose:
	@echo "Running tests with verbose output..."
	go test -v ./... | tee test-output.txt

test-json:
	@echo "Running tests with JSON output..."
	go test -json ./... > test-results.json

test-coverage:
	@echo "Generating comprehensive test coverage..."
	go test -v -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"
	@go tool cover -func=coverage.out | grep total | awk '{print "Total coverage: " $$3}'
	@echo "Checking coverage threshold..."
	@coverage=$$(go tool cover -func=coverage.out | grep total | awk '{print $$3}' | sed 's/%//'); \
	if [ $$(echo "$$coverage < 80" | bc -l) -eq 1 ]; then \
		echo "ERROR: Coverage $$coverage% is below 80% threshold"; \
		exit 1; \
	else \
		echo "SUCCESS: Coverage $$coverage% meets threshold"; \
	fi

# Test specific package
test-pkg:
	@if [ -z "$(PKG)" ]; then \
		echo "Usage: make test-pkg PKG=./internal/ui"; \
		exit 1; \
	fi
	go test -v -cover $(PKG)/...

# CI testing (used by GitHub Actions)
ci-test:
	@echo "Running CI test suite..."
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go test -tags=integration -coverprofile=integration.out -covermode=atomic ./test/integration/...
	go test -tags=e2e ./test/e2e/...
	@echo "Merging coverage reports..."
	@echo "mode: atomic" > combined-coverage.out
	@tail -n +2 coverage.out >> combined-coverage.out
	@tail -n +2 integration.out >> combined-coverage.out 2>/dev/null || true
	@go tool cover -func=combined-coverage.out

# Code quality
lint:
	@echo "Installing golangci-lint if not present..."
	@which golangci-lint || go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	golangci-lint run

security-check:
	@echo "Installing gosec if not present..."
	@which gosec || go install github.com/securego/gosec/v2/cmd/gosec@latest
	gosec -fmt=json -out=security-report.json ./... || true
	@echo "Security report saved to security-report.json"

# Build commands
build:
	go build -o flakedrop .

build-all:
	@echo "Building for all platforms..."
	GOOS=darwin GOARCH=amd64 go build -o dist/flakedrop-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 go build -o dist/flakedrop-darwin-arm64 .
	GOOS=linux GOARCH=amd64 go build -o dist/flakedrop-linux-amd64 .
	GOOS=windows GOARCH=amd64 go build -o dist/flakedrop-windows-amd64.exe .
	@echo "Binaries created in dist/"

# Clean up
clean:
	rm -f flakedrop
	rm -rf dist/
	rm -f coverage.out coverage.html
	rm -f security-report.json

# Development helpers
run-setup:
	go run main.go setup

run-help:
	go run main.go --help

# Install all development dependencies
dev-deps:
	@echo "Installing development dependencies..."
	go get -u github.com/stretchr/testify
	go get -u github.com/golang/mock
	go get -u github.com/DATA-DOG/go-sqlmock
	go get -u github.com/snowflakedb/gosnowflake
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/securego/gosec/v2/cmd/gosec@latest
	go install github.com/golang/mock/mockgen@latest
	go install github.com/vektra/mockery/v2@latest
	go install github.com/jstemmer/go-junit-report/v2@latest
	go install github.com/zimmski/go-mutesting/cmd/go-mutesting@latest
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install github.com/gordonklaus/ineffassign@latest
	go install honnef.co/go/tools/cmd/unused@latest
	@echo "Development dependencies installed"

# Generate mocks
mocks:
	@echo "Generating mocks..."
	mockery --all --output=internal/mocks --case underscore
	@echo "Mocks generated in internal/mocks/"

# Mutation testing
mutation-test:
	@echo "Running mutation tests..."
	go-mutesting ./internal/... --verbose || true

# Fuzz testing
fuzz:
	@echo "Running fuzz tests..."
	go test -fuzz=FuzzSplitStatements -fuzztime=30s ./internal/snowflake || true
	go test -fuzz=FuzzConfigParse -fuzztime=30s ./internal/config || true

# Static analysis
static-analysis:
	@echo "Running static analysis..."
	staticcheck ./...
	ineffassign ./...
	unused ./...

# Check for vulnerabilities
vuln-check:
	@echo "Checking for vulnerabilities..."
	go install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck ./...

# Generate test report
test-report:
	@echo "Generating test report..."
	go test -v ./... 2>&1 | go-junit-report -set-exit-code > test-report.xml
	@echo "Test report saved to test-report.xml"

# Pre-commit checks
pre-commit: lint test-unit security-check
	@echo "Pre-commit checks passed"

# Docker testing
docker-test:
	@echo "Building Docker test environment..."
	docker build -f Dockerfile.test -t flakedrop-test .
	docker run --rm flakedrop-test

# Help
help:
	@echo "Available targets:"
	@echo "  make test                - Run unit tests"
	@echo "  make test-all           - Run all tests"
	@echo "  make test-unit          - Run unit tests"
	@echo "  make test-integration   - Run integration tests"
	@echo "  make test-e2e           - Run end-to-end tests"
	@echo "  make test-performance   - Run performance tests"
	@echo "  make test-coverage      - Generate test coverage report"
	@echo "  make test-race          - Run tests with race detector"
	@echo "  make lint               - Run linters"
	@echo "  make security-check     - Run security checks"
	@echo "  make build              - Build the binary"
	@echo "  make build-all          - Build for all platforms"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make dev-deps           - Install development dependencies"
	@echo "  make pre-commit         - Run pre-commit checks"
	@echo "  make help               - Show this help message"