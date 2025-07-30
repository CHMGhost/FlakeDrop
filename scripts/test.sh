#!/bin/bash

# Test runner script for FlakeDrop CLI
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Test results tracking
PASSED=0
FAILED=0
SKIPPED=0

# Helper functions
print_header() {
    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN}$1${NC}"
    echo -e "${GREEN}========================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
    ((PASSED++))
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
    ((FAILED++))
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
    ((SKIPPED++))
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    if ! command -v go &> /dev/null; then
        print_error "Go is not installed"
        exit 1
    else
        print_success "Go is installed: $(go version)"
    fi
    
    if ! command -v git &> /dev/null; then
        print_error "Git is not installed"
        exit 1
    else
        print_success "Git is installed: $(git version)"
    fi
}

# Run unit tests
run_unit_tests() {
    print_header "Running Unit Tests"
    
    packages=(
        "./internal/config"
        "./internal/git"
        "./internal/ui"
        "./pkg/models"
        "./cmd"
    )
    
    for pkg in "${packages[@]}"; do
        echo -e "\nTesting $pkg..."
        if go test -v -cover "$pkg/..."; then
            print_success "Tests passed for $pkg"
        else
            print_error "Tests failed for $pkg"
        fi
    done
}

# Run integration tests
run_integration_tests() {
    print_header "Running Integration Tests"
    
    if [ "$SKIP_INTEGRATION" = "true" ]; then
        print_warning "Integration tests skipped (SKIP_INTEGRATION=true)"
        return
    fi
    
    if go test -v ./test/integration/...; then
        print_success "Integration tests passed"
    else
        print_error "Integration tests failed"
    fi
}

# Run E2E tests
run_e2e_tests() {
    print_header "Running End-to-End Tests"
    
    if [ "$SKIP_E2E" = "true" ]; then
        print_warning "E2E tests skipped (SKIP_E2E=true)"
        return
    fi
    
    # Build the binary first
    echo "Building CLI binary..."
    if go build -o flakedrop main.go; then
        print_success "CLI built successfully"
    else
        print_error "Failed to build CLI"
        return
    fi
    
    if go test -v ./test/e2e/...; then
        print_success "E2E tests passed"
    else
        print_error "E2E tests failed"
    fi
}

# Run benchmarks
run_benchmarks() {
    print_header "Running Benchmarks"
    
    if [ "$SKIP_BENCH" = "true" ]; then
        print_warning "Benchmarks skipped (SKIP_BENCH=true)"
        return
    fi
    
    echo "Running performance benchmarks..."
    if go test -bench=. -benchmem -run=^$ ./... > benchmark_results.txt; then
        print_success "Benchmarks completed"
        echo -e "\nTop 10 benchmark results:"
        grep -E "^Benchmark" benchmark_results.txt | head -10
    else
        print_error "Benchmarks failed"
    fi
}

# Run lint checks
run_lint() {
    print_header "Running Lint Checks"
    
    if ! command -v golangci-lint &> /dev/null; then
        print_warning "golangci-lint not installed, skipping lint checks"
        echo "Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"
        return
    fi
    
    if golangci-lint run --timeout=5m; then
        print_success "Lint checks passed"
    else
        print_error "Lint checks failed"
    fi
}

# Generate coverage report
generate_coverage() {
    print_header "Generating Coverage Report"
    
    echo "Generating test coverage..."
    if go test -coverprofile=coverage.out ./... > /dev/null 2>&1; then
        go tool cover -html=coverage.out -o coverage.html
        coverage=$(go tool cover -func=coverage.out | grep total | awk '{print $3}')
        print_success "Coverage report generated: $coverage"
        echo "View coverage report: open coverage.html"
    else
        print_error "Failed to generate coverage report"
    fi
}

# Run security scan
run_security_scan() {
    print_header "Running Security Scan"
    
    if ! command -v gosec &> /dev/null; then
        print_warning "gosec not installed, skipping security scan"
        echo "Install with: go install github.com/securego/gosec/v2/cmd/gosec@latest"
        return
    fi
    
    if gosec -quiet ./...; then
        print_success "Security scan passed"
    else
        print_error "Security scan found issues"
    fi
}

# Test specific package
test_package() {
    local package=$1
    print_header "Testing Package: $package"
    
    if go test -v -cover "$package"; then
        print_success "Tests passed for $package"
    else
        print_error "Tests failed for $package"
    fi
}

# Main test execution
main() {
    local start_time=$(date +%s)
    
    # Parse command line arguments
    case "${1:-all}" in
        unit)
            check_prerequisites
            run_unit_tests
            ;;
        integration)
            check_prerequisites
            run_integration_tests
            ;;
        e2e)
            check_prerequisites
            run_e2e_tests
            ;;
        bench)
            check_prerequisites
            run_benchmarks
            ;;
        lint)
            check_prerequisites
            run_lint
            ;;
        security)
            check_prerequisites
            run_security_scan
            ;;
        coverage)
            check_prerequisites
            generate_coverage
            ;;
        package)
            check_prerequisites
            test_package "$2"
            ;;
        all)
            check_prerequisites
            run_unit_tests
            run_integration_tests
            run_e2e_tests
            run_benchmarks
            run_lint
            generate_coverage
            run_security_scan
            ;;
        *)
            echo "Usage: $0 [unit|integration|e2e|bench|lint|security|coverage|package <path>|all]"
            exit 1
            ;;
    esac
    
    # Calculate execution time
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    # Print summary
    print_header "Test Summary"
    echo -e "Passed:  ${GREEN}$PASSED${NC}"
    echo -e "Failed:  ${RED}$FAILED${NC}"
    echo -e "Skipped: ${YELLOW}$SKIPPED${NC}"
    echo -e "\nTotal execution time: ${duration}s"
    
    # Exit with appropriate code
    if [ $FAILED -gt 0 ]; then
        exit 1
    else
        exit 0
    fi
}

# Run main function
main "$@"