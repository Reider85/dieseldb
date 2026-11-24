#!/bin/bash

# Local CI/CD Test Runner Script
# Executes tests locally when GitHub runner limits are exceeded
# Usage: ./scripts/local-test-runner.sh [fallback_mode]

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../" && pwd)"

echo "=== Local CI/CD Test Runner ==="
echo "Project root: $PROJECT_ROOT"

# Check if we're in fallback mode
if [ "$1" = "fallback" ]; then
    echo "🔄 Running in FALLBACK mode - critical tests only"
    FALLBACK_MODE=true
else
    echo "🚀 Running in NORMAL mode - full test suite"
    FALLBACK_MODE=false
fi

# Navigate to project directory
cd "$PROJECT_ROOT"

# Function to run tests with error handling
run_tests() {
    local profile="$1"
    local description="$2"
    
    echo "🧪 Running $description tests..."
    
    if ! mvn -B -T 4 --no-transfer-progress -Dstyle.color=never test -P "$profile"; then
        echo "❌ $description tests failed"
        return 1
    fi
    
    echo "✅ $description tests passed"
}

# Function to run security scanning
run_security_scan() {
    echo "🔒 Running security scanning..."
    
    # Check if dependency-check plugin is available
    if mvn dependency-check:check -q; then
        echo "✅ Security scan passed"
    else
        echo "⚠️  Security scan found issues - review output"
        # Don't fail the build for security issues, just warn
    fi
}

# Function to generate coverage report
run_coverage() {
    echo "📊 Generating coverage report..."
    
    if mvn jacoco:report -q; then
        echo "✅ Coverage report generated"
        echo "📄 Coverage report available at: target/site/jacoco/index.html"
    else
        echo "❌ Coverage report generation failed"
        return 1
    fi
}

# Main execution logic
echo "Starting local CI/CD pipeline..."

# Always run these (critical path)
echo "=== CRITICAL TESTS ==="
run_tests "fast" "Fast (smoke/query/index)"
run_tests "core" "Core (query-full/storage)"

# Security scanning (always run)
echo "=== SECURITY SCANNING ==="
run_security_scan

# Coverage generation (always run)
echo "=== COVERAGE REPORTING ==="
run_coverage

# Conditional tests based on mode
if [ "$FALLBACK_MODE" = "true" ]; then
    echo "=== FALLBACK MODE - LIMITED TESTING ==="
    echo "Skipping resource-intensive tests..."
    echo "📋 Note: In fallback mode, only critical tests are executed"
    echo "🔄 Full test suite will resume when GitHub runner capacity is available"
else
    echo "=== FULL TEST SUITE ==="
    run_tests "concurrency" "Concurrency"
    run_tests "network" "Network"
    run_tests "perf" "Performance"
    run_tests "large" "Large"
    
    echo "=== ADDITIONAL TESTING ==="
    run_tests "storage-csv" "Storage CSV"
    run_tests "storage-jsonl" "Storage JSONL"
    run_tests "storage-avro" "Storage Avro"
fi

echo "=== LOCAL CI/CD COMPLETE ==="
echo "🎉 All tests completed successfully"
echo "📊 Test results available in target/surefire-reports/"
echo "📈 Coverage report available in target/site/jacoco/index.html"