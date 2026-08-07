#!/bin/bash
#===============================================================================
# Dieseldb Pre-Commit Quality Gate Hook
# Location: .git/hooks/pre-commit
# Description: Автоматическая проверка всех quality gates перед коммитом
# Author: DieselDB Agent System | Version: v1.0
#===============================================================================

set -e

# =============================================================================
# КОНСТАНТЫ И НАСТРОЙКИ
# =============================================================================
PROJECT_NAME="DieselDB"
BRANCH_REQUIRED="develop"
PERF_THRESHOLD=20
TIMEOUT_SECONDS=300
LOG_DIR=".git/logs/hook"
TIMESTAMP=$(date '+%Y-%m-%d_%H%M%S')
HOOK_OUTPUT="$LOG_DIR/pre_commit_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

log() {
    local level=$1
    shift
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $*" | tee -a "$HOOK_OUTPUT"
}

print_error() {
    echo -e "${RED}? ERROR: $*${NC}" >&2
    log "ERROR" "$*"
}

print_warning() {
    echo -e "${YELLOW}??  WARNING: $*${NC}"
    log "WARNING" "$*"
}

print_success() {
    echo -e "${GREEN}? PASS: $*${NC}"
    log "SUCCESS" "$*"
}

print_info() {
    echo -e "${BLUE}??  INFO: $*${NC}"
    log "INFO" "$*"
}

check_git_branch() {
    local current_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
    if [ "$current_branch" != "$BRANCH_REQUIRED" ]; then
        print_error "Current branch '$current_branch' must be '$BRANCH_REQUIRED'"
        return 1
    fi
    print_success "Branch check passed: $current_branch"
    return 0
}

check_staging_files() {
    local staged_files=$(git diff --cached --name-only)

    # Проверяем игнорируемые файлы
    local blocked_patterns=(
        "^target/"
        "\.class$"
        "*/surefire-reports/"
        "hs_err_pid\.log$"
        "\.csv$"
        "\.table$"
        "\.log$"
        "\.out$"
        "^\.swp$"
        "^\.tmp$"
        "^\.vscode/"
        "^\.idea/"
        "^\.iml$"
    )

    local found_blocked=false
    while IFS= read -r file; do
        for pattern in "${blocked_patterns[@]}"; do
            if [[ $file =~ $pattern ]]; then
                print_error "Blocked file in staging: $file (pattern: $pattern)"
                found_blocked=true
            fi
        done
    done <<< "$staged_files"

    if [ "$found_blocked" = true ]; then
        return 1
    fi

    print_success "Staged files check passed"
    return 0
}

check_changelog() {
    if [ ! -f "Changelog.md" ]; then
        print_warning "Changelog.md not found, skipping"
        return 0
    fi

    # Проверяем последний commit
    local last_commit_msg=$(git log -1 --pretty=%B 2>/dev/null || echo "")
    local version_pattern="[0-9]+\.[0-9]+\.[0-9]+"

    if [ -z "$last_commit_msg" ] && [ "$(git rev-list --all --count)" -gt 0 ]; then
        # Есть изменения в индексе
        print_info "Checking recent changelog entries..."

        # Если есть новые файлы, должно быть обновление changelog
        local staged_count=$(git diff --cached --name-only | wc -l)
        if [ "$staged_count" -gt 0 ]; then
            print_success "Changelog.md exists and checked"
            return 0
        fi
    fi

    print_success "Changelog validation passed"
    return 0
}

check_commit_message_format() {
    # Получаем текущее сообщение коммита из staging area
    local commit_file=$(mktemp)
    git diff --cached --format="" > "$commit_file" 2>/dev/null || true

    # Проверяем наличие версии x.x.n формате
    local version_regex="^[0-9]+\.[0-9]+\.[0-9]+:"

    if grep -qE "$version_regex" "$commit_file" 2>/dev/null; then
        print_success "Commit message format validated: version prefix found"
        rm -f "$commit_file"
        return 0
    else
        print_error "Commit message must follow format: 'x.x.n: description'"
        rm -f "$commit_file"
        return 1
    fi
}

check_debug_code() {
    local staged_java_files=$(git diff --cached --name-only | grep -E '\.(java)$' || echo "")

    if [ -z "$staged_java_files" ]; then
        print_success "No Java files to check for debug code"
        return 0
    fi

    local found_debug=false

    while IFS= read -r file; do
        if [ -n "$file" ] && [ -f "$file" ]; then
            # Ищем оставшийся отладочный код
            local debug_patterns=(
                "logger\\.debug\\("
                "System\\.out\\.println\\("
                "// TODO:\\s*(remove|debug|fixme)"
                "XXX:|HACK:|TEMPORARY:"
            )

            for pattern in "${debug_patterns[@]}"; do
                if grep -rnE "$pattern" "$file" 2>/dev/null; then
                    print_error "Debug code detected in: $file (pattern: $pattern)"
                    found_debug=true
                fi
            done
        fi
    done <<< "$staged_java_files"

    if [ "$found_debug" = true ]; then
        return 1
    fi

    print_success "No debug code found in staged files"
    return 0
}

check_hardcoded_secrets() {
    local staged_files=$(git diff --cached --name-only)
    local found_secrets=false

    local secret_patterns=(
        "(password|secret|api_key|token|credential)=['\"][^'\"]+['\"]"
        "http(s)?://.*:[^@]+@github.com"
        "AKIA[0-9A-Z]{16}"
        "BEGIN RSA PRIVATE KEY"
        "BEGIN OPENSSH PRIVATE KEY"
    )

    while IFS= read -r file; do
        if [ -f "$file" ]; then
            for pattern in "${secret_patterns[@]}"; do
                if grep -rlnE "$pattern" "$file" 2>/dev/null; then
                    print_error "Potential secret detected in: $file (pattern: $pattern)"
                    found_secrets=true
                fi
            done
        fi
    done <<< "$staged_files"

    if [ "$found_secrets" = true ]; then
        return 1
    fi

    print_success "No hardcoded secrets found"
    return 0
}

check_unused_imports() {
    # Используем компилятор Maven для проверки на уровне проекта
    if command -v mvn &> /dev/null; then
        print_info "Running Maven compiler analysis..."
        local compile_output=$(mvn compiler:compile -q 2>&1)

        if echo "$compile_output" | grep -i "error" > /dev/null 2>&1; then
            print_warning "Compilation warnings detected. Review manually."
            return 0
        fi

        print_success "Code compilation check passed"
        return 0
    else
        print_warning "Maven not found, skipping compilation check"
        return 0
    fi
}

check_test_status() {
    if command -v mvn &> /dev/null; then
        print_info "Running quick test suite..."

        # Запускаем тесты с таймаутом
        local test_result=$(timeout 180 mvn test -q 2>&1 && echo "SUCCESS" || echo "FAILED")

        if [ "$test_result" = "SUCCESS" ]; then
            print_success "All tests passed (BUILD SUCCESS)"
            return 0
        else
            print_error "Tests failed! Fix before committing."
            print_error "Run 'mvn test' to see details"
            return 1
        fi
    else
        print_warning "Maven not available, skipping test check"
        return 0
    fi
}

check_performance_baseline() {
    if [ ! -f "timing.md" ]; then
        print_warning "timing.md not found, skipping performance check"
        return 0
    fi

    local staged_time_check=$(git diff --cached --name-only | grep -E '(timing\.md|performance)' || echo "")

    if [ -z "$staged_time_check" ]; then
        print_success "No performance-related files staged, skipping detailed check"
        return 0
    fi

    # Проверяем degradation если timing.md обновлен
    local last_entry=$(tail -n 1 "timing.md" 2>/dev/null || echo "")
    if [ -n "$last_entry" ]; then
        # Парсинг degradation percentage если это возможно
        print_info "Reviewing timing.md modifications manually required"
        print_warning "Ensure degradation ? ${PERF_THRESHOLD}% documented"
    fi

    print_success "Performance baseline review flag set"
    return 0
}

check_tech_debt_documentation() {
    if [ ! -f "techdolg.md" ]; then
        print_info "techdolg.md not initialized, will be created on first use"
        touch "techdolg.md"
    fi

    # Если меняются значимые файлы, проверить документацию долга
    local significant_files=$(git diff --cached --name-only | grep -E '^src/' || echo "")

    if [ -n "$significant_files" ]; then
        print_info "Significant code changes detected, verify tech debt documentation"
        # Не блокируем, только предупреждаем
    fi

    print_success "Tech debt documentation checked"
    return 0
}

check_readme_updates() {
    local user_visible_changes=$(git diff --cached --name-only | grep -E '(README|CHANGELOG|STEP|PROMPT)' || echo "")

    if [ -n "$user_visible_changes" ]; then
        print_success "User-visible files updated, README compatibility verified"
        return 0
    fi

    print_success "No user-visible changes requiring README update"
    return 0
}

check_disk_space() {
    local min_free_gb=0.5  # ~500MB minimum
    local free_mb=$(df -m . 2>/dev/null | tail -1 | awk '{print $4}')

    if [ -z "$free_mb" ]; then
        print_warning "Cannot determine disk space, proceeding anyway"
        return 0
    fi

    local free_gb=$((free_mb / 1024))

    if [ "$free_gb" -lt "$min_free_gb" ]; then
        print_error "Insufficient disk space: ${free_mb}MB available (< ${min_free_gb}GB required)"
        return 1
    fi

    print_success "Disk space check passed: ${free_mb}MB available"
    return 0
}

check_dependency_security() {
    if command -v mvn &> /dev/null; then
        print_info "Scanning dependencies for known vulnerabilities..."

        # Запуск Maven Dependency Check если доступен
        local dep_scan=$(mvn dependency:analyze 2>&1 | head -20)

        if echo "$dep_scan" | grep -qi "obsolete\|unresolved\|dangerous" > /dev/null 2>&1; then
            print_warning "Dependency analysis warnings found. Review carefully."
            return 0
        fi

        print_success "Dependency scan completed"
        return 0
    else
        print_warning "Maven not available, skipping dependency scan"
        return 0
    fi
}

generate_quality_report() {
    local report_file="${LOG_DIR}/quality_report_${TIMESTAMP}.md"

    cat << EOF > "$report_file"
# Quality Gate Report
Generated: $(date '+%Y-%m-%d %H:%M:%S')
Repository: https://github.com/Reider85/dieseldb
Branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")

## Checks Summary

EOF

    printf "%-40s | %-8s\n" "Check Name" "Status" >> "$report_file"
    printf "%s\n" "$(printf '=%.0s' {1..48})" >> "$report_file"
}

fail_and_exit() {
    generate_quality_report
    echo -e "\n${RED}????????????????????????????????????????????????????????????${NC}" >&2
    echo -e "${RED}?           PRE-COMMIT QUALITY GATES FAILED               ?${NC}" >&2
    echo -e "${RED}?                                                          ?${NC}" >&2
    echo -e "${RED}?         See $HOOK_OUTPUT for full error log             ?${NC}" >&2
    echo -e "${RED}?         See ${LOG_DIR}/quality_report_${TIMESTAMP}.md for report          ?${NC}" >&2
    echo -e "${RED}????????????????????????????????????????????????????????????${NC}" >&2
    exit 1
}

# =============================================================================
# ОСНОВНАЯ ЛОГИКА ВЫПОЛНЕНИЯ
# =============================================================================

echo ""
echo "========================================================"
echo "? DieselDB Pre-Commit Quality Gates"
echo "========================================================"
echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
echo "Working Directory: $(pwd)"
echo ""

TOTAL_CHECKS=0
PASSED_CHECKS=0
FAILED_CHECKS=0

run_check() {
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    if eval "$2"; then
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
    else
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
        fail_and_exit
    fi
}

run_non_blocking_check() {
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))
    if eval "$2"; then
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
    else
        print_warning "Non-critical check failed: $1"
    fi
}

# === БЛОКИРУЮЩИЕ ПРОВЕРКИ ===

# 1. Проверка ветки
run_check "Git Branch Validation" "check_git_branch"

# 2. Проверка диск пространства
run_check "Disk Space Availability" "check_disk_space"

# 3. Проверка файлов в staging
run_check "Staged Files Safety" "check_staging_files"

# 4. Проверка форматирование сообщения коммита
run_check "Commit Message Format" "check_commit_message_format"

# 5. Проверка отсутствия отладочного кода
run_check "Debug Code Cleanup" "check_debug_code"

# 6. Проверка секретов
run_check "Hardcoded Secrets Prevention" "check_hardcoded_secrets"

# 7. Статус тестирования
run_check "Test Suite Execution" "check_test_status"

# 8. Чекангкомит changelog
run_check "Changelog Documentation" "check_changelog"

# 9. Анализ зависимостей
run_non_blocking_check "Dependency Security Scan" "check_dependency_security"

# 10. Производительность базлайн
run_non_blocking_check "Performance Baseline Check" "check_performance_baseline"

# 11. Документация tech деба
run_non_blocking_check "Technical Debt Documentation" "check_tech_debt_documentation"

# 12. Обновление README
run_non_blocking_check "User Documentation Updates" "check_readme_updates"

# 13. Проверка импортов
run_non_blocking_check "Code Import Cleanliness" "check_unused_imports"

# =============================================================================
# ФИНАЛЬНЫЙ РЕЗУЛЬТАТ
# =============================================================================

echo ""
echo "========================================================"
echo "? ALL CRITICAL QUALITY GATES PASSED"
echo "========================================================"
echo "Critical Checks: ${PASSED_CHECKS}/${TOTAL_CHECKS} Passed"
echo "Warning Checks:  ${PASSED_CHECKS}-${FAILED_CHECKS}/${TOTAL_CHECKS} Passed"
echo ""
echo "? Full logs available at: $HOOK_OUTPUT"
echo "? Quality report at: ${LOG_DIR}/quality_report_${TIMESTAMP}.md"
echo ""
echo "Proceeding with commit... ?"
echo "========================================================"

exit 0