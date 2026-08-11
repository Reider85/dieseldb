#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

HOST="${1:-localhost}"
PORT="${2:-3306}"

build_classpath() {
    local cp="$SCRIPT_DIR/target/classes:$SCRIPT_DIR"
    local cpf="$SCRIPT_DIR/target/classpath.txt"
    if [ -f "$cpf" ]; then
        cp="$cp:$(cat "$cpf")"
    else
        if command -v mvn >/dev/null 2>&1; then
            (cd "$SCRIPT_DIR" && mvn -q dependency:build-classpath -Dmdep.outputFile="$cpf" -Dmdep.includeScope=runtime) >/dev/null 2>&1 || true
        fi
        if [ -f "$cpf" ]; then
            cp="$cp:$(cat "$cpf")"
        else
            local m2="${HOME}/.m2/repository"
            for coord in \
                "org/slf4j/slf4j-api/2.0.12/slf4j-api-2.0.12.jar" \
                "ch/qos/logback/logback-classic/1.5.6/logback-classic-1.5.6.jar" \
                "ch/qos/logback/logback-core/1.5.6/logback-core-1.5.6.jar"; do
                if [ -f "$m2/$coord" ]; then
                    cp="$cp:$m2/$coord"
                fi
            done
        fi
    fi
    echo "$cp"
}

CLASSPATH="$(build_classpath)"

echo "Connecting to DieselDB server at ${HOST}:${PORT}"
exec java -cp "$CLASSPATH" diesel.DatabaseClient "$HOST" "$PORT"
