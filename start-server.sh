#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PORT="${1:-3306}"
DATA_DIR="${2:-data}"

build_classpath() {
    local cp="$SCRIPT_DIR/target/classes:$SCRIPT_DIR"
    local cpf="$SCRIPT_DIR/target/classpath.txt"
    if [ -f "$cpf" ]; then
        cp="$cp:$(cat "$cpf")"
    else
        if command -v mvn >/dev/null 2>&1; then
            (cd "$SCRIPT_DIR" && mvn -q dependency:build-classpath -Dmdep.outputFile="$cpf" -Dmdep.includeScope=runtime) >/dev/null 2>&1 || true
        elif command -v powershell >/dev/null 2>&1 && [ -f "$SCRIPT_DIR/scripts/mvn.ps1" ]; then
            powershell -NoProfile -ExecutionPolicy Bypass -File "$SCRIPT_DIR/scripts/mvn.ps1" -B dependency:build-classpath "-Dmdep.outputFile=target\\classpath.txt" "-Dmdep.includeScope=runtime" >/dev/null 2>&1 || true
        fi
        if [ -f "$cpf" ]; then
            cp="$cp:$(cat "$cpf")"
        else
            local m2="${HOME}/.m2/repository"
            for coord in \
                "org/slf4j/slf4j-api/2.0.12/slf4j-api-2.0.12.jar" \
                "ch/qos/logback/logback-classic/1.5.6/logback-classic-1.5.6.jar" \
                "ch/qos/logback/logback-core/1.5.6/logback-core-1.5.6.jar" \
                "org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar" \
                "com/github/luben/zstd-jni/1.5.6-6/zstd-jni-1.5.6-6.jar" \
                "org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar" \
                "com/fasterxml/jackson/core/jackson-core/2.17.2/jackson-core-2.17.2.jar" \
                "com/google/code/gson/gson/2.8.9/gson-2.8.9.jar" \
                "org/apache/avro/avro/1.12.0/avro-1.12.0.jar" \
                "com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar"; do
                if [ -f "$m2/$coord" ]; then
                    cp="$cp:$m2/$coord"
                fi
            done
        fi
    fi
    echo "$cp"
}

CLASSPATH="$(build_classpath)"

mkdir -p "$DATA_DIR"

echo "Starting DieselDB server on port ${PORT} with data dir ${DATA_DIR}"
exec java -cp "$CLASSPATH" diesel.DatabaseServer "$PORT" "$DATA_DIR"
