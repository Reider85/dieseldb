# Changelog

## 3.0.100
Apply CSV/TSV parser+writer intrinsics patch - indexOf+substring fast path + typed StringBuilder.append on writers + loadFast byte reader path

## 3.0.99
Prompt 52 - JSONL compression via common CompressionCodec (zstd/lz4/snappy)

## 3.0.98
Prompt 52 follow-up - CSV/TSV reader type-parser precompilation (per-cell switch dropped)

## 3.0.97
Prompt 52 - CSV/TSV read/write I/O fast path (byte-level whole-file load + batched typed writers)

## 3.0.96
Prompt 51 - JSONL deterministic serialization (byte-reproducible output)

## 3.0.95
Update PROMPT_STATUS.md: add bug fix note for JSONL test fix

## 3.0.94
Fix testDropTableDeletesCompressedDelimitedFiles for JSONL storage type

## 3.0.93
Prompt 50 - JSONL load mode: .table vs .jsonl with auto_mtime fast path and optional mirror

## 3.0.92
Prompt 49 - JSONL append/rewrite write modes, delta sidecar, auto-compaction, crash-recovery

## 3.0.91
Prompt 48 - JSONL load-error diagnostics and garbage tolerance

## 3.0.90
Fix RegexRobustnessTest: table alias detection and unquoteQualifiedIdentifier for quoted identifiers

## 3.0.89
Prompt 47 - JSONL NULL semantics: null vs missing field vs empty string stay distinct

## 3.0.88
Prompt 46 - JSONL storage architecture integration and shared-infrastructure inheritance

## 3.0.87
Prompt 45 - JSONL nested storage modes (flatten/json_column) and array handling (json/expand), SQL JSON Path support

## 3.0.86
Fix ServerConnectionLimitTest/SocketTimeoutTest failures + harden server startup against stale/corrupt table files

## 3.0.85
Fix ClassCastException when csv.table.mirror=on + csv.load.mode=auto_mtime

## 3.0.84
Prompt 44 - JSONL schema modes (strict/inferred/hybrid), one-pass schema inference, sidecar evolution

## 3.0.83
Make PersistenceTest storage-type agnostic (fix compressed-delimited DROP test under tsv)

## 3.0.82
Prompt 43 - JSONL type mapping (JsonTypeMapper, strict/lenient coercion, 2^53 precision rules)

## 3.0.81
Fix Windows transient file-lock flake in atomic CSV save: rename retry attempts + backoff cap made configurable