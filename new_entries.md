
3.0.80 Prompt 42 - JSON streaming abstraction (unified parser/generator facade, Gson backend, parser limits)
3.0.81 Fix Windows transient file-lock flake in atomic CSV save: rename retry attempts + backoff cap made configurable
3.0.82 Prompt 43 - JSONL type mapping (JsonTypeMapper, strict/lenient coercion, 2^53 precision rules)
3.0.83 Make PersistenceTest storage-type agnostic (fix compressed-delimited DROP test under tsv)
3.0.84 Prompt 44 - JSONL schema modes (strict/inferred/hybrid), one-pass schema inference, sidecar evolution
3.0.85 Fix ClassCastException when csv.table.mirror=on + csv.load.mode=auto_mtime
3.0.86 Fix ServerConnectionLimitTest/SocketTimeoutTest failures + harden server startup against stale/corrupt table files
3.0.87 Prompt 45 - JSONL nested storage modes (flatten/json_column) and array handling (json/expand), SQL JSON Path support
3.0.88 Prompt 46 - JSONL storage architecture integration and shared-infrastructure inheritance (AtomicFileWriter, Object[] rows, deferred bulk rebuild, slf4j, UTF-8, write-lock saves) - verify-only: all items already implemented and closed by JsonlStorageTest alone (3.0.88 commit message from dieseldb git history - prompt 46 verification already done at 3.0.83/3.0.87)
