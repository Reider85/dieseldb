# Промпт 11 — S1192: Extract Duplicated Literals in AVRO Module

## Goal
Create `AvroMetricConstants.java` and replace ~80 duplicated string literals across ~20 AVRO files to fix SonarQube S1192 rule.

## Step 1: Create `AvroMetricConstants.java`

File: `diesel/storage/avro/AvroMetricConstants.java`

```java
package diesel.storage.avro;

final class AvroMetricConstants {
    private AvroMetricConstants() { }

    // Prometheus metric types
    static final String METRIC_TYPE_COUNTER = "counter";
    static final String METRIC_TYPE_GAUGE = "gauge";

    // Log format strings
    static final String LOG_FORMAT_AVRO_METRIC = "[AVRO-METRIC] {}";

    // Config-loading log messages
    static final String MSG_INVALID_VALUE_QUOTED = "Invalid {} value '{}', using default {}";
    static final String MSG_INVALID_VALUE_UNQUOTED = "Invalid {} value {}, using default {}";
    static final String MSG_INVALID_VALUE_EQUALS = "Invalid {} = \"{}\", using default {}";
    static final String MSG_CONFIG_READ_FAILED = "Could not read config.properties, using defaults: {}";
}
```

## Step 2: Update AvroMetrics.java (14 replacements)

- Line 319: `"[AVRO-METRIC] {}"` → `AvroMetricConstants.LOG_FORMAT_AVRO_METRIC`
- Lines 339, 340, 341: same
- Lines 362, 364, 366, 368, 381, 383: `"counter"` → `AvroMetricConstants.METRIC_TYPE_COUNTER`
- Lines 374, 376, 379, 385: `"gauge"` → `AvroMetricConstants.METRIC_TYPE_GAUGE`
- Lines 544, 554, 564: `"Invalid {} = \"{}\", using default {}"` → `AvroMetricConstants.MSG_INVALID_VALUE_EQUALS`

## Step 3: Update config classes — MSG_INVALID_VALUE_QUOTED (25 files)

Replace `"Invalid {} value '{}', using default {}"` with `AvroMetricConstants.MSG_INVALID_VALUE_QUOTED`:

| File | Lines |
|------|-------|
| AdaptiveCompressionManager.java | 478, 493, 508, 523 |
| AvroBlockConfig.java | 149 |
| AvroBloomFilterConfig.java | 154, 164, 178 |
| AvroBufferConfig.java | 178, 196, 206 |
| AvroCompressionConfig.java | 197, 208 |
| AvroDataValidator.java | 647, 677 |
| AvroObjectPool.java | 200, 218 |
| AvroPrimaryKeyIndex.java | 816 |
| AvroQueryConfig.java | 99, 113 |
| AvroStatistics.java | 307 |
| AvroTransactionManager.java | 869 |
| BZip2Codec.java | 117 |
| SchemaCompatibilityChecker.java | 360 |
| SchemaConflictResolver.java | 643 |

## Step 4: Update config classes — MSG_INVALID_VALUE_UNQUOTED (13 occurrences)

Replace `"Invalid {} value {}, using default {}"` with `AvroMetricConstants.MSG_INVALID_VALUE_UNQUOTED`:

| File | Lines |
|------|-------|
| AvroBlockConfig.java | 111, 117 |
| AvroBloomFilterConfig.java | 134 |
| AvroBufferConfig.java | 132, 137, 143 |
| AvroObjectPool.java | 172, 177, 182 |
| AvroQueryConfig.java | 70 |
| AvroTransactionManager.java | 322, 326, 330 |

## Step 5: Update config classes — MSG_CONFIG_READ_FAILED (14 files)

Replace `"Could not read config.properties, using defaults: {}"` with `AvroMetricConstants.MSG_CONFIG_READ_FAILED`:

Files: AvroBlockConfig, AvroBloomFilterConfig, AvroBufferConfig, AvroCompressionConfig, AvroCrashDetector, AvroDataValidator, AvroIntegrityChecker, AvroObjectPool, AvroPrimaryKeyIndex, AvroSyncMarkerManager, AvroTransactionManager, SchemaCompatibilityChecker, SchemaConflictResolver, SchemaEvolutionManager (all with `.java` extension).

## Step 6: Update AvroAuditLogger.java — MSG_INVALID_VALUE_EQUALS (3 occurrences)

Lines 559, 569, 579: `"Invalid {} = \"{}\", using default {}"` → `AvroMetricConstants.MSG_INVALID_VALUE_EQUALS`

## Step 7: Run tests

```bash
make test
```

## Step 8: Create changelog entry

```bash
make changelog DESC="Extract duplicated string literals in AVRO module to AvroMetricConstants (S1192)"
```

## Total impact
- ~80 S1192 issues closed
- 1 new file created
- ~20 files modified
