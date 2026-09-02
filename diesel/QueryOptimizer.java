package diesel;

import java.io.FileInputStream;
import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Adaptive query optimizer (Prompt 82). Monitors the ratio of estimated vs.
 * actual row counts during a SELECT execution and replans the remaining joins
 * when the deviation exceeds a configurable threshold. Also maintains a
 * learned plan cache (LRU) keyed by a normalized query fingerprint so that
 * subsequent executions of structurally similar queries can skip re-optimisation
 * and pick the best-known join strategy directly.
 *
 * <p>Thread-safety: every mutable field lives inside a single
 * {@link QueryExecutionState} instance that is created per query execution and
 * never shared between threads; the {@link PlanCache} itself is
 * {@link ConcurrentHashMap}-backed and safe for concurrent reads/writes.
 *
 * @see SelectQuery
 */
final class QueryOptimizer {

    private static final Logger LOGGER = Logger.getLogger(QueryOptimizer.class.getName());

    /* ──────────────── configuration keys ──────────────── */
    static final String ENABLED_KEY        = "adaptive.enabled";
    static final String THRESHOLD_KEY      = "adaptive.replan.threshold";
    static final String LEARNING_KEY       = "adaptive.learning.enabled";
    static final String CACHE_SIZE_KEY     = "adaptive.cache.size";
    static final String SAMPLING_KEY       = "adaptive.sampling.interval";

    /* ──────────────── defaults ──────────────── */
    private static final boolean DEFAULT_ENABLED     = true;
    private static final double  DEFAULT_THRESHOLD   = 0.5;  // replan when actual/est < 0.5 or > 2.0
    private static final boolean DEFAULT_LEARNING    = true;
    private static final int     DEFAULT_CACHE_SIZE  = 256;
    private static final int     DEFAULT_SAMPLING    = 4096;

    /* ──────────────── loaded config ──────────────── */
    private static boolean enabled       = DEFAULT_ENABLED;
    private static double  threshold     = DEFAULT_THRESHOLD;
    private static boolean learning      = DEFAULT_LEARNING;
    private static int     cacheSize     = DEFAULT_CACHE_SIZE;
    private static int     samplingRows  = DEFAULT_SAMPLING;

    /* ──────────────── singleton (stateless) ──────────────── */
    private static final QueryOptimizer INSTANCE = new QueryOptimizer();

    /* ──────────────── learned plan cache ──────────────── */
    private static final PlanCache planCache = new PlanCache();

    static {
        loadAdaptiveConfig();
    }

    private QueryOptimizer() {
    }

    static QueryOptimizer getInstance() {
        return INSTANCE;
    }

    /* ──────────────────────────────────────────────
     * Config loading (Pattern A – FileInputStream)
     * ────────────────────────────────────────────── */

    static void loadAdaptiveConfig() {
        try {
            File configFile = new File(ErrorMessages.CONFIG_FILE);
            if (configFile.exists()) {
                java.util.Properties props = new java.util.Properties();
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
                String rawEnabled = props.getProperty(ENABLED_KEY);
                if (rawEnabled != null) enabled = Boolean.parseBoolean(rawEnabled.trim());
                String rawThreshold = props.getProperty(THRESHOLD_KEY);
                if (rawThreshold != null) threshold = Double.parseDouble(rawThreshold.trim());
                String rawLearning = props.getProperty(LEARNING_KEY);
                if (rawLearning != null) learning = Boolean.parseBoolean(rawLearning.trim());
                String rawCache = props.getProperty(CACHE_SIZE_KEY);
                if (rawCache != null) cacheSize = Integer.parseInt(rawCache.trim());
                String rawSample = props.getProperty(SAMPLING_KEY);
                if (rawSample != null) samplingRows = Integer.parseInt(rawSample.trim());
            }
        } catch (Exception ignored) {
            LOGGER.fine("Adaptive config error, using defaults: " + ignored.getMessage());
        }
    }

    /* ──────────────────────────────────────────────
     * Test-only setters
     * ────────────────────────────────────────────── */

    static void setEnabledForTest(boolean v) { enabled = v; }
    static void setThresholdForTest(double v) { threshold = v; }
    static void setLearningForTest(boolean v) { learning = v; }
    static void setSamplingForTest(int v)     { samplingRows = v; }
    static void clearCacheForTest()           { planCache.clear(); }

    /* ──────────────────────────────────────────────
     * Per-execution state
     * ────────────────────────────────────────────── */

    /**
     * Creates a fresh execution state for a new query.
     * Called once per {@link SelectQuery#executeSelect}.
     */
    QueryExecutionState beginExecution(List<String> tableNames,
                                       List<QueryParser.JoinInfo> joins) {
        if (!enabled) {
            return QueryExecutionState.DISABLED;
        }
        // Skip fingerprint/cache for single-table queries (no joins = no replanning)
        if (joins == null || joins.isEmpty()) {
            return new QueryExecutionState(null, null, threshold);
        }
        String fingerprint = QueryFingerprint.compute(tableNames, joins);
        Set<String> learnedAlgorithms = planCache.lookup(fingerprint);
        return new QueryExecutionState(fingerprint, learnedAlgorithms, threshold);
    }

    /* ──────────────────────────────────────────────
     * Called after the query finishes
     * ────────────────────────────────────────────── */

    void recordExecution(QueryExecutionState state, long durationNanos,
                         long totalRows) {
        if (!enabled || !learning || state == QueryExecutionState.DISABLED
                || state.fingerprint == null || state.chosenAlgorithms.isEmpty()) {
            return;
        }
        if (!state.replanned && !state.hasSignificantDeviation()) {
            planCache.put(state.fingerprint, state.chosenAlgorithms);
        }
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.log(Level.FINE, "Adaptive optimizer: fingerprint={0} replanned={1} rows={2} devMax={3} cache={4}",
                    new Object[]{state.fingerprint, state.replanned, totalRows,
                            String.format("%.2f", state.maxDeviation), planCache.size()});
        }
    }

    /* ──────────────────────────────────────────────
     * QueryExecutionState – per-query mutable bookkeeping
     * ────────────────────────────────────────────── */

    static final class QueryExecutionState {
        static final QueryExecutionState DISABLED = new QueryExecutionState(null, null, 1.0);

        final String fingerprint;
        final Set<String> chosenAlgorithms;
        private final double threshold;
        boolean replanned;
        double maxDeviation;

        // Actual row counts tracked per pipeline stage
        private long lastEstimatedRows;
        private long lastActualRows;
        private int stepIndex;

        QueryExecutionState(String fingerprint, Set<String> learnedAlgorithms, double threshold) {
            this.fingerprint = fingerprint;
            this.chosenAlgorithms = learnedAlgorithms != null
                    ? new HashSet<>(learnedAlgorithms) : new HashSet<>();
            this.threshold = threshold;
            this.replanned = false;
            this.maxDeviation = 0.0;
            this.stepIndex = 0;
        }

        /**
         * Reports the estimated and actual row count for a pipeline step.
         * Returns {@code true} when replanning is recommended (deviation exceeds
         * the configured threshold).
         */
        boolean reportStep(long estimatedRows, long actualRows) {
            if (fingerprint == null) {
                // No joins → no replanning needed
                return false;
            }
            if (estimatedRows <= 0 || actualRows <= 0) {
                lastEstimatedRows = estimatedRows;
                lastActualRows = actualRows;
                stepIndex++;
                return false;
            }
            double ratio;
            if (estimatedRows > actualRows) {
                ratio = (double) actualRows / estimatedRows;
            } else {
                ratio = (double) estimatedRows / actualRows;
            }
            // ratio is in (0, 1];  1.0 = perfect, 0.0 = totally wrong
            double deviation = 1.0 - ratio;
            if (deviation > maxDeviation) {
                maxDeviation = deviation;
            }
            lastEstimatedRows = estimatedRows;
            lastActualRows = actualRows;
            stepIndex++;
            // Deviation exceeds threshold → replan
            return deviation > threshold;
        }

        boolean hasSignificantDeviation() {
            return maxDeviation > threshold;
        }

        void markReplanned() {
            this.replanned = true;
        }

        void chooseAlgorithm(String algorithmName) {
            if (fingerprint != null) {
                chosenAlgorithms.add(algorithmName);
            }
        }

        boolean isLearned(String algorithmName) {
            return chosenAlgorithms.contains(algorithmName);
        }

        int stepIndex() {
            return stepIndex;
        }
    }

    /* ──────────────────────────────────────────────
     * QueryFingerprint – normalised structural key
     * ────────────────────────────────────────────── */

    static final class QueryFingerprint {
        /**
         * Computes a normalised fingerprint from the list of joined tables and
         * their join types. Table names are sorted so that reordering the FROM
         * clause does not produce a different cache key. Literal values, column
         * names, and alias names are excluded – only structure matters.
         */
        static String compute(List<String> tableNames, List<QueryParser.JoinInfo> joins) {
            if (tableNames == null || tableNames.isEmpty()) {
                return "single";
            }
            List<String> sorted = new ArrayList<>(tableNames);
            Collections.sort(sorted);
            StringBuilder sb = new StringBuilder(sorted.get(0));
            for (int i = 1; i < sorted.size(); i++) {
                sb.append('|').append(sorted.get(i));
            }
            if (joins != null && !joins.isEmpty()) {
                for (QueryParser.JoinInfo j : joins) {
                    sb.append(':').append(j.joinType).append('@').append(j.tableName);
                }
            }
            return sb.toString();
        }
    }

    /* ──────────────────────────────────────────────
     * PlanCache – LRU ConcurrentHashMap
     * ────────────────────────────────────────────── */

    static final class PlanCache {
        private final LinkedHashMap<String, Set<String>> cache;

        PlanCache() {
            this(DEFAULT_CACHE_SIZE);
        }

        PlanCache(int maxSize) {
            this.cache = new LinkedHashMap<>(maxSize * 4 / 3 + 1, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Set<String>> eldest) {
                    return size() > maxSize;
                }
            };
        }

        synchronized Set<String> lookup(String fingerprint) {
            return cache.get(fingerprint);
        }

        synchronized void put(String fingerprint, Set<String> algorithms) {
            if (fingerprint != null && algorithms != null && !algorithms.isEmpty()) {
                cache.put(fingerprint, new HashSet<>(algorithms));
            }
        }

        synchronized void clear() {
            cache.clear();
        }

        synchronized int size() {
            return cache.size();
        }
    }
}
