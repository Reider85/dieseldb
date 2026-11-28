package diesel.optimizer;

import diesel.ConfigLoader;
import diesel.Database;
import diesel.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public final class StatisticsCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsCollector.class);

    public static final String STATS_TABLE = "DIESEL_STATS";
    public static final List<String> STATS_COLUMNS = List.of(
            "TABLE_NAME", "COLUMN_NAME", "ROW_COUNT", "NULL_FRAC", "DISTINCT_VALUES",
            "MOST_COMMON_VALS", "HISTOGRAM_BOUNDS", "LAST_ANALYZED");

    static final String COL_TABLE = "TABLE_NAME";
    static final String COL_COLUMN = "COLUMN_NAME";
    static final String COL_ROW_COUNT = "ROW_COUNT";
    static final String COL_NULL_FRAC = "NULL_FRAC";
    static final String COL_DISTINCT = "DISTINCT_VALUES";
    static final String COL_MCV = "MOST_COMMON_VALS";
    static final String COL_HISTOGRAM = "HISTOGRAM_BOUNDS";
    static final String COL_ANALYZED_AT = "LAST_ANALYZED";

    private static final String THRESHOLD_KEY = "auto_analyze.threshold";
    private static final String MIN_CHANGES_KEY = "auto_analyze.min.changes";
    private static final String BUCKETS_KEY = "auto_analyze.histogram.buckets";
    private static final String MCV_KEY = "auto_analyze.mcv.limit";
    private static final double DEFAULT_THRESHOLD = 0.2;
    private static final int DEFAULT_MIN_CHANGES = 100;
    private static final int DEFAULT_BUCKETS = Histogram.DEFAULT_BUCKETS;
    private static final int DEFAULT_MCV = 10;

    private static final Set<String> SCHEDULED = ConcurrentHashMap.newKeySet();
    private static final ScheduledExecutorService EXECUTOR =
            Executors.newSingleThreadScheduledExecutor(new StatsThreadFactory());
    private static volatile Double thresholdOverride;

    private StatisticsCollector() {
    }

    public static void setThresholdForTest(Double value) {
        thresholdOverride = value;
    }

    public static double threshold() {
        if (thresholdOverride != null) {
            return thresholdOverride;
        }
        String property = System.getProperty(THRESHOLD_KEY);
        if (property != null) {
            try {
                return Double.parseDouble(property.trim());
            } catch (NumberFormatException ignored) {
                return ConfigLoader.getDouble(THRESHOLD_KEY, DEFAULT_THRESHOLD);
            }
        }
        return ConfigLoader.getDouble(THRESHOLD_KEY, DEFAULT_THRESHOLD);
    }

    public static Map<String, ColumnStatistics> collect(Table table) {
        List<String> columns = table.getColumns();
        List<Map<String, Object>> rows = table.getLiveRows();
        long rowCount = rows.size();
        long analyzedAt = System.currentTimeMillis();
        int buckets = intConfig(BUCKETS_KEY, DEFAULT_BUCKETS);
        int mcvLimit = intConfig(MCV_KEY, DEFAULT_MCV);
        Map<String, ColumnStatistics> result = new LinkedHashMap<>();
        for (String column : columns) {
            Map<Object, Long> frequencies = new HashMap<>();
            long nullCount = 0;
            for (Map<String, Object> row : rows) {
                Object value = row.get(column);
                if (value == null) {
                    nullCount++;
                } else {
                    frequencies.merge(value, 1L, Long::sum);
                }
            }
            List<Map.Entry<Object, Long>> ranked = new ArrayList<>(frequencies.entrySet());
            ranked.sort((left, right) -> {
                int byCount = Long.compare(right.getValue(), left.getValue());
                return byCount != 0 ? byCount : Histogram.compare(left.getKey(), right.getKey());
            });
            List<ColumnStatistics.ValueFrequency> common = new ArrayList<>();
            for (int i = 0; i < Math.min(mcvLimit, ranked.size()); i++) {
                Map.Entry<Object, Long> entry = ranked.get(i);
                double frequency = rowCount == 0 ? 0.0 : (double) entry.getValue() / rowCount;
                common.add(new ColumnStatistics.ValueFrequency(entry.getKey(), entry.getValue(), frequency));
            }
            List<Object> sorted = new ArrayList<>(frequencies.keySet());
            sorted.sort(Histogram.comparator());
            List<Object> bounds = Histogram.equiDepth(sorted, buckets);
            result.put(column, new ColumnStatistics(column, rowCount, nullCount,
                    frequencies.size(), common, bounds, analyzedAt));
        }
        return result;
    }

    public static Map<String, ColumnStatistics> analyzeAndStore(Database database, Table table) {
        Map<String, ColumnStatistics> stats = collect(table);
        table.resetRowsChangedSinceAnalyze();
        table.setColumnStatistics(stats);
        if (database != null && !STATS_TABLE.equals(table.getName())) {
            Table statsTable = database.ensureStatsTable();
            List<Map<String, Object>> rows = new ArrayList<>(stats.size());
            for (ColumnStatistics columnStats : stats.values()) {
                rows.add(columnStats.toRow(table.getName()));
            }
            statsTable.replaceRowsAtomically(rows);
        }
        return stats;
    }

    public static void noteRowsChanged(Table table, long rows) {
        if (table != null && rows > 0) {
            table.noteRowsChanged(rows);
        }
    }

    public static void maybeAutoAnalyze(Database database, Table table) {
        if (database == null || table == null || STATS_TABLE.equals(table.getName())) {
            return;
        }
        double limit = threshold();
        if (limit <= 0) {
            return;
        }
        long changes = table.getRowsChangedSinceAnalyze();
        if (changes < intConfig(MIN_CHANGES_KEY, DEFAULT_MIN_CHANGES)
                || changes <= limit * Math.max(1L, table.getLiveRowCount())) {
            return;
        }
        String tableName = table.getName();
        if (!SCHEDULED.add(tableName)) {
            return;
        }
        try {
            EXECUTOR.execute(() -> {
                try {
                    analyzeAndStore(database, table);
                } catch (RuntimeException e) {
                    LOGGER.warn("Auto-analyze failed for {}: {}", tableName, e.toString());
                } finally {
                    SCHEDULED.remove(tableName);
                }
            });
        } catch (RejectedExecutionException e) {
            SCHEDULED.remove(tableName);
        }
    }

    private static int intConfig(String key, int defaultValue) {
        String property = System.getProperty(key);
        if (property != null) {
            try {
                return Integer.parseInt(property.trim());
            } catch (NumberFormatException ignored) {
                return ConfigLoader.getInt(key, defaultValue);
            }
        }
        return ConfigLoader.getInt(key, defaultValue);
    }

    private static final class StatsThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "diesel-auto-analyze");
            thread.setDaemon(true);
            return thread;
        }
    }
}
