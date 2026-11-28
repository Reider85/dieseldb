package diesel.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class ColumnStatistics implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String columnName;
    private final long rowCount;
    private final long nullCount;
    private final int distinctCount;
    private final List<ValueFrequency> mostCommonValues;
    private final List<Object> histogramBounds;
    private final long analyzedAtMillis;

    public ColumnStatistics(String columnName, long rowCount, long nullCount, int distinctCount,
                            List<ValueFrequency> mostCommonValues, List<Object> histogramBounds,
                            long analyzedAtMillis) {
        this.columnName = columnName;
        this.rowCount = rowCount;
        this.nullCount = nullCount;
        this.distinctCount = distinctCount;
        this.mostCommonValues = List.copyOf(mostCommonValues);
        this.histogramBounds = List.copyOf(histogramBounds);
        this.analyzedAtMillis = analyzedAtMillis;
    }

    public static final class ValueFrequency implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Object value;
        private final long count;
        private final double frequency;

        public ValueFrequency(Object value, long count, double frequency) {
            this.value = value;
            this.count = count;
            this.frequency = frequency;
        }

        public Object getValue() {
            return value;
        }

        public long getCount() {
            return count;
        }

        public double getFrequency() {
            return frequency;
        }
    }

    public String getColumnName() {
        return columnName;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getNullCount() {
        return nullCount;
    }

    public double getNullFraction() {
        return rowCount == 0 ? 0.0 : (double) nullCount / rowCount;
    }

    public int getDistinctCount() {
        return distinctCount;
    }

    public List<ValueFrequency> getMostCommonValues() {
        return mostCommonValues;
    }

    public List<Object> getHistogramBounds() {
        return histogramBounds;
    }

    public long getAnalyzedAtMillis() {
        return analyzedAtMillis;
    }

    public Map<String, Object> toRow(String tableName) {
        Map<String, Object> row = new HashMap<>();
        row.put(StatisticsCollector.COL_TABLE, tableName);
        row.put(StatisticsCollector.COL_COLUMN, columnName);
        row.put(StatisticsCollector.COL_ROW_COUNT, Long.toString(rowCount));
        row.put(StatisticsCollector.COL_NULL_FRAC, String.format(Locale.ROOT, "%.6f", getNullFraction()));
        row.put(StatisticsCollector.COL_DISTINCT, Integer.toString(distinctCount));
        List<String> encodedMcv = new ArrayList<>(mostCommonValues.size());
        for (ValueFrequency entry : mostCommonValues) {
            encodedMcv.add(escape(String.valueOf(entry.getValue())) + "|" + entry.getCount());
        }
        row.put(StatisticsCollector.COL_MCV, String.join(";", encodedMcv));
        List<String> encodedBounds = new ArrayList<>(histogramBounds.size());
        for (Object bound : histogramBounds) {
            encodedBounds.add(escape(String.valueOf(bound)));
        }
        row.put(StatisticsCollector.COL_HISTOGRAM, String.join(";", encodedBounds));
        row.put(StatisticsCollector.COL_ANALYZED_AT, Long.toString(analyzedAtMillis));
        return row;
    }

    public static ColumnStatistics fromRow(Map<String, Object> row) {
        String column = text(row.get(StatisticsCollector.COL_COLUMN));
        long rowCount = parseLong(row.get(StatisticsCollector.COL_ROW_COUNT));
        long nullCount = Math.round(parseDouble(row.get(StatisticsCollector.COL_NULL_FRAC)) * rowCount);
        int distinct = (int) parseLong(row.get(StatisticsCollector.COL_DISTINCT));
        long analyzedAt = parseLong(row.get(StatisticsCollector.COL_ANALYZED_AT));
        List<ValueFrequency> mcv = new ArrayList<>();
        String encodedMcv = text(row.get(StatisticsCollector.COL_MCV));
        if (!encodedMcv.isEmpty()) {
            for (String entry : splitEscaped(encodedMcv, ';')) {
                List<String> parts = splitEscaped(entry, '|');
                if (parts.size() == 2) {
                    long count = parseLong(parts.get(1));
                    double frequency = rowCount == 0 ? 0.0 : (double) count / rowCount;
                    mcv.add(new ValueFrequency(parts.get(0), count, frequency));
                }
            }
        }
        List<Object> bounds = new ArrayList<>();
        String encodedBounds = text(row.get(StatisticsCollector.COL_HISTOGRAM));
        if (!encodedBounds.isEmpty()) {
            for (String bound : splitEscaped(encodedBounds, ';')) {
                bounds.add(bound);
            }
        }
        return new ColumnStatistics(column, rowCount, nullCount, distinct, mcv, bounds, analyzedAt);
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("|", "\\|").replace(";", "\\;");
    }

    private static List<String> splitEscaped(String text, char delimiter) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (escaped) {
                current.append(c);
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == delimiter) {
                parts.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        parts.add(current.toString());
        return parts;
    }

    private static String text(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private static long parseLong(Object value) {
        try {
            return Long.parseLong(text(value).trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static double parseDouble(Object value) {
        try {
            return Double.parseDouble(text(value).trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}
