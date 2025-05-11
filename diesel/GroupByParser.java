package diesel;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

class GroupByParser {
    private static final Logger LOGGER = Logger.getLogger(GroupByParser.class.getName());

    List<String> parseGroupByClause(String groupByClause, String tableName) {
        LOGGER.log(Level.FINE, "Parsing GROUP BY clause: {0}", groupByClause);
        List<String> groupByColumns = new ArrayList<>();
        String[] columns = groupByClause.split(",(?=([^']*'[^']*')*[^']*$)");

        for (String column : columns) {
            String trimmedColumn = column.trim();
            if (trimmedColumn.isEmpty()) {
                throw new IllegalArgumentException("Empty column in GROUP BY clause");
            }

            String normalizedColumn = normalizeColumnName(trimmedColumn, tableName);
            groupByColumns.add(normalizedColumn);
            LOGGER.log(Level.FINE, "Parsed GROUP BY column: {0}", normalizedColumn);
        }

        LOGGER.log(Level.FINE, "Parsed GROUP BY columns: {0}", groupByColumns);
        return groupByColumns;
    }

    private String normalizeColumnName(String column, String tableName) {
        if (column == null || column.isEmpty()) {
            return column;
        }
        String normalized = column.trim();
        if (tableName != null && !normalized.contains(".")) {
            normalized = tableName + "." + normalized;
        }
        return normalized;
    }
}