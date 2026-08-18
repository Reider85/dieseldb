package diesel;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Shared SQL parsing utility methods for QueryParser and SubqueryParser.
 * Eliminates code duplication by providing a single implementation of
 * common parsing operations.
 */
class SqlParsingUtils {
    private static final Logger LOGGER = Logger.getLogger(SqlParsingUtils.class.getName());

    /**
     * Removes the surrounding double quotes from a quoted identifier.
     * Identifiers that are not quoted are returned unchanged.
     */
    static String unquoteIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }
        String trimmed = identifier.trim();
        if (trimmed.length() >= 2 && trimmed.charAt(0) == '"' && trimmed.charAt(trimmed.length() - 1) == '"') {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    /**
     * Removes the surrounding double quotes from each part of a possibly
     * qualified (table.column) identifier.
     */
    static String unquoteQualifiedIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }
        String[] parts = identifier.split("\\.", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append('.');
            }
            sb.append(unquoteIdentifier(parts[i]));
        }
        return sb.toString();
    }

    /**
     * Normalizes a column name to the format "table.column".
     * Resolves table aliases to actual table names.
     */
    static String normalizeColumnName(String column, String defaultTableName, Map<String, String> tableAliases) {
        String unquoted = unquoteQualifiedIdentifier(column);
        if (unquoted.contains(".")) {
            String[] parts = unquoted.split("\\.");
            String tableOrAlias = parts[0].trim();
            String colName = parts[1].trim();
            String tableName = tableAliases.getOrDefault(tableOrAlias, tableOrAlias);
            return tableName + "." + colName;
        }
        return defaultTableName + "." + unquoted.trim();
    }

    /**
     * Converts an operator string to the corresponding Operator enum.
     */
    static QueryParser.Operator parseOperator(String operatorStr) {
        return switch (operatorStr.toUpperCase().trim()) {
            case "=" -> QueryParser.Operator.EQUALS;
            case "!=", "<>" -> QueryParser.Operator.NOT_EQUALS;
            case "<" -> QueryParser.Operator.LESS_THAN;
            case ">" -> QueryParser.Operator.GREATER_THAN;
            case "<=" -> QueryParser.Operator.LESS_THAN_OR_EQUALS;
            case ">=" -> QueryParser.Operator.GREATER_THAN_OR_EQUALS;
            case SqlKeywords.LIKE -> QueryParser.Operator.LIKE;
            case SqlKeywords.NOT_LIKE -> QueryParser.Operator.NOT_LIKE;
            default -> throw new IllegalArgumentException("Unsupported operator: " + operatorStr);
        };
    }

    /**
     * Validates that a column exists in the combined column types map.
     * Throws IllegalArgumentException if the column is not found.
     */
    static void validateColumn(String column, Map<String, Class<?>> combinedColumnTypes) {
        String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
        boolean found = false;
        for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
            String entryKeyUnqualified = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1].trim() : entry.getKey();
            if (entryKeyUnqualified.equalsIgnoreCase(unqualifiedColumn)) {
                found = true;
                break;
            }
        }
        if (!found) {
            LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                    new Object[]{column, combinedColumnTypes.keySet()});
            throw new IllegalArgumentException("Unknown column: " + column);
        }
    }
}
