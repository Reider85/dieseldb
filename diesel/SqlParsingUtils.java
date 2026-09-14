package diesel;

import diesel.storage.json.JsonPathResolver;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
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
     * qualified (table.column) identifier. A dotted identifier quoted as a
     * whole ({@code "user.address.city"}) is unquoted first, then each
     * dot-separated part is unquoted individually.
     */
    static String unquoteQualifiedIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }
        String trimmed = identifier.trim();
        if (trimmed.length() >= 2 && trimmed.charAt(0) == '"' && trimmed.charAt(trimmed.length() - 1) == '"') {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        String[] parts = trimmed.split("\\.", -1);
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
     * Resolves table aliases to actual table names. Dotted identifiers that
     * do not start with a known table alias are kept whole, so a nested JSON
     * leaf ({@code user.address.city}) or a JSON Path on a whole-value column
     * ({@code PROFILE.user.address.city}) survives table-qualification.
     */
    static String normalizeColumnName(String column, String defaultTableName, Map<String, String> tableAliases) {
        Objects.requireNonNull(defaultTableName, "Default table name must not be null");
        Objects.requireNonNull(tableAliases, "Table aliases must not be null");
        String unquoted = unquoteQualifiedIdentifier(column);
        // Prompt 37 (java:S2259): unquoteQualifiedIdentifier returns null when
        // the input column is null; dereferencing it below would NPE.
        if (unquoted == null) {
            throw new QueryParseException("Column name must not be null");
        }
        if (unquoted.contains(".")) {
            String[] parts = unquoted.split("\\.");
            String tableOrAlias = parts[0].trim();
            String tableName = resolveTableOrAlias(tableOrAlias, tableAliases);
            if (tableName != null) {
                return tableName + "." + String.join(".", partsFrom(parts, 1));
            }
            return unquoted.trim();
        }
        return defaultTableName + "." + unquoted.trim();
    }

    private static String resolveTableOrAlias(String prefix, Map<String, String> tableAliases) {
        for (Map.Entry<String, String> entry : tableAliases.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(prefix)) {
                return entry.getValue();
            }
            if (entry.getValue().equalsIgnoreCase(prefix)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String[] partsFrom(String[] parts, int from) {
        String[] tail = new String[parts.length - from];
        System.arraycopy(parts, from, tail, 0, tail.length);
        return tail;
    }

    /**
     * Converts an operator string to the corresponding Operator enum.
     */
    static QueryParser.Operator parseOperator(String operatorStr) {
        Objects.requireNonNull(operatorStr, "Operator string must not be null");
        return switch (operatorStr.toUpperCase().trim()) {
            case "=" -> QueryParser.Operator.EQUALS;
            case "!=", "<>" -> QueryParser.Operator.NOT_EQUALS;
            case "<" -> QueryParser.Operator.LESS_THAN;
            case ">" -> QueryParser.Operator.GREATER_THAN;
            case "<=" -> QueryParser.Operator.LESS_THAN_OR_EQUALS;
            case ">=" -> QueryParser.Operator.GREATER_THAN_OR_EQUALS;
            case SqlKeywords.LIKE -> QueryParser.Operator.LIKE;
            case SqlKeywords.NOT_LIKE -> QueryParser.Operator.NOT_LIKE;
            default -> throw new IllegalArgumentException(ErrorMessages.UNSUPPORTED_OPERATOR_PREFIX + operatorStr);
        };
    }

    /**
     * Validates that a column exists in the combined column types map.
     * A dotted path matches when it is an exact (case-insensitive) column name
     * or resolves to a JSON Path prefix; a leading {@code TABLE.} segment, if
     * present, is dropped before the JSON Path lookup.
     * Throws IllegalArgumentException if the column is not found.
     */
    static void validateColumn(String column, Map<String, Class<?>> combinedColumnTypes) {
        Objects.requireNonNull(column, "Column name must not be null");
        if (!matchesColumnOrPath(column, combinedColumnTypes.keySet())) {
            LOGGER.log(Level.SEVERE, "Unknown column: {0}, available columns: {1}",
                    new Object[]{column, combinedColumnTypes.keySet()});
            throw new IllegalArgumentException(ErrorMessages.UNKNOWN_COLUMN_PREFIX + column);
        }
    }

    private static boolean matchesColumnOrPath(String column, Collection<String> schemaColumns) {
        for (String key : schemaColumns) {
            if (key.equalsIgnoreCase(column)) {
                return true;
            }
        }
        return JsonPathResolver.resolveQualified(schemaColumns, column).columnIndex() >= 0;
    }

    /**
     * Resolves the Java type of a possibly dotted column against the combined
     * column type map. Exact column names win, then the longest JSON Path
     * column prefix; a leading {@code TABLE.} segment is dropped before the
     * path lookup. Returns {@code null} when the column is not found.
     */
    static Class<?> resolveColumnType(String column, Map<String, Class<?>> combinedColumnTypes) {
        JsonPathResolver.ResolvedPath resolved = JsonPathResolver.resolveQualified(combinedColumnTypes.keySet(), column);
        if (resolved.columnIndex() < 0) {
            return null;
        }
        Class<?> type = combinedColumnTypes.get(resolved.column());
        if (type != null) {
            return type;
        }
        for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(resolved.column())) {
                return entry.getValue();
            }
        }
        return null;
    }
}
