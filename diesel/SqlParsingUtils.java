package diesel;

import diesel.storage.json.JsonPathResolver;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

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
    @Nullable
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
    @Nullable
    static String unquoteQualifiedIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }
        String trimmed = identifier.trim();
        // For multi-part quoted identifiers like "table"."col", each part is
        // independently quoted so we must NOT strip the outermost quotes.
        // Only strip when the identifier is a single quoted segment (no dot
        // between the outermost quotes) or an unquoted dotted name.
        boolean singleQuoted = trimmed.length() >= 2
                && trimmed.charAt(0) == '"' && trimmed.charAt(trimmed.length() - 1) == '"'
                && !trimmed.substring(1, trimmed.length() - 1).contains("\".");
        if (singleQuoted) {
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
     * Applies {@code transform} only to the segments of {@code input} that lie
     * outside single-quoted string literals; the literals themselves are
     * copied verbatim. Prompt 4 #49 (R3-013c): query normalization must never
     * mutate the contents of a string literal (case, whitespace around
     * operators inside the literal, or words that happen to look like SQL
     * keywords such as {@code LIKE}).
     *
     * <p>The scanner honors the same escape rules as
     * {@link SqlLexer#extractStringLiteral}: a doubled quote {@code ''} and a
     * backslash escape {@code \'} both stay inside the literal, so an
     * unterminated literal keeps the remainder of the input verbatim (the
     * lexer itself is the component that reports unterminated literals).
     *
     * @param input     the text to process
     * @param transform the regex chain applied to non-literal segments
     * @return the reassembled text
     */
    static String mapOutsideSingleQuotes(String input, Function<String, String> transform) {
        Objects.requireNonNull(input, "input must not be null");
        Objects.requireNonNull(transform, "transform must not be null");
        StringBuilder result = new StringBuilder(input.length());
        StringBuilder outside = new StringBuilder();
        int i = 0;
        int n = input.length();
        while (i < n) {
            char c = input.charAt(i);
            if (c == '\'') {
                if (outside.length() > 0) {
                    result.append(transform.apply(outside.toString()));
                    outside.setLength(0);
                }
                result.append(c);
                i++;
                while (i < n) {
                    char literalChar = input.charAt(i);
                    result.append(literalChar);
                    i++;
                    if (literalChar == '\\' && i < n) {
                        result.append(input.charAt(i));
                        i++;
                    } else if (literalChar == '\'') {
                        if (i < n && input.charAt(i) == '\'') {
                            result.append(input.charAt(i));
                            i++;
                        } else {
                            break;
                        }
                    }
                }
            } else {
                outside.append(c);
                i++;
            }
        }
        if (outside.length() > 0) {
            result.append(transform.apply(outside.toString()));
        }
        return result.toString();
    }

    /**
     * Validates that a column exists in the combined column types map.
     * A dotted path matches when it is an exact (case-insensitive) column name
     * or resolves to a JSON Path prefix; a leading {@code TABLE.} segment, if
     * present, is dropped before the JSON Path lookup.
     * Throws IllegalArgumentException if the column is not found.
     */
    /**
     * Strictly parses a boolean literal, accepting the synonyms
     * {@code true/false}, {@code 1/0}, {@code yes/no} and {@code t/f}
     * (case-insensitive, surrounding whitespace ignored). Unlike
     * {@link Boolean#parseBoolean(String)} this never silently treats an
     * arbitrary string as {@code false}, so a typo like {@code = 2} on a
     * BOOLEAN column raises a parse error instead of matching FALSE rows.
     * Mirrors the synonym set used by the storage readers
     * ({@code DelimitedRowReader.parseBooleanStrict}).
     *
     * @param raw the raw literal text
     * @return the parsed boolean
     * @throws IllegalArgumentException when the value is not a boolean synonym
     */
    static Boolean parseBooleanLiteral(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("Invalid boolean value: null");
        }
        return switch (raw.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "true", "1", "yes", "t" -> Boolean.TRUE;
            case "false", "0", "no", "f" -> Boolean.FALSE;
            default -> throw new IllegalArgumentException("Invalid boolean value: '" + raw + "'");
        };
    }

    /**
     * Applies the {@code compat.uppercase.string.literals} policy to an
     * extracted string-literal value. Prompt 4 #49 (R3-013c): the default
     * (false) returns the value unchanged so literals keep their original
     * case; the legacy mode (true, sysprop or config.properties) uppercases
     * the value at parse time, reproducing the pre-#49 parser behavior where
     * every literal was normalized through {@code toUpperCase()}.
     *
     * <p>Called at every site that extracts a single-quoted literal value
     * ({@code SqlLexer.extractStringLiteral} consumers), so INSERT values,
     * WHERE/JOIN ON comparisons and constant SELECTs all observe the same
     * policy.
     *
     * @param value the extracted literal (quotes already stripped)
     * @return the case-policy-applied value
     */
    static String normalizeStringLiteralCase(String value) {
        if (value == null || !ConfigLoader.getCompatUppercaseStringLiterals()) {
            return value;
        }
        return value.toUpperCase();
    }

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
    @Nullable
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
