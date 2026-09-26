package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Strategy class for subquery parsing operations in SubqueryParser.
 * Extracts complex logic from parseSelectItems and parseConditionValue methods
 * to reduce cognitive complexity (S3776) and eliminate brain methods (S6541).
 *
 * @since Prompt 9
 */
public class SubqueryResolver {

    private static final String QUOTED_IDENTIFIER_PATTERN = "\"[^\"]*\"";
    private static final String SIMPLE_IDENTIFIER_PATTERN = "[a-z_]\\w*";
    private static final String IDENTIFIER_PATTERN = "(?:" + QUOTED_IDENTIFIER_PATTERN + "|" + SIMPLE_IDENTIFIER_PATTERN + ")";
    private static final String QUALIFIED_IDENTIFIER_PATTERN = IDENTIFIER_PATTERN + "(?:\\." + IDENTIFIER_PATTERN + ")*+";

    private final QueryParser queryParser;

    public SubqueryResolver() {
        this.queryParser = new QueryParser();
    }

    /**
     * Classifies SELECT items into columns, aggregates, subqueries, and stars.
     * Replaces the complex regex dispatch logic in parseSelectItems.
     *
     * @param selectPart the SELECT clause content (without "SELECT")
     * @param database   the database for subquery parsing
     * @return SelectItems containing classified items and aliases
     */
    public QueryParser.SelectItems classifySelectItems(String selectPart, Database database) {
        List<String> selectItems = splitCommaSeparatedItems(selectPart);
        List<String> columns = new ArrayList<>();
        List<QueryParser.AggregateFunction> aggregates = new ArrayList<>();
        List<QueryParser.SubQuery> subQueries = new ArrayList<>();
        Map<String, String> columnAliases = new HashMap<>();

        // Strategy map for item classification
        Map<ItemType, SelectItemHandler> handlers = new HashMap<>();
        handlers.put(ItemType.SUBQUERY, new SubqueryHandler(database));
        handlers.put(ItemType.AGGREGATE, new AggregateHandler(database));
        handlers.put(ItemType.COLUMN, new ColumnHandler());
        handlers.put(ItemType.STAR, new StarHandler());

        for (String item : selectItems) {
            String trimmedItem = item.trim();
            ItemType type = classifyItemType(trimmedItem);
            SelectItemHandler handler = handlers.get(type);
            
            if (handler != null) {
                handler.handle(trimmedItem, columns, aggregates, subQueries, columnAliases);
            } else {
                throw new IllegalArgumentException("Invalid SELECT item: " + trimmedItem);
            }
        }

        return new QueryParser.SelectItems(columns, aggregates, subQueries, columnAliases);
    }

    /**
     * Converts a raw string value to the correct Java type based on the target column type.
     * Replaces the 12-branch if/else chain with a strategy map pattern.
     *
     * @param column     the column name
     * @param value      the raw string value to parse
     * @param columnType the target Java type for the column
     * @return the parsed value of the correct type
     */
    public Object parseConditionValue(String column, String value, Class<?> columnType) {
        try {
            if (value.startsWith("'") && value.endsWith("'")) {
                // String literal branch
                String strValue = SqlLexer.extractStringLiteral(value);
                return parseStringLiteral(strValue, columnType);
            } else {
                // Numeric/boolean branch - use strategy map
                return parseNumericOrBoolean(value, columnType);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Failed to parse value '" + value + "' for column " + column + ": " + e.getMessage(), e);
        }
    }

    /**
     * Splits comma-separated items while respecting quoted strings, parentheses, and brackets.
     * Extracted from SubqueryParser for shared use.
     *
     * @param input the input string to split
     * @return list of split items
     */
    public List<String> splitCommaSeparatedItems(String input) {
        List<String> items = new ArrayList<>();
        StringBuilder currentItem = new StringBuilder();
        int parenDepth = 0;
        boolean inQuotes = false;
        boolean inQuotedIdentifier = false;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '\'') {
                inQuotes = !inQuotes;
                currentItem.append(c);
            } else if (c == '"' && !inQuotes) {
                inQuotedIdentifier = !inQuotedIdentifier;
                currentItem.append(c);
            } else if (!inQuotes && !inQuotedIdentifier) {
                if (c == '(') {
                    parenDepth++;
                    currentItem.append(c);
                } else if (c == ')') {
                    parenDepth--;
                    currentItem.append(c);
                } else if (c == ',' && parenDepth == 0) {
                    addCommaSeparatedItem(currentItem, items);
                } else {
                    currentItem.append(c);
                }
            } else {
                currentItem.append(c);
            }
        }

        String lastItem = currentItem.toString().trim();
        if (!lastItem.isEmpty()) {
            items.add(lastItem);
        }
        return items;
    }

    // Private helper methods

    private ItemType classifyItemType(String item) {
        // Check in order of precedence: subquery, aggregate, column, star
        if (isSubQueryPattern(item)) return ItemType.SUBQUERY;
        if (isAggregatePattern(item)) return ItemType.AGGREGATE;
        if (isColumnPattern(item)) return ItemType.COLUMN;
        if (isStarPattern(item)) return ItemType.STAR;
        throw new IllegalArgumentException("Unrecognized SELECT item type: " + item);
    }

    private boolean isSubQueryPattern(String item) {
        return item.toUpperCase().startsWith("(") && item.toUpperCase().contains(SqlKeywords.SELECT);
    }

    private boolean isAggregatePattern(String item) {
        Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*+\\(\\s*+(" + QUALIFIED_IDENTIFIER_PATTERN + "|\\*|\\(\\s*+SELECT\\s++(?:[^()']++|'(?:\\\\.|[^'\\\\])*+'|\\([^()]*+\\))*+\\))\\s*+\\)(?:\\s++(?:AS\\s++)?(" + IDENTIFIER_PATTERN + "))?$", Pattern.DOTALL);
        return aggPattern.matcher(item).matches();
    }

    private boolean isColumnPattern(String item) {
        Pattern columnPattern = Pattern.compile("(?i)" + QUALIFIED_IDENTIFIER_PATTERN + "(?:\\s++(?:AS\\s++)?(" + IDENTIFIER_PATTERN + "))?$");
        return columnPattern.matcher(item).matches();
    }

    private boolean isStarPattern(String item) {
        return item.equals("*");
    }

    private Object parseStringLiteral(String strValue, Class<?> columnType) {
        if (columnType == String.class) {
            return strValue;
        } else if (columnType == UUID.class) {
            return UUID.fromString(strValue);
        } else if (columnType == LocalDate.class) {
            return LocalDate.parse(strValue);
        } else if (columnType == LocalDateTime.class) {
            return strValue.contains(".") ?
                    LocalDateTime.parse(strValue, QueryParser.DATETIME_MS_FORMATTER) :
                    LocalDateTime.parse(strValue, QueryParser.DATETIME_FORMATTER);
        }
        throw new IllegalArgumentException("Unsupported string literal type for column: " + columnType);
    }

    private Object parseNumericOrBoolean(String value, Class<?> columnType) {
        // Strategy map for numeric/boolean type conversion
        Map<Class<?>, Function<String, Object>> converters = new HashMap<>();
        converters.put(Integer.class, Integer::parseInt);
        converters.put(Long.class, Long::parseLong);
        converters.put(Short.class, Short::parseShort);
        converters.put(Byte.class, Byte::parseByte);
        converters.put(BigDecimal.class, BigDecimal::new);
        converters.put(Float.class, Float::parseFloat);
        converters.put(Double.class, Double::parseDouble);
        converters.put(Boolean.class, Boolean::parseBoolean);

        Function<String, Object> converter = converters.get(columnType);
        if (converter != null) {
            return converter.apply(value);
        }

        throw new IllegalArgumentException("Unsupported value type for column: " + columnType);
    }

    private static void addCommaSeparatedItem(StringBuilder currentItem, List<String> items) {
        String item = currentItem.toString().trim();
        if (!item.isEmpty()) {
            items.add(item);
        }
        currentItem.setLength(0);
    }

    // Inner classes for strategy pattern

    private interface SelectItemHandler {
        void handle(String item, List<String> columns, List<QueryParser.AggregateFunction> aggregates,
                   List<QueryParser.SubQuery> subQueries, Map<String, String> columnAliases);
    }

    private enum ItemType {
        SUBQUERY, AGGREGATE, COLUMN, STAR
    }

    private class SubqueryHandler implements SelectItemHandler {
        private final Database database;

        SubqueryHandler(Database database) {
            this.database = database;
        }

        @Override
        public void handle(String item, List<String> columns, List<QueryParser.AggregateFunction> aggregates,
                          List<QueryParser.SubQuery> subQueries, Map<String, String> columnAliases) {
            String subQueryStr = item.substring(1, item.lastIndexOf(")")).trim();
            String alias = extractAlias(item);
            validateSubQuery(subQueryStr);
            Query<?> subQuery = queryParser.parse(subQueryStr, database);
            subQueries.add(new QueryParser.SubQuery(subQuery, alias));
            if (alias != null) {
                columnAliases.put("subquery_" + subQueries.size(), alias);
            }
        }
    }

    private class AggregateHandler implements SelectItemHandler {
        private final Database database;

        AggregateHandler(Database database) {
            this.database = database;
        }

        @Override
        public void handle(String item, List<String> columns, List<QueryParser.AggregateFunction> aggregates,
                          List<QueryParser.SubQuery> subQueries, Map<String, String> columnAliases) {
            Pattern aggPattern = Pattern.compile("(?i)^(COUNT|MIN|MAX|AVG|SUM)\\s*+\\(\\s*+([^)]+)\\s*+\\)(?:\\s++(?:AS\\s++)?(" + IDENTIFIER_PATTERN + "))?$", Pattern.DOTALL);
            Matcher matcher = aggPattern.matcher(item);
            if (matcher.matches()) {
                String funcName = matcher.group(1);
                String arg = matcher.group(2);
                String alias = extractAlias(matcher.group(3));

                if (arg.toUpperCase().startsWith("(") && arg.toUpperCase().contains(SqlKeywords.SELECT)) {
                    String subQueryStr = arg.substring(1, arg.length() - 1).trim();
                    validateSubQuery(subQueryStr);
                    Query<?> subQuery = queryParser.parse(subQueryStr, database);
                    aggregates.add(new QueryParser.AggregateFunction(funcName, new QueryParser.SubQuery(subQuery, null), alias));
                } else {
                    String column = arg.equals("*") ? null : unquoteQualifiedIdentifier(arg);
                    aggregates.add(new QueryParser.AggregateFunction(funcName, column, alias));
                }
            }
        }
    }

    private class ColumnHandler implements SelectItemHandler {
        @Override
        public void handle(String item, List<String> columns, List<QueryParser.AggregateFunction> aggregates,
                          List<QueryParser.SubQuery> subQueries, Map<String, String> columnAliases) {
            String column = unquoteQualifiedIdentifier(item);
            String alias = extractAlias(item);
            columns.add(column);
            if (alias != null) {
                columnAliases.put(column, alias);
            }
        }
    }

    private class StarHandler implements SelectItemHandler {
        @Override
        public void handle(String item, List<String> columns, List<QueryParser.AggregateFunction> aggregates,
                          List<QueryParser.SubQuery> subQueries, Map<String, String> columnAliases) {
            columns.add("*");
        }
    }

    // Utility methods (extracted from original SubqueryParser)

    private static String unquoteIdentifier(String identifier) {
        return SqlParsingUtils.unquoteIdentifier(identifier);
    }

    private static String unquoteQualifiedIdentifier(String identifier) {
        return SqlParsingUtils.unquoteQualifiedIdentifier(identifier);
    }

    private static String extractAlias(String item) {
        if (item == null) return null;
        Pattern aliasPattern = Pattern.compile("(?i)AS\\s+(" + IDENTIFIER_PATTERN + ")$", Pattern.DOTALL);
        Matcher matcher = aliasPattern.matcher(item);
        if (matcher.find()) {
            return unquoteIdentifier(matcher.group(1));
        }
        return null;
    }

    private static void validateSubQuery(String subQueryStr) {
        // Implementation would validate subQuery syntax
        if (subQueryStr == null || subQueryStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Subquery cannot be empty");
        }
    }
}