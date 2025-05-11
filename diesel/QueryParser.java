package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

public class QueryParser {
    private static final Logger LOGGER = Logger.getLogger(QueryParser.class.getName());
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATETIME_MS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String UUID_PATTERN = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

    private final SelectQueryParser selectQueryParser;
    private final InsertQueryParser insertQueryParser;
    private final UpdateQueryParser updateQueryParser;
    private final DeleteQueryParser deleteQueryParser;
    private final CreateTableQueryParser createTableQueryParser;
    private final IndexQueryParser indexQueryParser;
    private final TransactionQueryParser transactionQueryParser;

    public QueryParser() {
        this.selectQueryParser = new SelectQueryParser();
        this.insertQueryParser = new InsertQueryParser();
        this.updateQueryParser = new UpdateQueryParser();
        this.deleteQueryParser = new DeleteQueryParser();
        this.createTableQueryParser = new CreateTableQueryParser();
        this.indexQueryParser = new IndexQueryParser();
        this.transactionQueryParser = new TransactionQueryParser();
    }

    enum Operator {
        EQUALS, NOT_EQUALS, LESS_THAN, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL, IN, LIKE, NOT_LIKE, IS_NULL, IS_NOT_NULL
    }

    enum JoinType {
        INNER, LEFT_INNER, RIGHT_INNER, LEFT_OUTER, RIGHT_OUTER, FULL_OUTER, CROSS
    }

    public static String convertLikePatternToRegex(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            throw new IllegalArgumentException("LIKE pattern cannot be null or empty");
        }

        StringBuilder escapedPattern = new StringBuilder();
        for (char c : pattern.toCharArray()) {
            if (".^$*+?()[{\\|".indexOf(c) >= 0) {
                escapedPattern.append('\\').append(c);
            } else {
                escapedPattern.append(c);
            }
        }

        String regex = escapedPattern.toString()
                .replace("%", ".*")
                .replace("_", ".");

        return "^" + regex + "$";
    }

    public Query<?> parse(String query, Database database) {
        try {
            String normalized = NormalizationUtils.normalizeQuery(query);
            LOGGER.log(Level.FINE, "Original query: {0}", query);
            LOGGER.log(Level.INFO, "Normalized query: {0}", normalized);
            if (normalized.startsWith("SELECT")) {
                return selectQueryParser.parseSelectQuery(normalized, query, database);
            } else if (normalized.startsWith("INSERT INTO")) {
                return insertQueryParser.parseInsertQuery(normalized, query, database);
            } else if (normalized.startsWith("UPDATE")) {
                return updateQueryParser.parseUpdateQuery(normalized, query, database);
            } else if (normalized.startsWith("DELETE FROM")) {
                return deleteQueryParser.parseDeleteQuery(normalized, query, database);
            } else if (normalized.startsWith("CREATE TABLE")) {
                return createTableQueryParser.parseCreateTableQuery(normalized, query);
            } else if (normalized.startsWith("CREATE UNIQUE CLUSTERED INDEX")) {
                return indexQueryParser.parseCreateUniqueDurableClusteredIndexQuery(normalized);
            } else if (normalized.startsWith("CREATE UNIQUE INDEX")) {
                return indexQueryParser.parseCreateUniqueIndexQuery(normalized);
            } else if (normalized.startsWith("CREATE HASH INDEX")) {
                return indexQueryParser.parseCreateHashIndexQuery(normalized);
            } else if (normalized.startsWith("CREATE INDEX")) {
                return indexQueryParser.parseCreateIndexQuery(normalized);
            } else {
                return transactionQueryParser.parseTransactionQuery(normalized);
            }
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse query: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
    }

    public static Object parseConditionValue(String conditionColumn, String valueStr, Class<?> columnType) {
        try {
            if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
                String strippedValue = valueStr.substring(1, valueStr.length() - 1);
                if (columnType == String.class) {
                    return strippedValue;
                } else if (columnType == LocalDate.class && strippedValue.matches("\\d{4}-\\d{2}-\\d{2}")) {
                    return LocalDate.parse(strippedValue);
                } else if (columnType == LocalDateTime.class && strippedValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}")) {
                    return LocalDateTime.parse(strippedValue, DATETIME_MS_FORMATTER);
                } else if (columnType == LocalDateTime.class && strippedValue.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")) {
                    return LocalDateTime.parse(strippedValue, DATETIME_FORMATTER);
                } else if (columnType == UUID.class && strippedValue.matches(UUID_PATTERN)) {
                    return UUID.fromString(strippedValue);
                } else if (columnType == Character.class && strippedValue.length() == 1) {
                    return strippedValue.charAt(0);
                } else {
                    throw new IllegalArgumentException("Value '" + strippedValue + "' does not match column type: " + columnType.getSimpleName());
                }
            } else if (valueStr.equalsIgnoreCase("TRUE") || valueStr.equalsIgnoreCase("FALSE")) {
                if (columnType == Boolean.class) {
                    return Boolean.parseBoolean(valueStr);
                } else {
                    throw new IllegalArgumentException("Boolean value '" + valueStr + "' does not match column type: " + columnType.getSimpleName());
                }
            } else {
                try {
                    if (columnType == BigDecimal.class) {
                        return new BigDecimal(valueStr);
                    } else if (columnType == Float.class) {
                        BigDecimal bd = new BigDecimal(valueStr);
                        float floatValue = bd.floatValue();
                        if (Float.isInfinite(floatValue) || Float.isNaN(floatValue)) {
                            throw new IllegalArgumentException("Numeric value '" + valueStr + "' out of range for Float");
                        }
                        return floatValue;
                    } else if (columnType == Double.class) {
                        BigDecimal bd = new BigDecimal(valueStr);
                        double doubleValue = bd.doubleValue();
                        if (Double.isInfinite(doubleValue) || Double.isNaN(doubleValue)) {
                            throw new IllegalArgumentException("Numeric value '" + valueStr + "' out of range for Double");
                        }
                        return doubleValue;
                    } else if (columnType == Byte.class) {
                        long parsedLong = Long.parseLong(valueStr);
                        if (parsedLong >= Byte.MIN_VALUE && parsedLong <= Byte.MAX_VALUE) {
                            return (byte) parsedLong;
                        } else {
                            throw new IllegalArgumentException("Numeric value '" + valueStr + "' out of range for Byte");
                        }
                    } else if (columnType == Short.class) {
                        long parsedLong = Long.parseLong(valueStr);
                        if (parsedLong >= Short.MIN_VALUE && parsedLong <= Short.MAX_VALUE) {
                            return (short) parsedLong;
                        } else {
                            throw new IllegalArgumentException("Numeric value '" + valueStr + "' out of range for Short");
                        }
                    } else if (columnType == Integer.class) {
                        return Integer.parseInt(valueStr);
                    } else if (columnType == Long.class) {
                        return Long.parseLong(valueStr);
                    } else {
                        throw new IllegalArgumentException("Numeric value '" + valueStr + "' does not match column type: " + columnType.getSimpleName());
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid numeric value: " + valueStr);
                }
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value: " + valueStr);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid value format: " + valueStr + " for column type: " + columnType.getSimpleName());
        }
    }
}