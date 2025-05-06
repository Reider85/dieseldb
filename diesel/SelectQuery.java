package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.regex.Pattern;

class SelectQuery implements Query<List<Map<String, Object>>> {
    private static final Logger LOGGER = Logger.getLogger(SelectQuery.class.getName());
    private final List<String> columns;
    private final List<QueryParser.Condition> conditions;
    private final List<QueryParser.JoinInfo> joins;
    private final String mainTableName;

    public SelectQuery(List<String> columns, List<QueryParser.Condition> conditions, List<QueryParser.JoinInfo> joins, String mainTableName) {
        this.columns = columns;
        this.conditions = conditions != null ? conditions : new ArrayList<>();
        this.joins = joins != null ? joins : new ArrayList<>();
        this.mainTableName = mainTableName;
    }

    @Override
    public List<Map<String, Object>> execute(Table table) {
        Database database = table.getDatabase();
        List<Map<String, Object>> result = new ArrayList<>();
        List<ReentrantReadWriteLock> acquiredLocks = new ArrayList<>();
        Map<String, Table> tables = new HashMap<>();
        tables.put(mainTableName, table);

        // Load all tables for JOIN
        for (QueryParser.JoinInfo join : joins) {
            Table joinTable = database.getTable(join.tableName);
            if (joinTable == null) {
                throw new IllegalArgumentException("Join table not found: " + join.tableName);
            }
            tables.put(join.tableName, joinTable);
        }

        // Get all rows from main table
        List<Map<String, Object>> mainRows = table.getRows();
        Map<String, Class<?>> combinedColumnTypes = new HashMap<>(table.getColumnTypes());

        // Collect all column types
        for (QueryParser.JoinInfo join : joins) {
            Table joinTable = tables.get(join.tableName);
            combinedColumnTypes.putAll(joinTable.getColumnTypes());
        }

        try {
            // For each row in main table
            for (int i = 0; i < mainRows.size(); i++) {
                Map<String, Object> mainRow = mainRows.get(i);
                ReentrantReadWriteLock mainLock = table.getRowLock(i);
                mainLock.readLock().lock();
                acquiredLocks.add(mainLock);

                List<Map<String, Map<String, Object>>> joinedRows = new ArrayList<>();
                joinedRows.add(new HashMap<>() {{ put(mainTableName, mainRow); }});

                // Process JOINs
                for (QueryParser.JoinInfo join : joins) {
                    Table joinTable = tables.get(join.tableName);
                    List<Map<String, Map<String, Object>>> newJoinedRows = new ArrayList<>();
                    List<Map<String, Object>> joinRows = joinTable.getRows();

                    for (Map<String, Map<String, Object>> currentJoin : joinedRows) {
                        for (int j = 0; j < joinRows.size(); j++) {
                            Map<String, Object> rightRow = joinRows.get(j);
                            Map<String, Map<String, Object>> newRow = new HashMap<>(currentJoin);
                            newRow.put(join.tableName, rightRow);

                            // Check JOIN conditions
                            Map<String, Object> flattenedRow = flattenJoinedRow(newRow);
                            if (join.joinType == QueryParser.JoinType.CROSS) {
                                // CROSS JOIN does not require ON conditions
                                newJoinedRows.add(newRow);
                            } else if (join.onConditions.isEmpty() && join.leftColumn != null && join.rightColumn != null) {
                                // Backward compatibility for old format
                                Map<String, Object> leftRow = currentJoin.get(join.originalTable);
                                Object leftValue = leftRow.get(join.leftColumn);
                                Object rightValue = rightRow.get(join.rightColumn);
                                if (!valuesEqual(leftValue, rightValue)) {
                                    continue;
                                }
                                newJoinedRows.add(newRow);
                            } else if (!join.onConditions.isEmpty()) {
                                // New behavior: evaluate ON conditions
                                if (!evaluateConditions(flattenedRow, join.onConditions, combinedColumnTypes)) {
                                    continue;
                                }
                                newJoinedRows.add(newRow);
                            } else {
                                throw new IllegalStateException("No valid ON condition specified for non-CROSS JOIN");
                            }

                            // Lock row from joined table
                            ReentrantReadWriteLock joinLock = joinTable.getRowLock(j);
                            joinLock.readLock().lock();
                            acquiredLocks.add(joinLock);
                        }
                    }
                    joinedRows = newJoinedRows;
                }

                // Apply WHERE and select columns
                for (Map<String, Map<String, Object>> joinedRow : joinedRows) {
                    Map<String, Object> flattenedRow = flattenJoinedRow(joinedRow);
                    if (conditions.isEmpty() || evaluateConditions(flattenedRow, conditions, combinedColumnTypes)) {
                        result.add(filterColumns(flattenedRow, columns));
                    }
                }
            }

            LOGGER.log(Level.INFO, "Selected {0} rows from table {1} with joins {2}",
                    new Object[]{result.size(), mainTableName, joins});
            return result;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.readLock().unlock();
            }
        }
    }

    private boolean valuesEqual(Object left, Object right) {
        if (left == null || right == null) {
            return false;
        }
        if (left instanceof Float && right instanceof Float) {
            return Math.abs(((Float) left) - ((Float) right)) < 1e-7;
        } else if (left instanceof Double && right instanceof Double) {
            return Math.abs(((Double) left) - ((Double) right)) < 1e-7;
        } else if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return ((BigDecimal) left).compareTo((BigDecimal) right) == 0;
        }
        return String.valueOf(left).equals(String.valueOf(right));
    }

    private Map<String, Object> flattenJoinedRow(Map<String, Map<String, Object>> joinedRow) {
        Map<String, Object> flattened = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : joinedRow.entrySet()) {
            String tableName = entry.getKey();
            for (Map.Entry<String, Object> column : entry.getValue().entrySet()) {
                flattened.put(tableName + "." + column.getKey(), column.getValue());
            }
        }
        return flattened;
    }

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> columnTypes) {
        boolean result = true;
        String lastConjunction = null;

        for (QueryParser.Condition condition : conditions) {
            boolean conditionResult = evaluateCondition(row, condition, columnTypes);

            if (lastConjunction == null) {
                result = conditionResult;
            } else if (lastConjunction.equalsIgnoreCase("AND")) {
                result = result && conditionResult;
            } else if (lastConjunction.equalsIgnoreCase("OR")) {
                result = result || conditionResult;
            }

            lastConjunction = condition.conjunction;
        }

        return result;
    }

    private boolean evaluateCondition(Map<String, Object> row, QueryParser.Condition condition, Map<String, Class<?>> columnTypes) {
        if (condition.isGrouped()) {
            boolean subResult = evaluateConditions(row, condition.subConditions, columnTypes);
            boolean result = condition.not ? !subResult : subResult;
            LOGGER.log(Level.FINE, "Evaluated grouped condition: {0}, result: {1}", new Object[]{condition, result});
            return result;
        }

        if (condition.isInOperator()) {
            Object rowValue = row.get(condition.column);
            if (rowValue == null) {
                LOGGER.log(Level.WARNING, "Row value for column {0} is null", condition.column);
                return condition.not;
            }
            boolean inResult = condition.inValues.contains(rowValue);
            boolean result = condition.not ? !inResult : inResult;
            LOGGER.log(Level.FINE, "Evaluated IN condition: {0}, rowValue={1}, result={2}",
                    new Object[]{condition, rowValue, result});
            return result;
        }

        if (condition.isColumnComparison()) {
            Object leftValue = row.get(condition.column);
            Object rightValue = row.get(condition.rightColumn);
            if (leftValue == null || rightValue == null) {
                LOGGER.log(Level.WARNING, "Row value for column {0} or {1} is null", new Object[]{condition.column, condition.rightColumn});
                return false;
            }

            Class<?> columnType = columnTypes.get(condition.column.split("\\.")[1]);
            if (columnType == null) {
                throw new IllegalArgumentException("Unknown column type for: " + condition.column);
            }

            int comparison = compareValues(leftValue, rightValue, columnType);
            boolean result;

            switch (condition.operator) {
                case EQUALS:
                    result = comparison == 0;
                    break;
                case NOT_EQUALS:
                    result = comparison != 0;
                    break;
                case LESS_THAN:
                    result = comparison < 0;
                    break;
                case GREATER_THAN:
                    result = comparison > 0;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported operator for column comparison: " + condition.operator);
            }

            result = condition.not ? !result : result;
            LOGGER.log(Level.FINE, "Evaluated column comparison condition: {0}, leftValue={1}, rightValue={2}, result={3}",
                    new Object[]{condition, leftValue, rightValue, result});
            return result;
        }

        Object rowValue = row.get(condition.column);
        if (rowValue == null) {
            LOGGER.log(Level.WARNING, "Row value for column {0} is null", condition.column);
            return false;
        }

        Object conditionValue = condition.value;
        Class<?> columnType = columnTypes.get(condition.column.split("\\.")[1]);
        if (columnType == null) {
            throw new IllegalArgumentException("Unknown column type for: " + condition.column);
        }

        if (condition.operator == QueryParser.Operator.LIKE || condition.operator == QueryParser.Operator.NOT_LIKE) {
            if (!(rowValue instanceof String) || !(conditionValue instanceof String)) {
                throw new IllegalArgumentException("LIKE/NOT LIKE requires String values");
            }
            String regex = QueryParser.convertLikePatternToRegex((String) conditionValue);
            boolean matches = Pattern.matches(regex, (String) rowValue);
            boolean result = condition.operator == QueryParser.Operator.LIKE ? matches : !matches;
            result = condition.not ? !result : result;
            LOGGER.log(Level.FINE, "Evaluated LIKE condition: {0}, rowValue={1}, regex={2}, result={3}",
                    new Object[]{condition, rowValue, regex, result});
            return result;
        }

        int comparison = compareValues(rowValue, conditionValue, columnType);
        boolean result;

        switch (condition.operator) {
            case EQUALS:
                result = comparison == 0;
                break;
            case NOT_EQUALS:
                result = comparison != 0;
                break;
            case LESS_THAN:
                result = comparison < 0;
                break;
            case GREATER_THAN:
                result = comparison > 0;
                break;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + condition.operator);
        }

        result = condition.not ? !result : result;
        LOGGER.log(Level.FINE, "Evaluated condition: {0}, rowValue={1}, conditionValue={2}, result={3}",
                new Object[]{condition, rowValue, conditionValue, result});
        return result;
    }

    private int compareValues(Object rowValue, Object conditionValue, Class<?> columnType) {
        if (rowValue == null || conditionValue == null) {
            return rowValue == conditionValue ? 0 : (rowValue == null ? -1 : 1);
        }

        if (columnType == String.class || columnType == Character.class || columnType == UUID.class) {
            String rowStr = rowValue.toString();
            String condStr = conditionValue.toString();
            return rowStr.compareTo(condStr);
        } else if (columnType == Integer.class || columnType == Long.class || columnType == Short.class || columnType == Byte.class) {
            long rowNum = ((Number) rowValue).longValue();
            long condNum = ((Number) conditionValue).longValue();
            return Long.compare(rowNum, condNum);
        } else if (columnType == Float.class) {
            float rowNum = ((Number) rowValue).floatValue();
            float condNum = ((Number) conditionValue).floatValue();
            return Float.compare(rowNum, condNum);
        } else if (columnType == Double.class) {
            double rowNum = ((Number) rowValue).doubleValue();
            double condNum = ((Number) conditionValue).doubleValue();
            return Double.compare(rowNum, condNum);
        } else if (columnType == BigDecimal.class) {
            BigDecimal rowNum = (BigDecimal) rowValue;
            BigDecimal condNum = (BigDecimal) conditionValue;
            return rowNum.compareTo(condNum);
        } else if (columnType == Boolean.class) {
            boolean rowBool = (Boolean) rowValue;
            boolean condBool = (Boolean) conditionValue;
            return Boolean.compare(rowBool, condBool);
        } else if (columnType == LocalDate.class) {
            LocalDate rowDate = (LocalDate) rowValue;
            LocalDate condDate = (LocalDate) conditionValue;
            return rowDate.compareTo(condDate);
        } else if (columnType == LocalDateTime.class) {
            LocalDateTime rowDateTime = (LocalDateTime) rowValue;
            LocalDateTime condDateTime = (LocalDateTime) conditionValue;
            return rowDateTime.compareTo(condDateTime);
        } else {
            throw new IllegalArgumentException("Unsupported column type for comparison: " + columnType.getSimpleName());
        }
    }

    private Map<String, Object> filterColumns(Map<String, Object> row, List<String> columns) {
        Map<String, Object> filtered = new LinkedHashMap<>();
        for (String column : columns) {
            String unqualifiedColumn = column.contains(".") ? column : mainTableName + "." + column;
            filtered.put(column, row.get(unqualifiedColumn));
        }
        return filtered;
    }
}