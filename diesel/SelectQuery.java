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
    private final Integer limit; // Поле для LIMIT

    public SelectQuery(List<String> columns, List<QueryParser.Condition> conditions, List<QueryParser.JoinInfo> joins, String mainTableName, Integer limit) {
        this.columns = columns;
        this.conditions = conditions != null ? conditions : new ArrayList<>();
        this.joins = joins != null ? joins : new ArrayList<>();
        this.mainTableName = mainTableName;
        this.limit = limit;
    }

    @Override
    public List<Map<String, Object>> execute(Table table) {
        Database database = table.getDatabase();
        List<Map<String, Object>> result = new ArrayList<>();
        List<ReentrantReadWriteLock> acquiredLocks = new ArrayList<>();
        Map<String, Table> tables = new HashMap<>();
        tables.put(mainTableName, table);

        Map<String, Class<?>> combinedColumnTypes = new HashMap<>(table.getColumnTypes());
        for (QueryParser.JoinInfo join : joins) {
            Table joinTable = database.getTable(join.tableName);
            if (joinTable == null) {
                throw new IllegalArgumentException("Join table not found: " + join.tableName);
            }
            tables.put(join.tableName, joinTable);
            combinedColumnTypes.putAll(joinTable.getColumnTypes());
        }

        try {
            List<Map<String, Object>> mainRows = getIndexedRows(table, conditions, mainTableName, combinedColumnTypes);
            if (mainRows == null) {
                mainRows = table.getRows();
            }

            List<Map<String, Map<String, Object>>> joinedRows = new ArrayList<>();
            for (Map<String, Object> mainRow : mainRows) {
                joinedRows.add(new HashMap<>() {{ put(mainTableName, mainRow); }});
            }

            for (QueryParser.JoinInfo join : joins) {
                Table joinTable = tables.get(join.tableName);
                List<Map<String, Map<String, Object>>> newJoinedRows = new ArrayList<>();

                boolean useHashJoin = canUseHashJoin(join, combinedColumnTypes);
                LOGGER.log(Level.FINE, "Join on {0}: useHashJoin={1}", new Object[]{join.tableName, useHashJoin});

                if (useHashJoin) {
                    Table buildTable = joinTable.getRows().size() <= mainRows.size() ? joinTable : tables.get(mainTableName);
                    Table probeTable = buildTable == joinTable ? tables.get(mainTableName) : joinTable;
                    String buildTableName = buildTable == joinTable ? join.tableName : mainTableName;
                    String probeTableName = probeTable == joinTable ? join.tableName : mainTableName;

                    QueryParser.Condition equalityCondition = join.onConditions.stream()
                            .filter(c -> c.operator == QueryParser.Operator.EQUALS && c.isColumnComparison())
                            .findFirst()
                            .orElse(null);
                    if (equalityCondition == null) {
                        throw new IllegalStateException("No equality condition for hash join");
                    }
                    String buildColumn = equalityCondition.rightColumn.contains(buildTableName) ? equalityCondition.rightColumn : equalityCondition.column;
                    String probeColumn = equalityCondition.rightColumn.contains(probeTableName) ? equalityCondition.rightColumn : equalityCondition.column;

                    Map<Object, List<Map<String, Object>>> hashTable = new HashMap<>();
                    List<Map<String, Object>> buildRows = getIndexedRows(buildTable, join.onConditions, buildTableName, combinedColumnTypes);
                    if (buildRows == null) {
                        buildRows = buildTable.getRows();
                    }
                    for (int i = 0; i < buildRows.size(); i++) {
                        Map<String, Object> row = buildRows.get(i);
                        Object key = row.get(buildColumn.split("\\.")[1]);
                        if (key != null) {
                            hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                            ReentrantReadWriteLock lock = buildTable.getRowLock(i);
                            lock.readLock().lock();
                            acquiredLocks.add(lock);
                        }
                    }

                    for (Map<String, Map<String, Object>> currentJoin : joinedRows) {
                        Map<String, Object> probeRow = currentJoin.get(probeTableName);
                        Object probeKey = probeRow.get(probeColumn.split("\\.")[1]);
                        if (probeKey != null) {
                            List<Map<String, Object>> matches = hashTable.get(probeKey);
                            if (matches != null) {
                                for (Map<String, Object> buildRow : matches) {
                                    Map<String, Map<String, Object>> newRow = new HashMap<>(currentJoin);
                                    newRow.put(buildTableName, buildRow);
                                    Map<String, Object> flattenedRow = flattenJoinedRow(newRow);
                                    if (evaluateConditions(flattenedRow, join.onConditions, combinedColumnTypes)) {
                                        newJoinedRows.add(newRow);
                                    }
                                }
                            }
                        }
                    }

                    LOGGER.log(Level.FINE, "Hash join completed: {0} rows produced for join on {1}",
                            new Object[]{newJoinedRows.size(), join.tableName});
                } else {
                    List<Map<String, Object>> joinRows = getIndexedRows(joinTable, join.onConditions, join.tableName, combinedColumnTypes);
                    if (joinRows == null) {
                        joinRows = joinTable.getRows();
                    }

                    for (Map<String, Map<String, Object>> currentJoin : joinedRows) {
                        for (int j = 0; j < joinRows.size(); j++) {
                            Map<String, Object> rightRow = joinRows.get(j);
                            Map<String, Map<String, Object>> newRow = new HashMap<>(currentJoin);
                            newRow.put(join.tableName, rightRow);

                            Map<String, Object> flattenedRow = flattenJoinedRow(newRow);
                            if (join.joinType == QueryParser.JoinType.CROSS) {
                                newJoinedRows.add(newRow);
                            } else if (join.onConditions.isEmpty() && join.leftColumn != null && join.rightColumn != null) {
                                Map<String, Object> leftRow = currentJoin.get(join.originalTable);
                                Object leftValue = leftRow.get(join.leftColumn);
                                Object rightValue = rightRow.get(join.rightColumn);
                                if (!valuesEqual(leftValue, rightValue)) {
                                    continue;
                                }
                                newJoinedRows.add(newRow);
                            } else if (!join.onConditions.isEmpty()) {
                                if (!evaluateConditions(flattenedRow, join.onConditions, combinedColumnTypes)) {
                                    continue;
                                }
                                newJoinedRows.add(newRow);
                                LOGGER.log(Level.FINE, "JOIN ON condition satisfied for {0} with conditions: {1}",
                                        new Object[]{join.tableName, join.onConditions});
                            } else {
                                throw new IllegalStateException("No valid ON condition specified for non-CROSS JOIN");
                            }

                            ReentrantReadWriteLock joinLock = joinTable.getRowLock(j);
                            joinLock.readLock().lock();
                            acquiredLocks.add(joinLock);
                        }
                    }
                }
                joinedRows = newJoinedRows;
            }

            for (Map<String, Map<String, Object>> joinedRow : joinedRows) {
                Map<String, Object> flattenedRow = flattenJoinedRow(joinedRow);
                if (conditions.isEmpty() || evaluateConditions(flattenedRow, conditions, combinedColumnTypes)) {
                    result.add(filterColumns(flattenedRow, columns));
                    if (limit != null && result.size() >= limit) {
                        break;
                    }
                }
            }

            LOGGER.log(Level.INFO, "Selected {0} rows from table {1} with joins {2}, limit={3}",
                    new Object[]{result.size(), mainTableName, joins, limit});
            return result;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.readLock().unlock();
            }
        }
    }

    private boolean canUseHashJoin(QueryParser.JoinInfo join, Map<String, Class<?>> combinedColumnTypes) {
        if (join.joinType != QueryParser.JoinType.INNER &&
                join.joinType != QueryParser.JoinType.LEFT_INNER &&
                join.joinType != QueryParser.JoinType.RIGHT_INNER) {
            return false;
        }
        return join.onConditions.stream()
                .anyMatch(c -> c.operator == QueryParser.Operator.EQUALS && c.isColumnComparison());
    }

    private List<Map<String, Object>> getIndexedRows(Table table, List<QueryParser.Condition> conditions, String tableName, Map<String, Class<?>> combinedColumnTypes) {
        if (conditions == null || conditions.isEmpty()) {
            return null;
        }

        for (QueryParser.Condition condition : conditions) {
            if (condition.isGrouped() || condition.isColumnComparison()) {
                continue;
            }

            String columnName = condition.column.contains(".") ? condition.column.split("\\.")[1] : condition.column;
            Index index = table.getIndex(columnName);
            if (index == null && table.hasClusteredIndex() && columnName.equals(table.getClusteredIndexColumn())) {
                index = table.getClusteredIndex();
            }

            if (index != null) {
                List<Integer> rowIndices = new ArrayList<>();
                if (condition.operator == QueryParser.Operator.EQUALS && condition.value != null) {
                    rowIndices.addAll(index.search(condition.value));
                    LOGGER.log(Level.FINE, "Used index on {0}.{1} for EQUALS condition, found {2} rows",
                            new Object[]{tableName, columnName, rowIndices.size()});
                } else if (condition.isInOperator() && condition.inValues != null) {
                    for (Object inValue : condition.inValues) {
                        rowIndices.addAll(index.search(inValue));
                    }
                    LOGGER.log(Level.FINE, "Used index on {0}.{1} for IN condition, found {2} rows",
                            new Object[]{tableName, columnName, rowIndices.size()});
                } else if (index instanceof BTreeIndex && (condition.operator == QueryParser.Operator.LESS_THAN || condition.operator == QueryParser.Operator.GREATER_THAN)) {
                    BTreeIndex bTreeIndex = (BTreeIndex) index;
                    Object low = condition.operator == QueryParser.Operator.GREATER_THAN ? condition.value : null;
                    Object high = condition.operator == QueryParser.Operator.LESS_THAN ? condition.value : null;
                    rowIndices.addAll(bTreeIndex.rangeSearch(low, high));
                    LOGGER.log(Level.FINE, "Used BTree index on {0}.{1} for range condition {2}, found {3} rows",
                            new Object[]{tableName, columnName, condition.operator, rowIndices.size()});
                }

                if (!rowIndices.isEmpty()) {
                    List<Map<String, Object>> indexedRows = new ArrayList<>();
                    for (int idx : rowIndices) {
                        if (idx >= 0 && idx < table.getRows().size()) {
                            indexedRows.add(table.getRows().get(idx));
                        }
                    }
                    return indexedRows;
                }
            }
        }

        return null;
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
            LOGGER.log(Level.FINEST, "Evaluated condition: {0}, result: {1}, conjunction: {2}, cumulative result: {3}",
                    new Object[]{condition, conditionResult, lastConjunction, result});
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
            boolean inResult = false;
            for (Object inValue : condition.inValues) {
                Class<?> columnType = columnTypes.get(condition.column.split("\\.")[1]);
                if (columnType == null) {
                    throw new IllegalArgumentException("Unknown column type for: " + condition.column);
                }
                int comparison = compareValues(rowValue, inValue, columnType);
                if (comparison == 0) {
                    inResult = true;
                    break;
                }
            }
            boolean result = condition.not ? !inResult : inResult;
            LOGGER.log(Level.FINE, "Evaluated IN condition: {0}, rowValue={1}, inValues={2}, result={3}",
                    new Object[]{condition, rowValue, condition.inValues, result});
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

        if (condition.isNullOperator()) {
            Object rowValue = row.get(condition.column);
            boolean result;
            if (condition.operator == QueryParser.Operator.IS_NULL) {
                result = rowValue == null;
            } else {
                result = rowValue != null;
            }
            result = condition.not ? !result : result;
            LOGGER.log(Level.FINE, "Evaluated null condition: {0}, rowValue={1}, result={2}",
                    new Object[]{condition, rowValue, result});
            return result;
        }

        Object rowValue = row.get(condition.column);
        if (rowValue == null) {
            LOGGER.log(Level.WARNING, "Row value for column {0} is null", condition.column);
            return false;
        }

        Class<?> columnType = columnTypes.get(condition.column.split("\\.")[1]);
        if (columnType == null) {
            throw new IllegalArgumentException("Unknown column type for: " + condition.column);
        }

        if (condition.operator == QueryParser.Operator.LIKE || condition.operator == QueryParser.Operator.NOT_LIKE) {
            String rowStr = rowValue.toString();
            String pattern = QueryParser.convertLikePatternToRegex((String) condition.value);
            boolean matches = Pattern.matches(pattern, rowStr);
            boolean result = condition.operator == QueryParser.Operator.LIKE ? matches : !matches;
            result = condition.not ? !result : result;
            LOGGER.log(Level.FINE, "Evaluated LIKE condition: {0}, rowValue={1}, pattern={2}, result={3}",
                    new Object[]{condition, rowValue, pattern, result});
            return result;
        }

        int comparison = compareValues(rowValue, condition.value, columnType);
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
                new Object[]{condition, rowValue, condition.value, result});
        return result;
    }

    private int compareValues(Object left, Object right, Class<?> columnType) {
        if (left == null || right == null) {
            return left == right ? 0 : (left == null ? -1 : 1);
        }

        if (columnType == String.class || columnType == UUID.class || columnType == Character.class) {
            return String.valueOf(left).compareTo(String.valueOf(right));
        } else if (columnType == Integer.class) {
            Integer leftInt = (Integer) left;
            Integer rightInt = (Integer) right;
            return leftInt.compareTo(rightInt);
        } else if (columnType == Long.class) {
            Long leftLong = (Long) left;
            Long rightLong = (Long) right;
            return leftLong.compareTo(rightLong);
        } else if (columnType == Short.class) {
            Short leftShort = (Short) left;
            Short rightShort = (Short) right;
            return leftShort.compareTo(rightShort);
        } else if (columnType == Byte.class) {
            Byte leftByte = (Byte) left;
            Byte rightByte = (Byte) right;
            return leftByte.compareTo(rightByte);
        } else if (columnType == Float.class) {
            Float leftFloat = (Float) left;
            Float rightFloat = (Float) right;
            return Float.compare(leftFloat, rightFloat);
        } else if (columnType == Double.class) {
            Double leftDouble = (Double) left;
            Double rightDouble = (Double) right;
            return Double.compare(leftDouble, rightDouble);
        } else if (columnType == BigDecimal.class) {
            BigDecimal leftBD = (BigDecimal) left;
            BigDecimal rightBD = (BigDecimal) right;
            return leftBD.compareTo(rightBD);
        } else if (columnType == Boolean.class) {
            Boolean leftBool = (Boolean) left;
            Boolean rightBool = (Boolean) right;
            return leftBool.compareTo(rightBool);
        } else if (columnType == LocalDate.class) {
            LocalDate leftDate = (LocalDate) left;
            LocalDate rightDate = (LocalDate) right;
            return leftDate.compareTo(rightDate);
        } else if (columnType == LocalDateTime.class) {
            LocalDateTime leftDateTime = (LocalDateTime) left;
            LocalDateTime rightDateTime = (LocalDateTime) right;
            return leftDateTime.compareTo(rightDateTime);
        } else {
            throw new IllegalArgumentException("Unsupported column type for comparison: " + columnType.getSimpleName());
        }
    }

    private Map<String, Object> filterColumns(Map<String, Object> row, List<String> columns) {
        Map<String, Object> filtered = new HashMap<>();
        for (String column : columns) {
            String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1] : column;
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String entryColumn = entry.getKey().contains(".") ? entry.getKey().split("\\.")[1] : entry.getKey();
                if (entryColumn.equalsIgnoreCase(unqualifiedColumn)) {
                    filtered.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return filtered;
    }
}