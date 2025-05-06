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
    private final Integer offset; // Поле для OFFSET

    public SelectQuery(List<String> columns, List<QueryParser.Condition> conditions, List<QueryParser.JoinInfo> joins,
                       String mainTableName, Integer limit, Integer offset) {
        this.columns = columns;
        this.conditions = conditions != null ? conditions : new ArrayList<>();
        this.joins = joins != null ? joins : new ArrayList<>();
        this.mainTableName = mainTableName;
        this.limit = limit;
        this.offset = offset;
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

            // Apply WHERE conditions and collect results, respecting LIMIT and OFFSET
            int rowsAdded = 0;
            int rowsSkipped = (offset != null) ? offset : 0;
            for (Map<String, Map<String, Object>> joinedRow : joinedRows) {
                Map<String, Object> flattenedRow = flattenJoinedRow(joinedRow);
                if (conditions.isEmpty() || evaluateConditions(flattenedRow, conditions, combinedColumnTypes)) {
                    if (rowsSkipped > 0) {
                        rowsSkipped--;
                        continue; // Skip rows until offset is satisfied
                    }
                    result.add(filterColumns(flattenedRow, columns));
                    rowsAdded++;
                    if (limit != null && rowsAdded >= limit) {
                        break; // Stop once limit is reached
                    }
                }
            }

            LOGGER.log(Level.INFO, "Selected {0} rows from table {1} with joins {2}, limit={3}, offset={4}",
                    new Object[]{result.size(), mainTableName, joins, limit, offset});
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
        for (Map.Entry<String, Map<String, Object>> tableEntry : joinedRow.entrySet()) {
            String tableName = tableEntry.getKey();
            Map<String, Object> row = tableEntry.getValue();
            for (Map.Entry<String, Object> columnEntry : row.entrySet()) {
                String columnName = tableName + "." + columnEntry.getKey();
                flattened.put(columnName, columnEntry.getValue());
            }
        }
        return flattened;
    }

    private boolean evaluateConditions(Map<String, Object> row, List<QueryParser.Condition> conditions, Map<String, Class<?>> combinedColumnTypes) {
        boolean result = true;
        String lastConjunction = null;

        for (QueryParser.Condition condition : conditions) {
            boolean conditionResult = evaluateCondition(row, condition, combinedColumnTypes);
            if (lastConjunction == null) {
                result = conditionResult;
            } else if (lastConjunction.equals("AND")) {
                result = result && conditionResult;
            } else if (lastConjunction.equals("OR")) {
                result = result || conditionResult;
            }
            lastConjunction = condition.conjunction;
        }

        return result;
    }

    private boolean evaluateCondition(Map<String, Object> row, QueryParser.Condition condition, Map<String, Class<?>> combinedColumnTypes) {
        if (condition.isGrouped()) {
            boolean subResult = evaluateConditions(row, condition.subConditions, combinedColumnTypes);
            return condition.not ? !subResult : subResult;
        }

        if (condition.isNullOperator()) {
            Object value = row.get(condition.column);
            boolean isNull = value == null;
            boolean result = condition.operator == QueryParser.Operator.IS_NULL ? isNull : !isNull;
            return condition.not ? !result : result;
        }

        if (condition.isInOperator()) {
            Object value = row.get(condition.column);
            if (value == null) {
                return condition.not;
            }
            boolean inResult = condition.inValues.stream().anyMatch(v -> valuesEqual(v, value));
            return condition.not ? !inResult : inResult;
        }

        Object leftValue = row.get(condition.column);
        if (leftValue == null) {
            return condition.not;
        }

        Object rightValue;
        if (condition.isColumnComparison()) {
            rightValue = row.get(condition.rightColumn);
            if (rightValue == null) {
                return condition.not;
            }
        } else {
            rightValue = condition.value;
        }

        int comparison;
        if (condition.operator == QueryParser.Operator.LIKE || condition.operator == QueryParser.Operator.NOT_LIKE) {
            if (!(leftValue instanceof String) || !(rightValue instanceof String)) {
                return condition.not;
            }
            String pattern = QueryParser.convertLikePatternToRegex((String) rightValue);
            boolean matches = Pattern.matches(pattern, (String) leftValue);
            boolean result = condition.operator == QueryParser.Operator.LIKE ? matches : !matches;
            return condition.not ? !result : result;
        }

        comparison = compareValues(leftValue, rightValue);
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
                throw new IllegalStateException("Unsupported operator: " + condition.operator);
        }

        return condition.not ? !result : result;
    }

    private int compareValues(Object left, Object right) {
        if (left == null || right == null) {
            return left == right ? 0 : (left == null ? -1 : 1);
        }

        if (left instanceof Number && right instanceof Number) {
            if (left instanceof BigDecimal && right instanceof BigDecimal) {
                return ((BigDecimal) left).compareTo((BigDecimal) right);
            }
            BigDecimal leftBD = new BigDecimal(left.toString());
            BigDecimal rightBD = new BigDecimal(right.toString());
            return leftBD.compareTo(rightBD);
        } else if (left instanceof LocalDate && right instanceof LocalDate) {
            return ((LocalDate) left).compareTo((LocalDate) right);
        } else if (left instanceof LocalDateTime && right instanceof LocalDateTime) {
            return ((LocalDateTime) left).compareTo((LocalDateTime) right);
        } else if (left instanceof Boolean && right instanceof Boolean) {
            return ((Boolean) left).compareTo((Boolean) right);
        } else if (left instanceof UUID && right instanceof UUID) {
            return ((UUID) left).compareTo((UUID) right);
        } else if (left instanceof String && right instanceof String) {
            return ((String) left).compareTo((String) right);
        } else if (left instanceof Character && right instanceof Character) {
            return ((Character) left).compareTo((Character) right);
        } else {
            throw new IllegalArgumentException("Incompatible types for comparison: " + left.getClass() + " and " + right.getClass());
        }
    }

    private Map<String, Object> filterColumns(Map<String, Object> row, List<String> columns) {
        Map<String, Object> filtered = new HashMap<>();
        for (String column : columns) {
            String unqualifiedColumn = column.contains(".") ? column : mainTableName + "." + column;
            if (row.containsKey(unqualifiedColumn)) {
                filtered.put(unqualifiedColumn, row.get(unqualifiedColumn));
            } else {
                String colName = column.contains(".") ? column.split("\\.")[1] : column;
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    if (entry.getKey().endsWith("." + colName)) {
                        filtered.put(entry.getKey(), entry.getValue());
                        break;
                    }
                }
            }
        }
        return filtered;
    }
}
