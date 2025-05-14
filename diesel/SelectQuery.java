package diesel;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class SelectQuery implements Query<List<Map<String, Object>>> {
    private static final Logger LOGGER = Logger.getLogger(SelectQuery.class.getName());
    private final List<String> columns;
    private final List<QueryParser.AggregateFunction> aggregates;
    private final List<QueryParser.Condition> conditions;
    private final List<QueryParser.JoinInfo> joins;
    private final String mainTableName;
    private final Integer limit;
    private final Integer offset;
    private final List<QueryParser.OrderByInfo> orderBy;
    private final List<String> groupBy;
    private final List<QueryParser.HavingCondition> havingConditions;
    private final Map<String, String> tableAliases; // Stores alias -> tableName mappings

    public SelectQuery(List<String> columns, List<QueryParser.AggregateFunction> aggregates, List<QueryParser.Condition> conditions,
                       List<QueryParser.JoinInfo> joins, String mainTableName, Integer limit, Integer offset,
                       List<QueryParser.OrderByInfo> orderBy, List<String> groupBy, List<QueryParser.HavingCondition> havingConditions,
                       Map<String, String> tableAliases) {
        this.columns = columns != null ? new ArrayList<>(columns) : new ArrayList<>();
        this.aggregates = aggregates != null ? new ArrayList<>(aggregates) : new ArrayList<>();
        this.conditions = conditions != null ? new ArrayList<>(conditions) : new ArrayList<>();
        this.joins = joins != null ? new ArrayList<>(joins) : new ArrayList<>();
        this.mainTableName = mainTableName;
        this.limit = limit;
        this.offset = offset;
        this.orderBy = orderBy != null ? new ArrayList<>(orderBy) : new ArrayList<>();
        this.groupBy = groupBy != null ? new ArrayList<>(groupBy) : new ArrayList<>();
        this.havingConditions = havingConditions != null ? new ArrayList<>(havingConditions) : new ArrayList<>();
        this.tableAliases = tableAliases != null ? new HashMap<>(tableAliases) : new HashMap<>();
        // Ensure main table is in aliases
        this.tableAliases.putIfAbsent(mainTableName, mainTableName);
    }

    @Override
    public List<Map<String, Object>> execute(Table table) {
        Database database = table.getDatabase();
        List<Map<String, Object>> result = new ArrayList<>();
        List<ReentrantReadWriteLock> acquiredLocks = new ArrayList<>();
        Map<String, Table> tables = new HashMap<>();
        tables.put(mainTableName, table);

        Map<String, Class<?>> combinedColumnTypes = new HashMap<>();
        // Initialize column types with main table
        table.getColumnTypes().forEach((col, type) -> combinedColumnTypes.put(mainTableName + "." + col, type));

        for (QueryParser.JoinInfo join : joins) {
            Table joinTable = database.getTable(join.tableName);
            if (joinTable == null) {
                throw new IllegalArgumentException("Join table not found: " + join.tableName);
            }
            tables.put(join.tableName, joinTable);
            // Update column types with table prefix
            joinTable.getColumnTypes().forEach((col, type) -> combinedColumnTypes.put(join.tableName + "." + col, type));
            // Add alias to tableAliases if present
            if (join.alias != null) {
                tableAliases.put(join.alias, join.tableName);
            }
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
                        Object key = row.get(normalizeColumnKey(buildColumn, buildTableName));
                        if (key != null) {
                            hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                            ReentrantReadWriteLock lock = buildTable.getRowLock(i);
                            lock.readLock().lock();
                            acquiredLocks.add(lock);
                        }
                    }

                    for (Map<String, Map<String, Object>> currentJoin : joinedRows) {
                        Map<String, Object> probeRow = currentJoin.get(probeTableName);
                        Object probeKey = probeRow.get(normalizeColumnKey(probeColumn, probeTableName));
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
                                Object leftValue = leftRow.get(normalizeColumnKey(join.leftColumn, join.originalTable));
                                Object rightValue = rightRow.get(normalizeColumnKey(join.rightColumn, join.tableName));
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

            List<Map<String, Object>> filteredRows = new ArrayList<>();
            for (Map<String, Map<String, Object>> joinedRow : joinedRows) {
                Map<String, Object> flattenedRow = flattenJoinedRow(joinedRow);
                if (conditions.isEmpty() || evaluateConditions(flattenedRow, conditions, combinedColumnTypes)) {
                    filteredRows.add(flattenedRow);
                }
            }

            List<Map<String, Object>> finalRows;
            if (!groupBy.isEmpty()) {
                Map<List<Object>, List<Map<String, Object>>> groupedRows = filteredRows.stream()
                        .collect(Collectors.groupingBy(row -> groupBy.stream()
                                .map(col -> row.get(normalizeColumnName(col, mainTableName)))
                                .collect(Collectors.toList())));

                finalRows = new ArrayList<>();
                for (List<Object> groupKey : groupedRows.keySet()) {
                    List<Map<String, Object>> group = groupedRows.get(groupKey);
                    Map<String, Object> resultRow = new HashMap<>();

                    for (int i = 0; i < groupBy.size(); i++) {
                        String column = groupBy.get(i);
                        String columnAlias = normalizeColumnKey(column, mainTableName);
                        resultRow.put(columnAlias, groupKey.get(i));
                    }

                    for (QueryParser.AggregateFunction agg : aggregates) {
                        String resultKey = agg.alias != null ? agg.alias : agg.toString();
                        if (agg.functionName.equals("COUNT")) {
                            long count;
                            if (agg.column == null) {
                                count = group.size();
                            } else {
                                String columnKey = normalizeColumnName(agg.column, mainTableName);
                                count = group.stream()
                                        .filter(row -> row.get(columnKey) != null)
                                        .count();
                            }
                            resultRow.put(resultKey, count);
                        } else if (agg.functionName.equals("MIN")) {
                            if (agg.column == null) {
                                throw new IllegalArgumentException("MIN requires a column argument");
                            }
                            String columnKey = normalizeColumnName(agg.column, mainTableName);
                            Object minValue = group.stream()
                                    .map(row -> row.get(columnKey))
                                    .filter(Objects::nonNull)
                                    .min(this::compareValues)
                                    .orElse(null);
                            resultRow.put(resultKey, minValue);
                        } else if (agg.functionName.equals("MAX")) {
                            if (agg.column == null) {
                                throw new IllegalArgumentException("MAX requires a column argument");
                            }
                            String columnKey = normalizeColumnName(agg.column, mainTableName);
                            Object maxValue = group.stream()
                                    .map(row -> row.get(columnKey))
                                    .filter(Objects::nonNull)
                                    .max(this::compareValues)
                                    .orElse(null);
                            resultRow.put(resultKey, maxValue);
                        } else if (agg.functionName.equals("AVG")) {
                            if (agg.column == null) {
                                throw new IllegalArgumentException("AVG requires a column argument");
                            }
                            String columnKey = normalizeColumnName(agg.column, mainTableName);
                            List<Object> values = group.stream()
                                    .map(row -> row.get(columnKey))
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList());
                            if (values.isEmpty()) {
                                resultRow.put(resultKey, null);
                            } else {
                                BigDecimal sum = BigDecimal.ZERO;
                                long count = 0;
                                for (Object value : values) {
                                    if (value instanceof Number) {
                                        sum = sum.add(new BigDecimal(value.toString()));
                                        count++;
                                    }
                                }
                                if (count > 0) {
                                    BigDecimal avg = sum.divide(BigDecimal.valueOf(count), 10, BigDecimal.ROUND_HALF_UP);
                                    Class<?> columnType = combinedColumnTypes.get(columnKey);
                                    if (columnType == Float.class) {
                                        resultRow.put(resultKey, avg.floatValue());
                                    } else if (columnType == Double.class) {
                                        resultRow.put(resultKey, avg.doubleValue());
                                    } else if (columnType == Integer.class) {
                                        resultRow.put(resultKey, avg.intValue());
                                    } else if (columnType == Long.class) {
                                        resultRow.put(resultKey, avg.longValue());
                                    } else if (columnType == Short.class) {
                                        resultRow.put(resultKey, avg.shortValue());
                                    } else if (columnType == Byte.class) {
                                        resultRow.put(resultKey, avg.byteValue());
                                    } else {
                                        resultRow.put(resultKey, avg);
                                    }
                                } else {
                                    resultRow.put(resultKey, null);
                                }
                            }
                        } else if (agg.functionName.equals("SUM")) {
                            if (agg.column == null) {
                                throw new IllegalArgumentException("SUM requires a column argument");
                            }
                            String columnKey = normalizeColumnName(agg.column, mainTableName);
                            List<Object> values = group.stream()
                                    .map(row -> row.get(columnKey))
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toList());
                            if (values.isEmpty()) {
                                resultRow.put(resultKey, null);
                            } else {
                                BigDecimal sum = BigDecimal.ZERO;
                                for (Object value : values) {
                                    if (value instanceof Number) {
                                        sum = sum.add(new BigDecimal(value.toString()));
                                    }
                                }
                                Class<?> columnType = combinedColumnTypes.get(columnKey);
                                if (columnType == Float.class) {
                                    resultRow.put(resultKey, sum.floatValue());
                                } else if (columnType == Double.class) {
                                    resultRow.put(resultKey, sum.doubleValue());
                                } else if (columnType == Integer.class) {
                                    resultRow.put(resultKey, sum.intValue());
                                } else if (columnType == Long.class) {
                                    resultRow.put(resultKey, sum.longValue());
                                } else if (columnType == Short.class) {
                                    resultRow.put(resultKey, sum.shortValue());
                                } else if (columnType == Byte.class) {
                                    resultRow.put(resultKey, sum.byteValue());
                                } else {
                                    resultRow.put(resultKey, sum);
                                }
                            }
                        } else {
                            throw new UnsupportedOperationException("Aggregate function not supported: " + agg.functionName);
                        }
                    }

                    for (String column : columns) {
                        if (!resultRow.containsKey(normalizeColumnKey(column, mainTableName))) {
                            String normalizedColumn = normalizeColumnName(column, mainTableName);
                            String unqualifiedColumn = normalizeColumnKey(normalizedColumn, mainTableName);
                            Object value = group.get(0).get(normalizedColumn);
                            resultRow.put(unqualifiedColumn, value);
                        }
                    }

                    if (!havingConditions.isEmpty()) {
                        if (!evaluateHavingConditions(resultRow, havingConditions)) {
                            continue;
                        }
                    }

                    finalRows.add(resultRow);
                }
                LOGGER.log(Level.FINE, "Applied GROUP BY with {0} columns, produced {1} groups",
                        new Object[]{groupBy.size(), finalRows.size()});
            } else {
                finalRows = filteredRows;
            }

            if (!orderBy.isEmpty()) {
                finalRows.sort((row1, row2) -> compareRows(row1, row2, orderBy));
                LOGGER.log(Level.FINE, "Applied ORDER BY with {0} clauses", orderBy.size());
            }

            int rowsSkipped = (offset != null) ? offset : 0;
            int maxRows = (limit != null) ? limit : Integer.MAX_VALUE;
            List<Map<String, Object>> selectedRows = new ArrayList<>();
            for (int i = 0; i < finalRows.size() && selectedRows.size() < maxRows; i++) {
                if (rowsSkipped > 0) {
                    rowsSkipped--;
                    continue;
                }
                selectedRows.add(finalRows.get(i));
            }

            if (!aggregates.isEmpty() && groupBy.isEmpty()) {
                Map<String, Object> resultRow = new HashMap<>();
                for (QueryParser.AggregateFunction agg : aggregates) {
                    String resultKey = agg.alias != null ? agg.alias : agg.toString();
                    if (agg.functionName.equals("COUNT")) {
                        long count;
                        if (agg.column == null) {
                            count = selectedRows.size();
                        } else {
                            String columnKey = normalizeColumnName(agg.column, mainTableName);
                            count = selectedRows.stream()
                                    .filter(row -> row.get(columnKey) != null)
                                    .count();
                        }
                        resultRow.put(resultKey, count);
                    } else if (agg.functionName.equals("MIN")) {
                        if (agg.column == null) {
                            throw new IllegalArgumentException("MIN requires a column argument");
                        }
                        String columnKey = normalizeColumnName(agg.column, mainTableName);
                        Object minValue = selectedRows.stream()
                                .map(row -> row.get(columnKey))
                                .filter(Objects::nonNull)
                                .min(this::compareValues)
                                .orElse(null);
                        resultRow.put(resultKey, minValue);
                    } else if (agg.functionName.equals("MAX")) {
                        if (agg.column == null) {
                            throw new IllegalArgumentException("MAX requires a column argument");
                        }
                        String columnKey = normalizeColumnName(agg.column, mainTableName);
                        Object maxValue = selectedRows.stream()
                                .map(row -> row.get(columnKey))
                                .filter(Objects::nonNull)
                                .max(this::compareValues)
                                .orElse(null);
                        resultRow.put(resultKey, maxValue);
                    } else if (agg.functionName.equals("AVG")) {
                        if (agg.column == null) {
                            throw new IllegalArgumentException("AVG requires a column argument");
                        }
                        String columnKey = normalizeColumnName(agg.column, mainTableName);
                        List<Object> values = selectedRows.stream()
                                .map(row -> row.get(columnKey))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                        if (values.isEmpty()) {
                            resultRow.put(resultKey, null);
                        } else {
                            BigDecimal sum = BigDecimal.ZERO;
                            long count = 0;
                            for (Object value : values) {
                                if (value instanceof Number) {
                                    sum = sum.add(new BigDecimal(value.toString()));
                                    count++;
                                }
                            }
                            if (count > 0) {
                                BigDecimal avg = sum.divide(BigDecimal.valueOf(count), 10, BigDecimal.ROUND_HALF_UP);
                                Class<?> columnType = combinedColumnTypes.get(columnKey);
                                if (columnType == Float.class) {
                                    resultRow.put(resultKey, avg.floatValue());
                                } else if (columnType == Double.class) {
                                    resultRow.put(resultKey, avg.doubleValue());
                                } else if (columnType == Integer.class) {
                                    resultRow.put(resultKey, avg.intValue());
                                } else if (columnType == Long.class) {
                                    resultRow.put(resultKey, avg.longValue());
                                } else if (columnType == Short.class) {
                                    resultRow.put(resultKey, avg.shortValue());
                                } else if (columnType == Byte.class) {
                                    resultRow.put(resultKey, avg.byteValue());
                                } else {
                                    resultRow.put(resultKey, avg);
                                }
                            } else {
                                resultRow.put(resultKey, null);
                            }
                        }
                    } else if (agg.functionName.equals("SUM")) {
                        if (agg.column == null) {
                            throw new IllegalArgumentException("SUM requires a column argument");
                        }
                        String columnKey = normalizeColumnName(agg.column, mainTableName);
                        List<Object> values = selectedRows.stream()
                                .map(row -> row.get(columnKey))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());
                        if (values.isEmpty()) {
                            resultRow.put(resultKey, null);
                        } else {
                            BigDecimal sum = BigDecimal.ZERO;
                            for (Object value : values) {
                                if (value instanceof Number) {
                                    sum = sum.add(new BigDecimal(value.toString()));
                                }
                            }
                            Class<?> columnType = combinedColumnTypes.get(columnKey);
                            if (columnType == Float.class) {
                                resultRow.put(resultKey, sum.floatValue());
                            } else if (columnType == Double.class) {
                                resultRow.put(resultKey, sum.doubleValue());
                            } else if (columnType == Integer.class) {
                                resultRow.put(resultKey, sum.intValue());
                            } else if (columnType == Long.class) {
                                resultRow.put(resultKey, sum.longValue());
                            } else if (columnType == Short.class) {
                                resultRow.put(resultKey, sum.shortValue());
                            } else if (columnType == Byte.class) {
                                resultRow.put(resultKey, sum.byteValue());
                            } else {
                                resultRow.put(resultKey, sum);
                            }
                        }
                    } else {
                        throw new UnsupportedOperationException("Aggregate function not supported: " + agg.functionName);
                    }
                }
                result.add(resultRow);
            } else {
                for (Map<String, Object> row : selectedRows) {
                    result.add(filterColumns(row, columns));
                }
            }

            LOGGER.log(Level.INFO, "Selected {0} rows from table {1} with joins {2}, aggregates {3}, groupBy {4}, having={5}, limit={6}, offset={7}, orderBy={8}",
                    new Object[]{result.size(), mainTableName, joins, aggregates, groupBy, havingConditions, limit, offset, orderBy});
            return result;
        } finally {
            for (ReentrantReadWriteLock lock : acquiredLocks) {
                lock.readLock().unlock();
            }
        }
    }

    private boolean evaluateHavingConditions(Map<String, Object> row, List<QueryParser.HavingCondition> havingConditions) {
        boolean result = true;
        String lastConjunction = null;

        for (QueryParser.HavingCondition condition : havingConditions) {
            boolean conditionResult;
            if (condition.isGrouped()) {
                conditionResult = evaluateHavingConditions(row, condition.subConditions);
                conditionResult = condition.not ? !conditionResult : conditionResult;
            } else {
                String key = condition.aggregate.alias != null ? condition.aggregate.alias : condition.aggregate.toString();
                Object actualValue = row.get(key);
                if (actualValue == null) {
                    conditionResult = condition.not;
                } else {
                    int comparison = compareValues(actualValue, condition.value);
                    conditionResult = switch (condition.operator) {
                        case EQUALS -> comparison == 0;
                        case NOT_EQUALS -> comparison != 0;
                        case LESS_THAN -> comparison < 0;
                        case GREATER_THAN -> comparison > 0;
                        case LESS_THAN_OR_EQUALS -> comparison <= 0;
                        case GREATER_THAN_OR_EQUALS -> comparison >= 0;
                        default -> throw new IllegalStateException("Unsupported operator in HAVING: " + condition.operator);
                    };
                    conditionResult = condition.not ? !conditionResult : conditionResult;
                }
            }

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

    private int compareRows(Map<String, Object> row1, Map<String, Object> row2, List<QueryParser.OrderByInfo> orderBy) {
        for (QueryParser.OrderByInfo order : orderBy) {
            String column = order.column;
            String normalizedColumn = null; // Инициализируем null для ясности
            String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;

            // Проверяем, является ли столбец алиасом из SELECT
            for (String selectColumn : columns) {
                String[] parts = selectColumn.trim().split("\\s+AS\\s+|\\s+", 2);
                String columnAlias = parts.length > 1 ? parts[1].trim() : normalizeColumnKey(selectColumn, mainTableName);
                if (unqualifiedColumn.equalsIgnoreCase(columnAlias)) {
                    normalizedColumn = columnAlias; // Используем алиас напрямую
                    break;
                }
            }

            // Если не алиас, проверяем, является ли столбец квалифицированным именем с алиасом таблицы
            if (normalizedColumn == null) {
                for (String alias : tableAliases.keySet()) {
                    if (column.equalsIgnoreCase(alias + "." + unqualifiedColumn)) {
                        String tableName = tableAliases.get(alias);
                        normalizedColumn = tableName + "." + unqualifiedColumn; // Разрешаем в реальное имя столбца
                        break;
                    }
                }
            }

            // Если всё ещё не разрешено, нормализуем как имя столбца
            if (normalizedColumn == null) {
                normalizedColumn = normalizeColumnName(column, mainTableName);
            }

            Object value1 = row1.get(normalizedColumn);
            Object value2 = row2.get(normalizedColumn);

            if (value1 == null && value2 == null) {
                continue;
            }
            if (value1 == null) {
                return order.ascending ? -1 : 1;
            }
            if (value2 == null) {
                return order.ascending ? 1 : -1;
            }

            int comparison = compareValues(value1, value2);
            if (comparison != 0) {
                return order.ascending ? comparison : -comparison;
            }
        }
        return 0;
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

            String columnName = normalizeColumnName(condition.column, tableName);
            String unqualifiedColumn = normalizeColumnKey(columnName, tableName);
            Index index = table.getIndex(unqualifiedColumn);
            if (index == null && table.hasClusteredIndex() && unqualifiedColumn.equals(table.getClusteredIndexColumn())) {
                index = table.getClusteredIndex();
            }

            if (index != null) {
                List<Integer> rowIndices = new ArrayList<>();
                if (condition.operator == QueryParser.Operator.EQUALS && condition.value != null) {
                    rowIndices.addAll(index.search(condition.value));
                    LOGGER.log(Level.FINE, "Used index on {0}.{1} for EQUALS condition, found {2} rows",
                            new Object[]{tableName, unqualifiedColumn, rowIndices.size()});
                } else if (condition.isInOperator() && condition.inValues != null) {
                    for (Object inValue : condition.inValues) {
                        rowIndices.addAll(index.search(inValue));
                    }
                    LOGGER.log(Level.FINE, "Used index on {0}.{1} for IN condition, found {2} rows",
                            new Object[]{tableName, unqualifiedColumn, rowIndices.size()});
                } else if (index instanceof BTreeIndex && (condition.operator == QueryParser.Operator.LESS_THAN || condition.operator == QueryParser.Operator.GREATER_THAN)) {
                    BTreeIndex bTreeIndex = (BTreeIndex) index;
                    Object low = condition.operator == QueryParser.Operator.GREATER_THAN ? condition.value : null;
                    Object high = condition.operator == QueryParser.Operator.LESS_THAN ? condition.value : null;
                    rowIndices.addAll(bTreeIndex.rangeSearch(low, high));
                    LOGGER.log(Level.FINE, "Used BTree index on {0}.{1} for range condition {2}, found {3} rows",
                            new Object[]{tableName, unqualifiedColumn, condition.operator, rowIndices.size()});
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
            String column = normalizeColumnName(condition.column, mainTableName);
            Object value = row.get(column);
            boolean isNull = value == null;
            boolean result = condition.operator == QueryParser.Operator.IS_NULL ? isNull : !isNull;
            return condition.not ? !result : result;
        }

        if (condition.isInOperator()) {
            String column = normalizeColumnName(condition.column, mainTableName);
            Object value = row.get(column);
            if (value == null) {
                return condition.not;
            }
            boolean inResult = condition.inValues.stream().anyMatch(v -> valuesEqual(v, value));
            return condition.not ? !inResult : inResult;
        }

        String leftColumn = normalizeColumnName(condition.column, mainTableName);
        Object leftValue = row.get(leftColumn);
        if (leftValue == null) {
            return condition.not;
        }

        Object rightValue;
        if (condition.isColumnComparison()) {
            String rightColumn = normalizeColumnName(condition.rightColumn, mainTableName);
            rightValue = row.get(rightColumn);
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
            case LESS_THAN_OR_EQUALS:
                result = comparison <= 0;
                break;
            case GREATER_THAN_OR_EQUALS:
                result = comparison >= 0;
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
            String normalizedColumn = normalizeColumnName(column, mainTableName);
            String columnAlias = normalizeColumnKey(column, mainTableName);
            String[] parts = column.trim().split("\\s+AS\\s+|\\s+", 2);
            if (parts.length > 1) {
                columnAlias = parts[1].trim();
                if (columnAlias.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
                    LOGGER.log(Level.FINE, "Using column alias: {0} -> {1}", new Object[]{normalizedColumn, columnAlias});
                } else {
                    columnAlias = normalizeColumnKey(column, mainTableName);
                }
            }
            if (row.containsKey(normalizedColumn)) {
                filtered.put(columnAlias, row.get(normalizedColumn));
            } else {
                // Пытаемся найти столбец по его неквалифицированному имени
                String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column.trim();
                for (Map.Entry<String, String> aliasEntry : tableAliases.entrySet()) {
                    String tableName = aliasEntry.getValue();
                    String possibleKey = tableName + "." + unqualifiedColumn;
                    if (row.containsKey(possibleKey)) {
                        filtered.put(columnAlias, row.get(possibleKey));
                        break;
                    }
                }
            }
        }
        return filtered;
    }

    private String normalizeColumnName(String column, String defaultTable) {
        if (column.contains(".")) {
            String[] parts = column.split("\\.", 2);
            String prefix = parts[0].trim();
            String colName = parts[1].trim();
            // Check if prefix is an alias or table name
            String resolvedTable = tableAliases.getOrDefault(prefix, prefix);
            // If resolvedTable is not in tableAliases values, use defaultTable
            if (!tableAliases.containsValue(resolvedTable)) {
                resolvedTable = defaultTable;
            }
            return resolvedTable + "." + colName;
        }
        return defaultTable + "." + column.trim();
    }

    private String normalizeColumnKey(String column, String defaultTable) {
        String normalized = normalizeColumnName(column, defaultTable);
        return normalized.contains(".") ? normalized.split("\\.")[1].trim() : normalized.trim();
    }

    public List<String> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    public List<QueryParser.AggregateFunction> getAggregates() {
        return Collections.unmodifiableList(aggregates);
    }

    public List<QueryParser.Condition> getConditions() {
        return Collections.unmodifiableList(conditions);
    }

    public List<QueryParser.JoinInfo> getJoins() {
        return Collections.unmodifiableList(joins);
    }

    public String getTableName() {
        return mainTableName;
    }

    public Integer getLimit() {
        return limit;
    }

    public Integer getOffset() {
        return offset;
    }

    public List<QueryParser.OrderByInfo> getOrderBy() {
        return Collections.unmodifiableList(orderBy);
    }

    public List<String> getGroupBy() {
        return Collections.unmodifiableList(groupBy);
    }

    public List<QueryParser.HavingCondition> getHavingConditions() {
        return Collections.unmodifiableList(havingConditions);
    }

    public Map<String, String> getTableAliases() {
        return Collections.unmodifiableMap(tableAliases);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SELECT ");

        // Append columns and aggregates
        List<String> selectItems = new ArrayList<>();
        selectItems.addAll(columns);
        selectItems.addAll(aggregates.stream().map(QueryParser.AggregateFunction::toString).toList());
        sb.append(String.join(", ", selectItems));

        // Append FROM clause
        sb.append(" FROM ").append(mainTableName);
        String mainTableAlias = tableAliases.entrySet().stream()
                .filter(e -> e.getValue().equals(mainTableName) && !e.getKey().equals(mainTableName))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
        if (mainTableAlias != null) {
            sb.append(" ").append(mainTableAlias);
        }

        // Append JOIN clauses
        for (QueryParser.JoinInfo join : joins) {
            sb.append(" ").append(join.joinType.toString().replace("_", " ")).append(" ");
            sb.append(join.tableName);
            if (join.alias != null) {
                sb.append(" ").append(join.alias);
            }
            if (!join.onConditions.isEmpty()) {
                sb.append(" ON ");
                sb.append(join.onConditions.stream()
                        .map(QueryParser.Condition::toString)
                        .collect(Collectors.joining(" ")));
            }
        }

        // Append WHERE clause
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            sb.append(conditions.stream()
                    .map(QueryParser.Condition::toString)
                    .collect(Collectors.joining(" ")));
        }

        // Append GROUP BY clause
        if (!groupBy.isEmpty()) {
            sb.append(" GROUP BY ");
            sb.append(String.join(", ", groupBy));
        }

        // Append HAVING clause
        if (!havingConditions.isEmpty()) {
            sb.append(" HAVING ");
            sb.append(havingConditions.stream()
                    .map(QueryParser.HavingCondition::toString)
                    .collect(Collectors.joining(" ")));
        }

        // Append ORDER BY clause
        if (!orderBy.isEmpty()) {
            sb.append(" ORDER BY ");
            sb.append(orderBy.stream()
                    .map(QueryParser.OrderByInfo::toString)
                    .collect(Collectors.joining(", ")));
        }

        // Append LIMIT clause
        if (limit != null) {
            sb.append(" LIMIT ").append(limit);
        }

        // Append OFFSET clause
        if (offset != null) {
            sb.append(" OFFSET ").append(offset);
        }

        return sb.toString();
    }
}