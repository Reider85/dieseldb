package diesel;

import java.util.List;
import java.util.Map;
import diesel.partition.PartitionManager;
import diesel.partition.PartitionPruningIntegration;
import diesel.partition.RangePartition;
import diesel.partition.ListPartition;
import diesel.partition.Subpartition;

/**
 * Executes an EXPLAIN statement: renders a textual execution-plan tree for a
 * SELECT / INSERT / UPDATE / DELETE statement without executing it. With the
 * ANALYZE keyword the statement is executed and the plan is followed by the
 * actual metrics (returned row count, hash-join metrics, affected rows and
 * elapsed time).
 *
 * @see SelectQuery
 * @see Query
 */
class ExplainQuery implements Query<String> {
    private final Query<?> innerQuery;
    private final boolean analyze;
    private final String innerSql;

    /**
     * Creates an explain query for the given inner statement.
     *
     * @param innerQuery the parsed SELECT / INSERT / UPDATE / DELETE query
     * @param analyze    true for EXPLAIN ANALYZE (executes the statement)
     * @param innerSql   the raw SQL text of the inner statement, used by
     *                   {@link Database} to resolve the target table
     */
    ExplainQuery(Query<?> innerQuery, boolean analyze, String innerSql) {
        this.innerQuery = innerQuery;
        this.analyze = analyze;
        this.innerSql = innerSql;
    }

    Query<?> getInnerQuery() {
        return innerQuery;
    }

    String getInnerSql() {
        return innerSql;
    }

    boolean isAnalyze() {
        return analyze;
    }

    @Override
    public String execute(Table table) {
        if (innerQuery instanceof SelectQuery sq) {
            return executeSelect(sq, table);
        }
        return executeDml(table);
    }

    private String executeSelect(SelectQuery select, Table table) {
        StringBuilder sb = new StringBuilder(select.describePlan(table));
        
        // Add partition pruning information if the table is partitioned
        addPartitionPruningInfo(sb, table, select);
        
        if (analyze) {
            long start = System.nanoTime();
            List<Map<String, Object>> rows = select.execute(table);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            sb.append('\n').append("Actual metrics (ANALYZE):").append('\n');
            sb.append("  rows: ").append(rows.size()).append('\n');
            sb.append("  elapsed: ").append(elapsedMs).append(" ms").append('\n');
            if (!select.getJoins().isEmpty()) {
                sb.append("  hash join table size: ").append(select.getLastHashJoinTableSize()).append('\n');
                sb.append("  hash join build time: ").append(select.getLastHashJoinBuildTimeMs()).append(" ms").append('\n');
                sb.append("  hash join probe time: ").append(select.getLastHashJoinProbeTimeMs()).append(" ms").append('\n');
                sb.append("  hash join partitioned: ").append(select.isLastJoinUsedPartitioning()).append('\n');
            }
            sb.append("  index lookups: ").append(select.getLastIndexLookupCount()).append('\n');
            sb.append("  index-only scans: ").append(select.getLastIndexOnlyScanCount()).append('\n');
        }
        return sb.toString();
    }

    private String executeDml(Table table) {
        String operation;
        if (innerQuery instanceof InsertQuery) {
            operation = SqlKeywords.INSERT;
        } else if (innerQuery instanceof UpdateQuery) {
            operation = SqlKeywords.UPDATE;
        } else {
            operation = SqlKeywords.DELETE;
        }
        StringBuilder sb = new StringBuilder("Execution Plan\n");
        sb.append("  Operation: ").append(operation).append('\n');
        sb.append("  Table: ").append(table.getName()).append(" (estimated rows: ").append(table.rowCount()).append(")\n");
        if (innerQuery instanceof InsertQuery iq) {
            sb.append("  Columns: ").append(iq.getColumns()).append('\n');
        } else if (innerQuery instanceof UpdateQuery uq) {
            sb.append("  Columns: ").append(uq.getUpdates().keySet()).append('\n');
            sb.append("  Conditions: ").append(formatConditions(uq.getConditions())).append('\n');
            sb.append(MessageConstants.SQL_INDEX_PREFIX).append(describeIndex(table, uq.getConditions())).append('\n');
        } else {
            DeleteQuery delete = (DeleteQuery) innerQuery;
            sb.append("  Conditions: ").append(formatConditions(delete.getConditions())).append('\n');
            sb.append(MessageConstants.SQL_INDEX_PREFIX).append(describeIndex(table, delete.getConditions())).append('\n');
        }

        if (analyze) {
            long start = System.nanoTime();
            innerQuery.execute(table);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            sb.append('\n').append("Actual metrics (ANALYZE):").append('\n');
            sb.append("  affected rows: ").append(affectedRows()).append('\n');
            sb.append("  elapsed: ").append(elapsedMs).append(" ms").append('\n');
        }
        return sb.toString();
    }

    private String formatConditions(List<QueryParser.Condition> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return "none";
        }
        return conditions.stream()
                .map(QueryParser.Condition::toString)
                .collect(java.util.stream.Collectors.joining(" "));
    }

    /**
     * Reports which secondary index a DML statement would use for its single
     * equality, IN, or range condition, mirroring {@link DeleteQuery#execute}
     * and {@link UpdateQuery#identifyRows}.
     */
    private String describeIndex(Table table, List<QueryParser.Condition> conditions) {
        if (conditions == null || conditions.size() != 1) {
            return ErrorMessages.NONE_FULL_SCAN;
        }
        QueryParser.Condition condition = conditions.get(0);
        if (condition.isGrouped() || condition.not || condition.rightColumn != null
                || condition.subQuery != null) {
            return ErrorMessages.NONE_FULL_SCAN;
        }
        if (!(condition.operator == QueryParser.Operator.EQUALS || condition.isInOperator()
                || condition.operator == QueryParser.Operator.GREATER_THAN_OR_EQUALS
                || condition.operator == QueryParser.Operator.LESS_THAN_OR_EQUALS)) {
            return ErrorMessages.NONE_FULL_SCAN;
        }
        Index index = table.getIndex(condition.column);
        if (index == null) {
            return ErrorMessages.NONE_FULL_SCAN;
        }
        return SelectQuery.indexTypeName(index) + MessageConstants.SQL_INDEX_ON + table.getName() + "." + condition.column;
    }

    private long affectedRows() {
        if (innerQuery instanceof InsertQuery iq) {
            return iq.getLastAffectedRows();
        }
        if (innerQuery instanceof UpdateQuery uq) {
            return uq.getLastAffectedRows();
        }
        if (innerQuery instanceof DeleteQuery dq) {
            return dq.getLastAffectedRows();
        }
        return 0;
    }
    
    /**
     * Adds partition pruning information to the explain output.
     */
    private void addPartitionPruningInfo(StringBuilder sb, Table table, SelectQuery select) {
        if (table.getPartitionManager() == null) {
            return; // Table is not partitioned
        }
        
        PartitionManager partitionManager = table.getPartitionManager();
        
        // Extract filter values from the WHERE clause conditions; the raw SQL
        // fallback covers condition lists that cannot yet be introspected.
        Map<String, Object> filterValues = PartitionPruningIntegration.extractFilterValuesFromConditions(
            select.getConditions());
        if (filterValues.isEmpty() && innerSql != null) {
            filterValues = PartitionPruningIntegration.extractFilterValues(innerSql);
        }

        if (filterValues.isEmpty()) {
            sb.append("\n  Partition pruning: No filter conditions on partition column - scanning all ")
              .append(partitionManager.getPartitions().size()).append(" partitions");
            appendSubpartitionInfo(sb, partitionManager, null);
            return;
        }

        if (partitionManager.hasSubpartitions()) {
            PartitionManager.TwoLevelPruningResult twoLevel =
                    partitionManager.routeSelectWithTwoLevelPruning(filterValues);
            sb.append("\n  Partition pruning: ").append(twoLevel.getPruningSummary());
            appendSubpartitionInfo(sb, partitionManager, twoLevel);
            return;
        }

        // Apply partition pruning
        PartitionManager.PartitionPruningResult pruningResult =
            partitionManager.routeSelectWithPruning(filterValues);

        sb.append("\n  Partition pruning: ").append(pruningResult.getPruningSummary());

        // Show detailed partition information if not too many partitions
        if (pruningResult.getPartitionsToScan().size() <= 5) {
            sb.append("\n  Partitions to scan:");
            for (PartitionManager.Partition partition : pruningResult.getPartitionsToScan()) {
                sb.append("\n    - ").append(partition.getName());
                if (partition instanceof RangePartition) {
                    RangePartition range = (RangePartition) partition;
                    sb.append(" [").append(range.getMinValue()).append(" - ").append(range.getMaxValue()).append(")");
                } else if (partition instanceof ListPartition) {
                    ListPartition list = (ListPartition) partition;
                    if (list.getPartitionValues() != null) {
                        sb.append(" ").append(list.getPartitionValues());
                    }
                }
            }
        }
    }

    private void appendSubpartitionInfo(StringBuilder sb, PartitionManager manager,
                                        PartitionManager.TwoLevelPruningResult result) {
        if (!manager.hasSubpartitions()) {
            return;
        }
        sb.append("\n  Subpartition pruning:");
        for (java.util.Map.Entry<String, Subpartition> entry : manager.getSubpartitions().entrySet()) {
            Subpartition sub = entry.getValue();
            Subpartition.PruningResult pruned = result == null ? null : result.getLevel2().get(entry.getKey());
            sb.append("\n    ").append(entry.getKey()).append(" -> ").append(sub.getName())
              .append(" (").append(sub.getType()).append(" on ").append(sub.getColumn()).append(")");
            if (pruned != null) {
                sb.append(": scan ").append(pruned.getPartitionsToScan().size())
                  .append(", skip ").append(pruned.getPartitionsSkipped())
                  .append(", total ").append(pruned.getTotalPartitions());
            }
        }
    }
}
