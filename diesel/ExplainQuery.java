package diesel;

import java.util.List;
import java.util.Map;

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
        if (innerQuery instanceof SelectQuery) {
            return executeSelect((SelectQuery) innerQuery, table);
        }
        return executeDml(table);
    }

    private String executeSelect(SelectQuery select, Table table) {
        StringBuilder sb = new StringBuilder(select.describePlan(table));
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
        if (innerQuery instanceof InsertQuery) {
            sb.append("  Columns: ").append(((InsertQuery) innerQuery).getColumns()).append('\n');
        } else if (innerQuery instanceof UpdateQuery) {
            UpdateQuery update = (UpdateQuery) innerQuery;
            sb.append("  Columns: ").append(update.getUpdates().keySet()).append('\n');
            sb.append("  Conditions: ").append(formatConditions(update.getConditions())).append('\n');
            sb.append("  Index: none (full scan)\n");
        } else {
            DeleteQuery delete = (DeleteQuery) innerQuery;
            sb.append("  Conditions: ").append(formatConditions(delete.getConditions())).append('\n');
            sb.append("  Index: ").append(describeDeleteIndex(table, delete.getConditions())).append('\n');
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
     * Reports which secondary index DELETE would use for its single equality
     * or IN condition, mirroring {@link DeleteQuery#execute} (the clustered
     * index is not consulted by DELETE).
     */
    private String describeDeleteIndex(Table table, List<QueryParser.Condition> conditions) {
        if (conditions == null || conditions.size() != 1) {
            return "none (full scan)";
        }
        QueryParser.Condition condition = conditions.get(0);
        if (condition.isGrouped() || condition.not
                || !(condition.operator == QueryParser.Operator.EQUALS || condition.isInOperator())) {
            return "none (full scan)";
        }
        Index index = table.getIndex(condition.column);
        if (index == null) {
            return "none (full scan)";
        }
        return SelectQuery.indexTypeName(index) + " index on " + table.getName() + "." + condition.column;
    }

    private long affectedRows() {
        if (innerQuery instanceof InsertQuery) {
            return ((InsertQuery) innerQuery).getLastAffectedRows();
        }
        if (innerQuery instanceof UpdateQuery) {
            return ((UpdateQuery) innerQuery).getLastAffectedRows();
        }
        if (innerQuery instanceof DeleteQuery) {
            return ((DeleteQuery) innerQuery).getLastAffectedRows();
        }
        return 0;
    }
}
