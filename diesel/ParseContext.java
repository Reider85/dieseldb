package diesel;

import java.util.Map;

/**
 * Prompt 32 (java:S107): Parameter Object for the condition/HAVING parse
 * pipeline. The parser methods shared seven context arguments (table name,
 * database, original query, join flag and the combined-column-type / alias
 * maps); this immutable holder replaces them so signatures stay below the
 * 7-parameter limit. Per-condition state (conjunction, NOT flag) and
 * clause-specific data (aggregate functions, subqueries) remain explicit
 * parameters.
 */
final class ParseContext {

    final String defaultTableName;
    final Database database;
    final String originalQuery;
    final boolean isJoinCondition;
    final Map<String, Class<?>> combinedColumnTypes;
    final Map<String, String> tableAliases;
    final Map<String, String> columnAliases;

    ParseContext(String defaultTableName, Database database, String originalQuery, boolean isJoinCondition,
                 Map<String, Class<?>> combinedColumnTypes, Map<String, String> tableAliases,
                 Map<String, String> columnAliases) {
        this.defaultTableName = defaultTableName;
        this.database = database;
        this.originalQuery = originalQuery;
        this.isJoinCondition = isJoinCondition;
        this.combinedColumnTypes = combinedColumnTypes;
        this.tableAliases = tableAliases;
        this.columnAliases = columnAliases;
    }
}