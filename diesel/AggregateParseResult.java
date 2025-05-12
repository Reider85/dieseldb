package diesel;

class AggregateParseResult {
    final boolean isAggregate;
    final String conditionColumn;
    final int aggEndIndex;

    AggregateParseResult(boolean isAggregate, String conditionColumn, int aggEndIndex) {
        this.isAggregate = isAggregate;
        this.conditionColumn = conditionColumn;
        this.aggEndIndex = aggEndIndex;
    }
}