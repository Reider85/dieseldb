package diesel;

class OperatorParseResult {
    final String[] partsByOperator;
    final QueryParser.Operator operator;

    OperatorParseResult(String[] partsByOperator, QueryParser.Operator operator) {
        this.partsByOperator = partsByOperator;
        this.operator = operator;
    }
}