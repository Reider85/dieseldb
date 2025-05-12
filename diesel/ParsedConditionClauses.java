package diesel;

import java.util.List;

public class ParsedConditionClauses {
    final String conditionStr;
    final Integer limit;
    final Integer offset;
    final List<OrderByInfo> orderBy;
    final List<String> groupBy;
    final String havingStr;

    public ParsedConditionClauses(String conditionStr, Integer limit, Integer offset,
                                  List<OrderByInfo> orderBy, List<String> groupBy, String havingStr) {
        this.conditionStr = conditionStr;
        this.limit = limit;
        this.offset = offset;
        this.orderBy = orderBy;
        this.groupBy = groupBy;
        this.havingStr = havingStr;
    }
}