package diesel;

import java.util.List;

public class ParsedClauses {
    final List<Condition> conditions;
    final List<Condition> havingConditions;
    final List<OrderByInfo> orderBy;
    final List<String> groupBy;
    final Integer limit;
    final Integer offset;

    public ParsedClauses(List<Condition> conditions, List<Condition> havingConditions,
                         List<OrderByInfo> orderBy, List<String> groupBy, Integer limit, Integer offset) {
        this.conditions = conditions;
        this.havingConditions = havingConditions;
        this.orderBy = orderBy;
        this.groupBy = groupBy;
        this.limit = limit;
        this.offset = offset;
    }
}