package diesel;

import java.util.List;

public class ParsedSelectItems {
    final List<String> columns;
    final List<AggregateFunction> aggregates;

    public ParsedSelectItems(List<String> columns, List<AggregateFunction> aggregates) {
        this.columns = columns;
        this.aggregates = aggregates;
    }
}