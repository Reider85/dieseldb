package diesel;

import java.util.List;
import java.util.Map;

public class ParsedTableAndJoins {
    final Table mainTable;
    final List<JoinInfo> joins;
    final Map<String, Class<?>> combinedColumnTypes;
    final String remaining;

    public ParsedTableAndJoins(Table mainTable, List<JoinInfo> joins,
                               Map<String, Class<?>> combinedColumnTypes, String remaining) {
        this.mainTable = mainTable;
        this.joins = joins;
        this.combinedColumnTypes = combinedColumnTypes;
        this.remaining = remaining;
    }
}