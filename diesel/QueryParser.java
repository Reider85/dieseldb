package diesel;
import java.util.*;
import java.util.stream.Collectors;
// Query parser (SRP, Creator)
class QueryParser {
    public Query parse(String query) {
        String normalized = query.trim().toUpperCase();
        if (!normalized.startsWith("SELECT")) {
            throw new IllegalArgumentException("Only SELECT queries supported");
        }

        String[] parts = normalized.split("FROM");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid query format");
        }

        String selectPart = parts[0].replace("SELECT", "").trim();
        String[] selectColumns = selectPart.split(",");
        List<String> columns = Arrays.stream(selectColumns)
                .map(String::trim)
                .collect(Collectors.toList());

        String tableAndCondition = parts[1].trim();
        String conditionColumn = null;
        String conditionValue = null;

        if (tableAndCondition.contains("WHERE")) {
            String[] tableCondition = tableAndCondition.split("WHERE");
            String condition = tableCondition[1].trim();
            String[] conditionParts = condition.split("=");
            if (conditionParts.length == 2) {
                conditionColumn = conditionParts[0].trim();
                conditionValue = conditionParts[1].trim().replace("'", "");
            }
        }

        return new SelectQuery(columns, conditionColumn, conditionValue);
    }
}

