package diesel;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.Logger;
import java.util.logging.Level;
// Query parser (SRP, Creator)
class QueryParser {
    private static final Logger LOGGER = Logger.getLogger(QueryParser.class.getName());

    public Query parse(String query) {
        try {
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
            String tableName = tableAndCondition.split(" ")[0].trim();
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

            LOGGER.log(Level.INFO, "Parsed query: columns={0}, table={1}, condition={2}={3}",
                    new Object[]{columns, tableName, conditionColumn != null ? conditionColumn : "none",
                            conditionValue != null ? conditionValue : "none"});

            return new SelectQuery(columns, conditionColumn, conditionValue);
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse query: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
    }
}

