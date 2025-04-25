package diesel;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.Logger;
import java.util.logging.Level;
// Query parser (SRP, Creator)
// Query parser (SRP, Creator)
class QueryParser {
    private static final Logger LOGGER = Logger.getLogger(QueryParser.class.getName());

    public Query<?> parse(String query) {
        try {
            String normalized = query.trim().toUpperCase();
            if (normalized.startsWith("SELECT")) {
                return parseSelectQuery(normalized, query);
            } else if (normalized.startsWith("INSERT INTO")) {
                return parseInsertQuery(normalized, query);
            }
            throw new IllegalArgumentException("Unsupported query type");
        } catch (IllegalArgumentException e) {
            LOGGER.log(Level.SEVERE, "Failed to parse query: {0}, Error: {1}", new Object[]{query, e.getMessage()});
            throw e;
        }
    }

    private Query<List<Map<String, String>>> parseSelectQuery(String normalized, String original) {
        String[] parts = normalized.split("FROM");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid SELECT query format");
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

        LOGGER.log(Level.INFO, "Parsed SELECT query: columns={0}, table={1}, condition={2}={3}",
                new Object[]{columns, tableName, conditionColumn != null ? conditionColumn : "none",
                        conditionValue != null ? conditionValue : "none"});

        return new SelectQuery(columns, conditionColumn, conditionValue);
    }

    private Query<Void> parseInsertQuery(String normalized, String original) {
        String[] parts = normalized.split("VALUES");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid INSERT query format");
        }

        String tableAndColumns = parts[0].replace("INSERT INTO", "").trim();
        String tableName = tableAndColumns.substring(0, tableAndColumns.indexOf("(")).trim();
        String columnsPart = tableAndColumns.substring(tableAndColumns.indexOf("(") + 1,
                tableAndColumns.indexOf(")")).trim();
        List<String> columns = Arrays.stream(columnsPart.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        String valuesPart = parts[1].trim();
        if (!valuesPart.startsWith("(") || !valuesPart.endsWith(")")) {
            throw new IllegalArgumentException("Invalid VALUES syntax");
        }
        valuesPart = valuesPart.substring(1, valuesPart.length() - 1).trim();
        List<String> values = Arrays.stream(valuesPart.split(","))
                .map(val -> val.trim().replace("'", ""))
                .collect(Collectors.toList());

        LOGGER.log(Level.INFO, "Parsed INSERT query: table={0}, columns={1}, values={2}",
                new Object[]{tableName, columns, values});

        return new InsertQuery(columns, values);
    }
}

