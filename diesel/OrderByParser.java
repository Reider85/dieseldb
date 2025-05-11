package diesel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

class OrderByParser {
    private static final Logger LOGGER = Logger.getLogger(OrderByParser.class.getName());

    List<OrderByInfo> parseOrderByClause(String orderByClause, Map<String, Class<?>> combinedColumnTypes) {
        LOGGER.log(Level.FINE, "Parsing ORDER BY clause: {0}", orderByClause);
        List<OrderByInfo> orderByItems = new ArrayList<>();
        String[] items = orderByClause.split(",(?=([^']*'[^']*')*[^']*$)");

        for (String item : items) {
            String trimmedItem = item.trim();
            String[] parts = trimmedItem.split("\\s+");
            if (parts.length == 0) {
                throw new IllegalArgumentException("Invalid ORDER BY item: " + trimmedItem);
            }

            String column = parts[0].trim();
            boolean isDescending = false;

            if (parts.length > 1) {
                String direction = parts[1].toUpperCase();
                if (direction.equals("ASC")) {
                    isDescending = false;
                } else if (direction.equals("DESC")) {
                    isDescending = true;
                } else {
                    throw new IllegalArgumentException("Invalid ORDER BY direction: " + direction);
                }
            }

            String unqualifiedColumn = column.contains(".") ? column.split("\\.")[1].trim() : column;
            boolean found = false;
            for (Map.Entry<String, Class<?>> entry : combinedColumnTypes.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(unqualifiedColumn)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOGGER.log(Level.SEVERE, "Unknown column in ORDER BY: {0}, available columns: {1}",
                        new Object[]{column, combinedColumnTypes.keySet()});
                throw new IllegalArgumentException("Unknown column in ORDER BY: " + column);
            }

            orderByItems.add(new OrderByInfo(column, isDescending));
            LOGGER.log(Level.FINE, "Parsed ORDER BY item: column={0}, descending={1}",
                    new Object[]{column, isDescending});
        }

        LOGGER.log(Level.FINE, "Parsed ORDER BY items: {0}", orderByItems);
        return orderByItems;
    }
}