package diesel.security;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory {@link UserStoreBackend} shared by the security unit tests
 * (prompt4 #31): stores copies of the rows per system table, so the store
 * logic can be tested without the engine.
 */
class InMemoryUserStoreBackend implements UserStoreBackend {

    private final Map<String, List<Map<String, Object>>> tables = new HashMap<>();

    @Override
    public List<Map<String, Object>> readAll(String tableName) {
        return tables.getOrDefault(tableName, List.of());
    }

    @Override
    public void appendRow(String tableName, Map<String, Object> row) {
        tables.computeIfAbsent(tableName, k -> new ArrayList<>()).add(new LinkedHashMap<>(row));
    }

    @Override
    public void updateRow(String tableName, Map<String, Object> row) {
        List<Map<String, Object>> rows = tables.get(tableName);
        if (rows == null) {
            throw new IllegalStateException("No such table: " + tableName);
        }
        for (int i = 0; i < rows.size(); i++) {
            if (String.valueOf(rows.get(i).get("ID")).equals(String.valueOf(row.get("ID")))) {
                rows.set(i, new LinkedHashMap<>(row));
                return;
            }
        }
        throw new IllegalStateException("No row with ID " + row.get("ID") + " in " + tableName);
    }
}
