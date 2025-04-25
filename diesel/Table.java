package diesel;
import java.util.*;
import java.io.*;
import java.util.stream.Collectors;

class Table implements TableStorage {
    private final List<Map<String, Object>> rows = new ArrayList<>();
    private final List<String> columns;
    private final Map<String, Class<?>> columnTypes; // Хранит типы столбцов
    private static final File TABLES_DIR = new File("tables");

    public Table(List<String> columns, Map<String, Class<?>> columnTypes) {
        this.columns = new ArrayList<>(columns);
        this.columnTypes = new HashMap<>(columnTypes);
        if (!TABLES_DIR.exists()) {
            TABLES_DIR.mkdirs();
        }
    }

    @Override
    public void addRow(Map<String, Object> row) {
        if (row.keySet().containsAll(columns)) {
            Map<String, Object> validatedRow = new HashMap<>();
            for (String col : columns) {
                Object value = row.get(col);
                Class<?> expectedType = columnTypes.get(col);
                if (value != null && !expectedType.isInstance(value)) {
                    throw new IllegalArgumentException(
                            String.format("Invalid type for column %s: expected %s, got %s",
                                    col, expectedType.getSimpleName(), value.getClass().getSimpleName()));
                }
                validatedRow.put(col, value);
            }
            rows.add(validatedRow);
        }
    }

    @Override
    public List<Map<String, Object>> getRows() {
        return new ArrayList<>(rows);
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    @Override
    public Map<String, Class<?>> getColumnTypes() {
        return new HashMap<>(columnTypes);
    }

    @Override
    public void saveToFile(String tableName) {
        File file = new File(TABLES_DIR, tableName + ".csv");
        StringBuilder content = new StringBuilder();
        // Write header with type information
        content.append(String.join(",", columns)).append("\n");
        content.append(columns.stream()
                .map(col -> columnTypes.get(col).getSimpleName())
                .collect(Collectors.joining(","))).append("\n");
        // Write rows
        for (Map<String, Object> row : rows) {
            for (int i = 0; i < columns.size(); i++) {
                String value = row.getOrDefault(columns.get(i), "").toString();
                content.append(value);
                if (i < columns.size() - 1) {
                    content.append(",");
                }
            }
            content.append("\n");
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file), 65536)) {
            writer.write(content.toString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to save table " + tableName + ": " + e.getMessage());
        }
    }

    @Override
    public void loadFromFile(String tableName) {
        File file = new File(TABLES_DIR, tableName + ".csv");
        if (!file.exists()) {
            return;
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            // Read header
            String header = reader.readLine();
            if (header == null || !header.equals(String.join(",", columns))) {
                throw new RuntimeException("Invalid header in table file: " + tableName);
            }
            // Read type information
            String typesLine = reader.readLine();
            if (typesLine == null) {
                throw new RuntimeException("Missing type information in table file: " + tableName);
            }
            String[] types = typesLine.split(",");
            if (types.length != columns.size()) {
                throw new RuntimeException("Type count mismatch in table file: " + tableName);
            }
            // Read rows
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length != columns.size()) {
                    continue;
                }
                Map<String, Object> row = new HashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    String value = values[i];
                    Class<?> type = columnTypes.get(columns.get(i));
                    if (type == Integer.class) {
                        row.put(columns.get(i), value.isEmpty() ? null : Integer.parseInt(value));
                    } else {
                        row.put(columns.get(i), value);
                    }
                }
                rows.add(row);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load table " + tableName + ": " + e.getMessage());
        }
    }
}