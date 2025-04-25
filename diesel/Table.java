package diesel;
import java.util.*;
import java.io.*;

class Table implements TableStorage {
    private final List<Map<String, String>> rows = new ArrayList<>();
    private final List<String> columns;
    private static final File TABLES_DIR = new File("tables");

    public Table(List<String> columns) {
        this.columns = new ArrayList<>(columns);
        // Ensure directory exists once during table creation
        if (!TABLES_DIR.exists()) {
            TABLES_DIR.mkdirs();
        }
    }

    @Override
    public void addRow(Map<String, String> row) {
        if (row.keySet().containsAll(columns)) {
            rows.add(new HashMap<>(row));
        }
    }

    @Override
    public List<Map<String, String>> getRows() {
        return new ArrayList<>(rows);
    }

    @Override
    public List<String> getColumns() {
        return new ArrayList<>(columns);
    }

    @Override
    public void saveToFile(String tableName) {
        File file = new File(TABLES_DIR, tableName + ".csv");
        // Use StringBuilder to build content
        StringBuilder content = new StringBuilder();
        // Write header
        content.append(String.join(",", columns)).append("\n");
        // Write rows
        for (Map<String, String> row : rows) {
            for (int i = 0; i < columns.size(); i++) {
                String value = row.getOrDefault(columns.get(i), "");
                content.append(value);
                if (i < columns.size() - 1) {
                    content.append(",");
                }
            }
            content.append("\n");
        }
        // Write to file with large buffer
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file), 65536)) { // 64KB buffer
            writer.write(content.toString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to save table " + tableName + ": " + e.getMessage());
        }
    }

    @Override
    public void loadFromFile(String tableName) {
        File file = new File(TABLES_DIR, tableName + ".csv");
        if (!file.exists()) {
            return; // No file means empty table
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            // Read header
            String header = reader.readLine();
            if (header == null || !header.equals(String.join(",", columns))) {
                String errorMsg = String.format("Invalid header in table file: %s. Expected: %s, Found: %s",
                        tableName, String.join(",", columns), header == null ? "null" : header);
                throw new RuntimeException(errorMsg);
            }
            // Read rows
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length != columns.size()) {
                    continue; // Skip malformed rows
                }
                Map<String, String> row = new HashMap<>();
                for (int i = 0; i < columns.size(); i++) {
                    row.put(columns.get(i), values[i]);
                }
                rows.add(row);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load table " + tableName + ": " + e.getMessage());
        }
    }
}