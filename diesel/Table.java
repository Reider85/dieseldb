package diesel;
import java.util.*;
import java.io.*;

class Table implements TableStorage {
    private final List<Map<String, String>> rows = new ArrayList<>();
    private final List<String> columns;

    public Table(List<String> columns) {
        this.columns = new ArrayList<>(columns);
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
        File dir = new File("tables");
        if (!dir.exists()) {
            dir.mkdirs();
        }
        File file = new File(dir, tableName + ".csv");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            // Write header
            writer.write(String.join(",", columns));
            writer.newLine();
            // Write rows
            for (Map<String, String> row : rows) {
                List<String> values = new ArrayList<>();
                for (String col : columns) {
                    values.add(row.getOrDefault(col, ""));
                }
                writer.write(String.join(",", values));
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to save table " + tableName + ": " + e.getMessage());
        }
    }

    @Override
    public void loadFromFile(String tableName) {
        File file = new File("tables", tableName + ".csv");
        if (!file.exists()) {
            return; // No file means empty table
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            // Read header
            String header = reader.readLine();
            if (header == null || !header.equals(String.join(",", columns))) {
                throw new RuntimeException("Invalid header in table file: " + tableName);
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