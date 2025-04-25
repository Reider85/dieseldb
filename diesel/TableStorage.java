package diesel;
import java.util.*;

interface TableStorage {
    List<Map<String, Object>> getRows(); // Изменено с String на Object
    List<String> getColumns();
    Map<String, Class<?>> getColumnTypes(); // Новый метод для хранения типов столбцов
    void addRow(Map<String, Object> row); // Изменено с String на Object
    void saveToFile(String tableName);
    void loadFromFile(String tableName);
}