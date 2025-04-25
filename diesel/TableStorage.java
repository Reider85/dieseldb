package diesel;
import java.util.*;

interface TableStorage {
    List<Map<String, Object>> getRows(); // �������� � String �� Object
    List<String> getColumns();
    Map<String, Class<?>> getColumnTypes(); // ����� ����� ��� �������� ����� ��������
    void addRow(Map<String, Object> row); // �������� � String �� Object
    void saveToFile(String tableName);
    void loadFromFile(String tableName);
}