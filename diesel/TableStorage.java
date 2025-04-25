package diesel;
import java.util.*;
interface TableStorage {
    List<Map<String, String>> getRows();
    List<String> getColumns();
    void addRow(Map<String, String> row);
}
