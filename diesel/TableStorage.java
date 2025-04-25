package diesel;
import java.util.*;
public interface TableStorage {
    List<Map<String, String>> getRows();
    List<String> getColumns();
}
