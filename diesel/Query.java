package diesel;
import java.util.*;

interface Query {
    List<Map<String, String>> execute(Table table);
}

