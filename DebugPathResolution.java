import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import diesel.storage.json.JsonPathResolver;

public class DebugPathResolution {
    public static void main(String[] args) {
        List<String> columns = List.of("ID", "PROFL");
        TreeMap<String, Integer> indexByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < columns.size(); i++) {
            indexByName.put(columns.get(i), i);
        }
        
        JsonPathResolver.SchemaIndex schemaIndex = new JsonPathResolver.SchemaIndex(columns, indexByName);
        
        String path = "PROFL.user.address.city";
        System.out.println("Resolving path: " + path);
        System.out.println("Available columns: " + columns);
        System.out.println("Index: " + indexByName);
        
        JsonPathResolver.ResolvedPath resolved = schemaIndex.resolve(path);
        System.out.println("Resolved: " + resolved);
        System.out.println("Column index: " + resolved.columnIndex());
        System.out.println("Column: " + resolved.column());
        System.out.println("Segments: " + resolved.segments());
    }
}