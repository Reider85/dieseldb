import diesel.storage.json.JsonPathResolver;
import diesel.storage.json.JsonParserConfig;
import java.util.List;

public class DebugExtract {
    public static void main(String[] args) {
        String jsonText = "{\"user\":{\"name\":\"Alice\",\"address\":{\"city\":\"Москва\"}}}";
        List<String> segments = List.of("user", "address", "city");
        
        System.out.println("JSON text: " + jsonText);
        System.out.println("Segments: " + segments);
        
        Object result = JsonPathResolver.extract(jsonText, segments, JsonParserConfig.defaults());
        System.out.println("Extracted: " + result);
    }
}