package diesel.storage.avro;

import org.apache.avro.Schema;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Index for Avro schema fields, enabling O(1) lookups by field name.
 * 
 * Cache for position and field information per schema, eliminating O(n) linear scans
 * in {@link AvroRowStorage#fromRecord} and {@link AvroQueryExecutor#convertRecordToMap}.
 * 
 * <p>Immutable; thread-safe via static {@link #CACHE}.</p>
 * 
 * @since 3.3.77 (O2 Avro-1)
 */
public final class AvroSchemaIndex {
    private final Map<String, Integer> nameToPos;
    private final Map<String, Schema.Field> nameToField;
    
    /**
     * Cache of schema indexes keyed by schema object.
     * Unbounded but bounded by distinct schema count (typically one per table version).
     */
    private static final ConcurrentHashMap<Schema, AvroSchemaIndex> CACHE = new ConcurrentHashMap<>();

    private AvroSchemaIndex(Map<String, Integer> nameToPos, Map<String, Schema.Field> nameToField) {
        this.nameToPos = Map.copyOf(nameToPos);  // defensive copy
        this.nameToField = Map.copyOf(nameToField);
    }

    /**
     * Get or build an index for the given schema.
     * 
     * @param schema Avro schema to index (must not be null)
     * @return existing or newly built index
     */
    public static AvroSchemaIndex forSchema(Schema schema) {
        return CACHE.computeIfAbsent(schema, AvroSchemaIndex::build);
    }

    /**
     * Clear the cache (for testing/reset).
     */
    public static void clearCache() {
        CACHE.clear();
    }

    /**
     * Get the number of cached schemas.
     */
    public static int cacheSize() {
        return CACHE.size();
    }

    /**
     * Get field position by name (case-sensitive).
     * 
     * @param fieldName field name to look up (must not be null)
     * @return position (0-based) or null if not found
     */
    public Integer getPosition(String fieldName) {
        return nameToPos.get(fieldName);
    }

    /**
     * Get field by name (case-sensitive).
     * 
     * @param fieldName field name to look up (must not be null)
     * @return field or null if not found
     */
    public Schema.Field getField(String fieldName) {
        return nameToField.get(fieldName);
    }

    /**
     * Build a new index for the given schema.
     */
    private static AvroSchemaIndex build(Schema schema) {
        Map<String, Integer> nameToPos = new HashMap<>();
        Map<String, Schema.Field> nameToField = new HashMap<>();
        
        for (int i = 0; i < schema.getFields().size(); i++) {
            Schema.Field field = schema.getFields().get(i);
            String name = field.name();
            nameToPos.put(name, i);
            nameToField.put(name, field);
        }
        
        return new AvroSchemaIndex(nameToPos, nameToField);
    }
}