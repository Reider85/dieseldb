package diesel.storage;

/**
 * Enumeration of supported storage types for tables.
 */
public enum StorageType {
    CSV("csv"),
    TSV("tsv"),
    JSONL("jsonl"),
    AVRO("avro"),
    IN_MEMORY("in_memory");
    
    private final String typeName;
    
    StorageType(String typeName) {
        this.typeName = typeName;
    }
    
    public String getTypeName() {
        return typeName;
    }
    
    public static StorageType fromString(String typeName) {
        if (typeName == null) {
            return IN_MEMORY;
        }
        
        return switch (typeName.trim().toLowerCase()) {
            case "csv" -> CSV;
            case "tsv" -> TSV;
            case "jsonl" -> JSONL;
            case "avro" -> AVRO;
            case "in_memory", "inmemory" -> IN_MEMORY;
            default -> IN_MEMORY;
        };
    }
}