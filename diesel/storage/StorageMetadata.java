package diesel.storage;

import java.util.Map;

/**
 * Metadata about a storage implementation. Used by the RowStorage interface
 * to provide information about capabilities and configuration.
 */
public final class StorageMetadata {
    
    private final String storageType;
    private final boolean supportsPersistence;
    private final boolean supportsMVCC;
    private final Map<String, Object> properties;

    public StorageMetadata(String storageType, boolean supportsPersistence, boolean supportsMVCC) {
        this(storageType, supportsPersistence, supportsMVCC, Map.of());
    }

    public StorageMetadata(String storageType, boolean supportsPersistence, boolean supportsMVCC, 
                          Map<String, Object> properties) {
        this.storageType = storageType;
        this.supportsPersistence = supportsPersistence;
        this.supportsMVCC = supportsMVCC;
        this.properties = properties != null ? Map.copyOf(properties) : Map.of();
    }

    public String getStorageType() {
        return storageType;
    }

    public boolean supportsPersistence() {
        return supportsPersistence;
    }

    public boolean supportsMVCC() {
        return supportsMVCC;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "StorageMetadata{" +
                "storageType='" + storageType + '\'' +
                ", supportsPersistence=" + supportsPersistence +
                ", supportsMVCC=" + supportsMVCC +
                ", properties=" + properties +
                '}';
    }
}