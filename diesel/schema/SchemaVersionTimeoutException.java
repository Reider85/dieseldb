package diesel.schema;

/**
 * Thrown when attempting to access a schema version that has been killed due to timeout.
 * Schema versions older than configured timeout are removed from the registry to bound memory usage.
 * 
 * @see SchemaVersionRegistry#resolve(long)
 * @see SchemaVersionRegistry#reapStalePins(long)
 */
public class SchemaVersionTimeoutException extends RuntimeException {
    
    private final long version;
    
    /**
     * Creates a new exception for a specific version that was killed.
     * 
     * @param version The version that was accessed but is no longer available
     */
    public SchemaVersionTimeoutException(long version) {
        super("Schema version " + version + " has been killed due to timeout");
        this.version = version;
    }
    
    /**
     * Creates a new exception with a custom message.
     * 
     * @param version The version that was accessed
     * @param message Detailed error message
     */
    public SchemaVersionTimeoutException(long version, String message) {
        super(message);
        this.version = version;
    }
    
    public long getVersion() {
        return version;
    }
}