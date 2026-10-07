package diesel.storage.tablespace;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry for managing multiple tablespaces.
 * 
 * This is a stub implementation that only supports the default tablespace.
 * Multi-tablespace support will be implemented in Phase 2.
 */
public class TablespaceRegistry {
    private final Map<String, Tablespace> tablespaces;
    private final Tablespace defaultTablespace;
    
    /**
     * Creates a new tablespace registry.
     * 
     * @param defaultTablespacePath the path for the default tablespace
     * @param maxFileSizeBytes maximum file size in bytes
     */
    public TablespaceRegistry(Path defaultTablespacePath, long maxFileSizeBytes) {
        this.tablespaces = new HashMap<>();
        this.defaultTablespace = new Tablespace(defaultTablespacePath, maxFileSizeBytes);
        
        // Register the default tablespace
        tablespaces.put("default", defaultTablespace);
    }
    
    /**
     * Gets the default tablespace.
     * 
     * @return the default tablespace
     */
    public Tablespace getDefaultTablespace() {
        return defaultTablespace;
    }
    
    /**
     * Gets a tablespace by name.
     * 
     * @param name the tablespace name
     * @return the tablespace
     * @throws UnsupportedOperationException for non-default tablespaces (stub implementation)
     */
    public Tablespace getTablespace(String name) {
        if ("default".equals(name)) {
            return defaultTablespace;
        }
        
        // Multi-tablespace support not implemented yet
        throw new UnsupportedOperationException("Multi-tablespace support not implemented yet. Only 'default' tablespace is supported.");
    }
    
    /**
     * Creates a new tablespace.
     * 
     * @param name the tablespace name
     * @param path the directory path for the tablespace
     * @param maxFileSizeBytes maximum file size in bytes
     * @throws UnsupportedOperationException always (stub implementation)
     */
    public void createTablespace(String name, Path path, long maxFileSizeBytes) {
        // Multi-tablespace support not implemented yet
        throw new UnsupportedOperationException("Multi-tablespace support not implemented yet. Only 'default' tablespace is supported.");
    }
    
    /**
     * Removes a tablespace.
     * 
     * @param name the tablespace name
     * @throws UnsupportedOperationException always (stub implementation)
     */
    public void removeTablespace(String name) {
        // Multi-tablespace support not implemented yet
        throw new UnsupportedOperationException("Multi-tablespace support not implemented yet. Only 'default' tablespace is supported.");
    }
    
    /**
     * Lists all tablespace names.
     * 
     * @return list of tablespace names
     */
    public java.util.List<String> listTablespaces() {
        return new java.util.ArrayList<>(tablespaces.keySet());
    }
    
    /**
     * Flushes all tablespaces.
     * 
     * @throws IOException if an I/O error occurs
     */
    public synchronized void flush() throws java.io.IOException {
        for (Tablespace tablespace : tablespaces.values()) {
            tablespace.flush();
        }
    }
    
    /**
     * Closes all tablespaces.
     * 
     * @throws IOException if an I/O error occurs
     */
    public synchronized void close() throws java.io.IOException {
        for (Tablespace tablespace : tablespaces.values()) {
            tablespace.close();
        }
    }
}