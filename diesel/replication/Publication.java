package diesel.replication;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a publication that defines which tables are replicated.
 * Publications can include multiple tables and support filtering.
 */
public class Publication {
    
    private final String name;
    private final Set<String> tables;
    private final boolean includeAllTables;
    private final ConflictResolutionStrategy conflictResolution;
    private final long createdTime;
    
    public Publication(String name, Set<String> tables, ConflictResolutionStrategy conflictResolution) {
        this.name = name;
        this.tables = new HashSet<>(tables);
        this.includeAllTables = false;
        this.conflictResolution = conflictResolution;
        this.createdTime = System.currentTimeMillis();
    }
    
    public Publication(String name, ConflictResolutionStrategy conflictResolution) {
        this.name = name;
        this.tables = new HashSet<>();
        this.includeAllTables = true;
        this.conflictResolution = conflictResolution;
        this.createdTime = System.currentTimeMillis();
    }
    
    public String getName() {
        return name;
    }
    
    public Set<String> getTables() {
        return new HashSet<>(tables);
    }
    
    public boolean isIncludeAllTables() {
        return includeAllTables;
    }
    
    public ConflictResolutionStrategy getConflictResolution() {
        return conflictResolution;
    }
    
    public long getCreatedTime() {
        return createdTime;
    }
    
    public boolean containsTable(String tableName) {
        if (includeAllTables) {
            return true;
        }
        return tables.contains(tableName);
    }
    
    public void addTable(String tableName) {
        tables.add(tableName);
    }
    
    public void removeTable(String tableName) {
        tables.remove(tableName);
    }
    
    public boolean isEmpty() {
        return tables.isEmpty() && !includeAllTables;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Publication that = (Publication) o;
        return Objects.equals(name, that.name);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
    
    @Override
    public String toString() {
        return String.format("Publication{name='%s', includeAllTables=%s, tables=%s, conflictResolution=%s, createdTime=%d}",
                           name, includeAllTables, tables, conflictResolution, createdTime);
    }
    
    /**
     * Conflict resolution strategies for logical replication.
     */
    public enum ConflictResolutionStrategy {
        ERROR,    // Throw error on conflict (default)
        SKIP,     // Skip conflicting changes
        REPLACE   // Replace with master data
    }
}