package diesel.replication.raft;

/**
 * Raft log entry for leadership and configuration changes (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Represents a single entry in the Raft log with term-based consistency guarantees.
 * Each entry contains the term it was created in, its index, and the command/data
 * being replicated across the cluster.
 */
public class RaftLogEntry {
    
    private final long term;
    private final long index;
    private final String command;
    
    /**
     * Create a new Raft log entry.
     */
    public RaftLogEntry(long term, long index, String command) {
        if (term < 0) {
            throw new IllegalArgumentException("Term cannot be negative");
        }
        if (index < 0) {
            throw new IllegalArgumentException("Index cannot be negative");
        }
        if (command == null) {
            throw new IllegalArgumentException("Command cannot be null");
        }
        
        this.term = term;
        this.index = index;
        this.command = command;
    }
    
    /**
     * Get the term in which this entry was created.
     */
    public long getTerm() {
        return term;
    }
    
    /**
     * Get the index of this entry.
     */
    public long getIndex() {
        return index;
    }
    
    /**
     * Get the command/data being replicated.
     */
    public String getCommand() {
        return command;
    }
    
    /**
     * Check if this entry is more recent than another entry.
     */
    public boolean isMoreRecentThan(RaftLogEntry other) {
        if (other == null) {
            return true;
        }
        return this.term > other.term || 
               (this.term == other.term && this.index > other.index);
    }
    
    /**
     * Check if this entry is from the same term as another entry.
     */
    public boolean isSameTerm(RaftLogEntry other) {
        return other != null && this.term == other.term;
    }
    
    /**
     * Check if this entry is from a newer term than another entry.
     */
    public boolean isNewerTerm(RaftLogEntry other) {
        return other != null && this.term > other.term;
    }
    
    /**
     * Create a log entry for a configuration change.
     */
    public static RaftLogEntry createConfigChange(long term, long index, String configChange) {
        return new RaftLogEntry(term, index, "CONFIG_CHANGE:" + configChange);
    }
    
    /**
     * Create a log entry for a write operation.
     */
    public static RaftLogEntry createWriteOperation(long term, long index, String sql) {
        return new RaftLogEntry(term, index, "WRITE:" + sql);
    }
    
    /**
     * Create a log entry for a no-op entry.
     */
    public static RaftLogEntry createNoOp(long term, long index) {
        return new RaftLogEntry(term, index, "NO_OP");
    }
    
    /**
     * Check if this is a configuration change entry.
     */
    public boolean isConfigChange() {
        return command.startsWith("CONFIG_CHANGE:");
    }
    
    /**
     * Check if this is a write operation entry.
     */
    public boolean isWriteOperation() {
        return command.startsWith("WRITE:");
    }
    
    /**
     * Check if this is a no-op entry.
     */
    public boolean isNoOp() {
        return "NO_OP".equals(command);
    }
    
    /**
     * Get the configuration change data (if this is a config change entry).
     */
    public String getConfigChangeData() {
        if (isConfigChange()) {
            return command.substring("CONFIG_CHANGE:".length());
        }
        throw new IllegalStateException("Entry is not a configuration change");
    }
    
    /**
     * Get the SQL statement (if this is a write operation entry).
     */
    public String getSqlStatement() {
        if (isWriteOperation()) {
            return command.substring("WRITE:".length());
        }
        throw new IllegalStateException("Entry is not a write operation");
    }
    
    @Override
    public String toString() {
        return String.format("RaftLogEntry{term=%d, index=%d, command='%s'}", term, index, command);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RaftLogEntry other = (RaftLogEntry) obj;
        return term == other.term && index == other.index && command.equals(other.command);
    }
    
    @Override
    public int hashCode() {
        return Long.hashCode(term) * 31 + Long.hashCode(index) * 31 + command.hashCode();
    }
}