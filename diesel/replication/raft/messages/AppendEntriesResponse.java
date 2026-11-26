package diesel.replication.raft.messages;

/**
 * AppendEntries RPC response (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Response to AppendEntries RPC from follower to leader.
 */
public class AppendEntriesResponse extends RaftMessage {
    
    private final boolean success;
    private final long lastMatchIndex;
    
    public AppendEntriesResponse(String nodeId, long term, boolean success, long lastMatchIndex) {
        super(nodeId, term, RaftMessageType.APPEND_ENTRIES_RESPONSE);
        this.success = success;
        this.lastMatchIndex = lastMatchIndex;
    }
    
    /**
     * Get whether the AppendEntries was successful.
     */
    public boolean isSuccess() {
        return success;
    }
    
    /**
     * Get the index of the last successfully matched entry.
     */
    public long getLastMatchIndex() {
        return lastMatchIndex;
    }
    
    @Override
    public String toString() {
        return String.format("%s{success=%b, lastMatchIndex=%d}", getType(), success, lastMatchIndex);
    }
}