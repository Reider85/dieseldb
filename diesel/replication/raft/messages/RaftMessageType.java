package diesel.replication.raft.messages;

/**
 * Raft message types (prompt4.md #62, R3-016 step 5/5).
 */
public enum RaftMessageType {
    APPEND_ENTRIES_REQUEST("AppendEntriesRequest"),
    APPEND_ENTRIES_RESPONSE("AppendEntriesResponse"),
    REQUEST_VOTE_REQUEST("RequestVoteRequest"),
    REQUEST_VOTE_RESPONSE("RequestVoteResponse"),
    HEARTBEAT("Heartbeat"),
    CONFIG_CHANGE_REQUEST("ConfigChangeRequest"),
    CONFIG_CHANGE_RESPONSE("ConfigChangeResponse");
    
    private final String displayName;
    
    RaftMessageType(String displayName) {
        this.displayName = displayName;
    }
    
    public String getDisplayName() {
        return displayName;
    }
}