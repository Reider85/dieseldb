package diesel.replication.raft.messages;

import java.util.List;
import diesel.replication.raft.RaftLogEntry;

/**
 * AppendEntries RPC request (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Used by leaders to replicate log entries to followers and as heartbeats.
 */
public class AppendEntriesRequest extends RaftMessage {
    
    private final long prevLogIndex;
    private final long prevLogTerm;
    private final List<RaftLogEntry> entries;
    private final long commitIndex;
    
    public AppendEntriesRequest(String nodeId, long term, long prevLogIndex, 
                               long prevLogTerm, List<RaftLogEntry> entries, long commitIndex) {
        super(nodeId, term, RaftMessageType.APPEND_ENTRIES_REQUEST);
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries != null ? entries : List.of();
        this.commitIndex = commitIndex;
    }
    
    /**
     * Get the index of the log entry immediately preceding the new entries.
     */
    public long getPrevLogIndex() {
        return prevLogIndex;
    }
    
    /**
     * Get the term of the log entry immediately preceding the new entries.
     */
    public long getPrevLogTerm() {
        return prevLogTerm;
    }
    
    /**
     * Get the log entries to replicate (empty for heartbeat).
     */
    public List<RaftLogEntry> getEntries() {
        return entries;
    }
    
    /**
     * Get the leader's commit index.
     */
    public long getCommitIndex() {
        return commitIndex;
    }
    
    /**
     * Check if this is a heartbeat (no entries to replicate).
     */
    public boolean isHeartbeat() {
        return entries.isEmpty();
    }
    
    @Override
    public String toString() {
        return String.format("%s{prevLogIndex=%d, prevLogTerm=%d, entries=%d, commitIndex=%d}", 
            getType(), prevLogIndex, prevLogTerm, entries.size(), commitIndex);
    }
}