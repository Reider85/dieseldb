package diesel.replication.raft.messages;

import java.util.Objects;

/**
 * Base Raft message interface (prompt4.md #62, R3-016 step 5/5).
 */
public abstract class RaftMessage {
    
    protected final String nodeId;
    protected final long term;
    protected final RaftMessageType type;
    
    protected RaftMessage(String nodeId, long term, RaftMessageType type) {
        this.nodeId = Objects.requireNonNull(nodeId);
        this.term = term;
        this.type = Objects.requireNonNull(type);
    }
    
    /**
     * Get the ID of the node that sent this message.
     */
    public String getNodeId() {
        return nodeId;
    }
    
    /**
     * Get the term associated with this message.
     */
    public long getTerm() {
        return term;
    }
    
    /**
     * Get the type of this message.
     */
    public RaftMessageType getType() {
        return type;
    }
    
    /**
     * Get the string representation of this message.
     */
    @Override
    public String toString() {
        return String.format("%s{nodeId=%s, term=%d}", type, nodeId, term);
    }
}