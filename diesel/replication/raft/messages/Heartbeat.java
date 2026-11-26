package diesel.replication.raft.messages;

/**
 * Heartbeat message (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Used by leaders to maintain authority and prevent election timeouts.
 */
public class Heartbeat extends RaftMessage {
    
    public Heartbeat(String nodeId, long term) {
        super(nodeId, term, RaftMessageType.HEARTBEAT);
    }
    
    @Override
    public String toString() {
        return String.format("%s", getType());
    }
}