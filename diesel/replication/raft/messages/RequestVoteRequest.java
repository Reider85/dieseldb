package diesel.replication.raft.messages;

/**
 * RequestVote RPC request (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Used by candidates to request votes from other nodes.
 */
public class RequestVoteRequest extends RaftMessage {
    
    private final long candidateId;
    private final long lastLogIndex;
    private final long lastLogTerm;
    
    public RequestVoteRequest(String nodeId, long term, long candidateId, 
                             long lastLogIndex, long lastLogTerm) {
        super(nodeId, term, RaftMessageType.REQUEST_VOTE_REQUEST);
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }
    
    /**
     * Get the candidate's ID.
     */
    public long getCandidateId() {
        return candidateId;
    }
    
    /**
     * Get the index of the candidate's last log entry.
     */
    public long getLastLogIndex() {
        return lastLogIndex;
    }
    
    /**
     * Get the term of the candidate's last log entry.
     */
    public long getLastLogTerm() {
        return lastLogTerm;
    }
    
    @Override
    public String toString() {
        return String.format("%s{candidateId=%d, lastLogIndex=%d, lastLogTerm=%d}", 
            getType(), candidateId, lastLogIndex, lastLogTerm);
    }
}