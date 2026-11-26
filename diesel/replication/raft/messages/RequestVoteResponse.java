package diesel.replication.raft.messages;

/**
 * RequestVote RPC response (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Response to RequestVote RPC from other nodes to candidate.
 */
public class RequestVoteResponse extends RaftMessage {
    
    private final boolean voteGranted;
    
    public RequestVoteResponse(String nodeId, long term, boolean voteGranted) {
        super(nodeId, term, RaftMessageType.REQUEST_VOTE_RESPONSE);
        this.voteGranted = voteGranted;
    }
    
    /**
     * Get whether the vote was granted.
     */
    public boolean isVoteGranted() {
        return voteGranted;
    }
    
    @Override
    public String toString() {
        return String.format("%s{voteGranted=%b}", getType(), voteGranted);
    }
}