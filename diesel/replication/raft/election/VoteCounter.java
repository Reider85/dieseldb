package diesel.replication.raft.election;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vote counter for Raft leader election (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Tracks votes received during an election and determines if a majority
 * has been achieved to become the leader.
 */
public class VoteCounter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(VoteCounter.class);
    
    // Configuration
    private final int majorityThreshold;
    private String nodeId;
    
    // State
    private final Set<String> votedNodes = ConcurrentHashMap.newKeySet();
    private final AtomicInteger voteCount = new AtomicInteger(0);
    private final AtomicBoolean electionActive = new AtomicBoolean(false);
    
    /**
     * Create a new vote counter.
     */
    public VoteCounter(int clusterSize) {
        this.majorityThreshold = (clusterSize / 2) + 1;
        this.nodeId = "unknown"; // Will be set by caller
    }
    
    /**
     * Start a new election.
     */
    public void startElection(String nodeId) {
        if (!electionActive.compareAndSet(false, true)) {
            LOGGER.warn("Election already active");
            return;
        }
        
        this.nodeId = nodeId;
        votedNodes.clear();
        voteCount.set(0);
        
        LOGGER.info("Starting election for node {}, majority threshold: {}", nodeId, majorityThreshold);
    }
    
    /**
     * Reset the vote counter.
     */
    public void reset() {
        electionActive.set(false);
        votedNodes.clear();
        voteCount.set(0);
        LOGGER.debug("Vote counter reset");
    }
    
    /**
     * Record a vote from a node.
     */
    public boolean receiveVote(String voterNodeId) {
        if (!electionActive.get()) {
            LOGGER.warn("Received vote from {} but election is not active", voterNodeId);
            return false;
        }
        
        if (votedNodes.contains(voterNodeId)) {
            LOGGER.debug("Already received vote from {}", voterNodeId);
            return false;
        }
        
        boolean added = votedNodes.add(voterNodeId);
        if (added) {
            int newCount = voteCount.incrementAndGet();
            LOGGER.info("Received vote from {}, total votes: {}", voterNodeId, newCount);
            
            // Check if we have majority
            if (newCount >= majorityThreshold) {
                LOGGER.info("Achieved majority votes: {}/{}", newCount, majorityThreshold);
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Check if we have achieved majority votes.
     */
    public boolean hasMajority() {
        return voteCount.get() >= majorityThreshold;
    }
    
    /**
     * Get the current vote count.
     */
    public int getVoteCount() {
        return voteCount.get();
    }
    
    /**
     * Get the majority threshold.
     */
    public int getMajorityThreshold() {
        return majorityThreshold;
    }
    
    /**
     * Get the nodes that have voted.
     */
    public Set<String> getVotedNodes() {
        return Set.copyOf(votedNodes);
    }
    
    /**
     * Check if election is active.
     */
    public boolean isElectionActive() {
        return electionActive.get();
    }
    
    /**
     * Get election statistics.
     */
    public ElectionStats getStats() {
        return new ElectionStats(
            nodeId,
            voteCount.get(),
            majorityThreshold,
            votedNodes.size(),
            electionActive.get(),
            hasMajority()
        );
    }
    
    /**
     * Election statistics.
     */
    public static class ElectionStats {
        private final String nodeId;
        private final int voteCount;
        private final int majorityThreshold;
        private final int votedNodeCount;
        private final boolean electionActive;
        private final boolean hasMajority;
        
        public ElectionStats(String nodeId, int voteCount, int majorityThreshold, 
                           int votedNodeCount, boolean electionActive, boolean hasMajority) {
            this.nodeId = nodeId;
            this.voteCount = voteCount;
            this.majorityThreshold = majorityThreshold;
            this.votedNodeCount = votedNodeCount;
            this.electionActive = electionActive;
            this.hasMajority = hasMajority;
        }
        
        // Getters
        public String getNodeId() { return nodeId; }
        public int getVoteCount() { return voteCount; }
        public int getMajorityThreshold() { return majorityThreshold; }
        public int getVotedNodeCount() { return votedNodeCount; }
        public boolean isElectionActive() { return electionActive; }
        public boolean hasMajority() { return hasMajority; }
        
        @Override
        public String toString() {
            return String.format("ElectionStats{node=%s, votes=%d/%d, votedNodes=%d, active=%s, majority=%s}", 
                nodeId, voteCount, majorityThreshold, votedNodeCount, electionActive, hasMajority);
        }
    }
}