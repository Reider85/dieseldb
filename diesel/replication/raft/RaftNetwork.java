package diesel.replication.raft;

import java.io.IOException;
import java.util.function.Consumer;
import diesel.replication.raft.messages.*;

/**
 * Raft network interface for node communication (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Handles communication between Raft nodes, including sending and receiving
 * RPC messages and managing network connections.
 */
public interface RaftNetwork {
    
    /**
     * Start the network listener.
     */
    void start(Consumer<RaftMessage> messageHandler) throws IOException;
    
    /**
     * Stop the network listener.
     */
    void stop();
    
    /**
     * Send AppendEntries RPC to another node.
     */
    void sendAppendEntries(String targetNodeId, AppendEntriesRequest request);
    
    /**
     * Send AppendEntries response to another node.
     */
    void sendAppendEntriesResponse(String targetNodeId, AppendEntriesResponse response);
    
    /**
     * Send RequestVote RPC to another node.
     */
    void sendRequestVote(String targetNodeId, RequestVoteRequest request);
    
    /**
     * Send RequestVote response to another node.
     */
    void sendRequestVoteResponse(String targetNodeId, RequestVoteResponse response);
    
    /**
     * Send heartbeat to another node.
     */
    void sendHeartbeat(String targetNodeId, Heartbeat heartbeat);
    
    /**
     * Check if a node is reachable.
     */
    boolean isNodeReachable(String nodeId);
    
    /**
     * Get the current cluster size.
     */
    int getClusterSize();
    
    /**
     * Get the current node ID.
     */
    String getNodeId();
}