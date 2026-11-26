package diesel.replication.raft;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import diesel.replication.raft.messages.*;
import diesel.replication.raft.RaftNetwork;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Base test class for Raft functionality (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Provides common setup, teardown, and utilities for Raft testing.
 * Handles temporary directory management, cluster setup, and test isolation.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class BaseRaftTest {
    
    protected static final String TEST_DIR_PREFIX = "raft-test-";
    protected static final int DEFAULT_ELECTION_TIMEOUT = 1000; // 1 second for testing
    protected static final int DEFAULT_HEARTBEAT_INTERVAL = 200; // 200ms for testing
    
    protected List<Path> tempDirs = new ArrayList<>();
    protected List<RaftNode> raftNodes = new ArrayList<>();
    protected List<RaftConfig> raftConfigs = new ArrayList<>();
    
    @BeforeEach
    void setUp() throws Exception {
        // Create temporary directories for each node
        for (int i = 0; i < 3; i++) {
            Path tempDir = createTempDirectory();
            tempDirs.add(tempDir);
            
            // Create Raft configuration
            RaftConfig config = RaftConfig.createThreeNodeConfig();
            config = new RaftConfig(
                true,
                "node-" + (i + 1),
                List.of("node-1", "node-2", "node-3"),
                DEFAULT_ELECTION_TIMEOUT,
                DEFAULT_HEARTBEAT_INTERVAL,
                tempDir,
                5,
                1024 * 1024, // 1MB segments for testing
                10,
                true
            );
            
            raftConfigs.add(config);
        }
    }
    
    @AfterEach
    void tearDown() throws Exception {
        // Shutdown all Raft nodes
        for (RaftNode node : raftNodes) {
            if (node != null) {
                try {
                    node.close();
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
        }
        raftNodes.clear();
        
        // Delete temporary directories
        for (Path tempDir : tempDirs) {
            try {
                deleteDirectory(tempDir);
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        tempDirs.clear();
        
        raftConfigs.clear();
    }
    
    /**
     * Create a temporary directory for testing.
     */
    protected Path createTempDirectory() throws IOException {
        Path tempDir = Files.createTempDirectory(TEST_DIR_PREFIX);
        return tempDir;
    }
    
    /**
     * Delete a directory and all its contents.
     */
    protected void deleteDirectory(Path directory) throws IOException {
        Files.walk(directory)
            .sorted((a, b) -> -a.compareTo(b)) // Sort in reverse order to delete files first
            .forEach(path -> {
                try {
                    Files.delete(path);
                } catch (IOException e) {
                    // Ignore delete errors
                }
            });
    }
    
    /**
     * Create a Raft node for testing.
     */
    protected RaftNode createRaftNode(int nodeIndex) throws IOException {
        RaftConfig config = raftConfigs.get(nodeIndex);
        
        // Create mock network for testing
        MockRaftNetwork network = new MockRaftNetwork(config.getNodeId(), config.getClusterNodes());
        
        // Create Raft log
        RaftLog raftLog = new RaftLog(config);
        raftLog.start();
        
        // Create Raft node
        RaftNode node = new RaftNode(config, raftLog, network);
        raftNodes.add(node);
        
        return node;
    }
    
    /**
     * Start all Raft nodes.
     */
    protected void startAllNodes() throws IOException {
        for (int i = 0; i < 3; i++) {
            RaftNode node = createRaftNode(i);
            node.start();
        }
        
        // Wait for cluster to stabilize
        try {
            waitForClusterStabilization(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for cluster stabilization", e);
        }
    }
    
    /**
     * Wait for cluster to stabilize (elect a leader).
     */
    protected void waitForClusterStabilization(long timeoutMs) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        RaftNode leader = null;
        
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            // Check if we have a leader
            for (RaftNode node : raftNodes) {
                if (node.isLeader()) {
                    leader = node;
                    break;
                }
            }
            
            if (leader != null) {
                // Leader found, wait a bit more for stability
                Thread.sleep(500);
                return;
            }
            
            Thread.sleep(100);
        }
        
        if (leader == null) {
            throw new RuntimeException("Cluster did not stabilize within " + timeoutMs + "ms");
        }
    }
    
    /**
     * Get the current leader.
     */
    protected RaftNode getLeader() {
        for (RaftNode node : raftNodes) {
            if (node.isLeader()) {
                return node;
            }
        }
        return null;
    }
    
    /**
     * Get followers (non-leader nodes).
     */
    protected List<RaftNode> getFollowers() {
        List<RaftNode> followers = new ArrayList<>();
        for (RaftNode node : raftNodes) {
            if (!node.isLeader()) {
                followers.add(node);
            }
        }
        return followers;
    }
    
    /**
     * Simulate a network partition.
     */
    protected void simulateNetworkPartition(List<RaftNode> partition1, List<RaftNode> partition2) {
        // In a real implementation, this would manipulate the network layer
        // For testing, we'll just mark the nodes as partitioned
        for (RaftNode node : partition1) {
            // Mark as partitioned
        }
        for (RaftNode node : partition2) {
            // Mark as partitioned
        }
    }
    
    /**
     * Simulate node failure.
     */
    protected void simulateNodeFailure(RaftNode node) {
        try {
            node.close();
        } catch (Exception e) {
            // Ignore failure simulation errors
        }
        raftNodes.remove(node);
    }
    
    /**
     * Mock Raft network for testing.
     */
    protected static class MockRaftNetwork implements RaftNetwork {
        
        private final String nodeId;
        private final List<String> clusterNodes;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final List<RaftMessage> receivedMessages = new ArrayList<>();
        
        public MockRaftNetwork(String nodeId, List<String> clusterNodes) {
            this.nodeId = nodeId;
            this.clusterNodes = new ArrayList<>(clusterNodes);
        }
        
        @Override
        public void start(java.util.function.Consumer<RaftMessage> messageHandler) throws IOException {
            running.set(true);
        }
        
        @Override
        public void stop() {
            running.set(false);
        }
        
        @Override
        public void sendAppendEntries(String targetNodeId, AppendEntriesRequest request) {
            // Simulate network delay
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        @Override
        public void sendAppendEntriesResponse(String targetNodeId, AppendEntriesResponse response) {
            // Simulate network delay
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        @Override
        public void sendRequestVote(String targetNodeId, RequestVoteRequest request) {
            // Simulate network delay
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        @Override
        public void sendRequestVoteResponse(String targetNodeId, RequestVoteResponse response) {
            // Simulate network delay
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        @Override
        public void sendHeartbeat(String targetNodeId, Heartbeat heartbeat) {
            // Simulate network delay
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        @Override
        public boolean isNodeReachable(String nodeId) {
            return clusterNodes.contains(nodeId) && !nodeId.equals(this.nodeId);
        }
        
        @Override
        public int getClusterSize() {
            return clusterNodes.size();
        }
        
        @Override
        public String getNodeId() {
            return nodeId;
        }
        
        public boolean isRunning() {
            return running.get();
        }
        
        public List<RaftMessage> getReceivedMessages() {
            return new ArrayList<>(receivedMessages);
        }
    }
}