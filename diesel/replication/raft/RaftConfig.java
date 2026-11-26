package diesel.replication.raft;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Raft configuration management (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Provides centralized configuration for Raft nodes including cluster settings,
 * timing parameters, and storage options. Supports loading from configuration
 * properties and provides default values.
 */
public class RaftConfig {
    
    // Configuration keys
    public static final String RAFT_ENABLED = "raft.enabled";
    public static final String RAFT_NODE_ID = "raft.node.id";
    public static final String RAFT_CLUSTER_NODES = "raft.cluster.nodes";
    public static final String RAFT_ELECTION_TIMEOUT_MS = "raft.election.timeout.ms";
    public static final String RAFT_HEARTBEAT_INTERVAL_MS = "raft.heartbeat.interval.ms";
    public static final String RAFT_DATA_DIR = "raft.data.dir";
    public static final String RAFT_LOG_MAX_SEGMENTS = "raft.log.max.segments";
    public static final String RAFT_LOG_MAX_SEGMENT_SIZE = "raft.log.max.segment.size";
    public static final String RAFT_LOG_MAX_BATCH_SIZE = "raft.log.max.batch.size";
    public static final String RAFT_QUORUM_REQUIRED = "raft.quorum.required";
    
    // Default values
    public static final boolean DEFAULT_RAFT_ENABLED = false;
    public static final String DEFAULT_RAFT_NODE_ID = "node-1";
    public static final String DEFAULT_RAFT_CLUSTER_NODES = "node-1";
    public static final int DEFAULT_RAFT_ELECTION_TIMEOUT_MS = 3000;
    public static final int DEFAULT_RAFT_HEARTBEAT_INTERVAL_MS = 1000;
    public static final String DEFAULT_RAFT_DATA_DIR = "./data/raft";
    public static final int DEFAULT_RAFT_LOG_MAX_SEGMENTS = 10;
    public static final int DEFAULT_RAFT_LOG_MAX_SEGMENT_SIZE = 64 * 1024 * 1024; // 64MB
    public static final int DEFAULT_RAFT_LOG_MAX_BATCH_SIZE = 100;
    public static final boolean DEFAULT_RAFT_QUORUM_REQUIRED = true;
    
    // Configuration values
    private final boolean enabled;
    private final String nodeId;
    private final List<String> clusterNodes;
    private final int electionTimeoutMs;
    private final int heartbeatIntervalMs;
    private final Path raftDir;
    private final int maxLogSegments;
    private final int maxSegmentSize;
    private final int maxBatchSize;
    private final boolean quorumRequired;
    
    /**
     * Create Raft configuration from properties.
     */
    public static RaftConfig fromConfig() {
        // This would normally load from a configuration system
        // For now, use defaults with some overrides for testing
        return new RaftConfig(
            true, // enabled
            "node-1", // nodeId
            List.of("node-1", "node-2", "node-3"), // clusterNodes
            3000, // electionTimeoutMs
            1000, // heartbeatIntervalMs
            Paths.get("./data/raft"), // raftDir
            10, // maxLogSegments
            64 * 1024 * 1024, // maxSegmentSize
            100, // maxBatchSize
            true // quorumRequired
        );
    }
    
    /**
     * Create Raft configuration with custom values.
     */
    public RaftConfig(boolean enabled, String nodeId, List<String> clusterNodes, 
                     int electionTimeoutMs, int heartbeatIntervalMs, Path raftDir,
                     int maxLogSegments, int maxSegmentSize, int maxBatchSize,
                     boolean quorumRequired) {
        
        // Validate parameters
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalArgumentException("Node ID cannot be null or empty");
        }
        
        if (clusterNodes == null || clusterNodes.isEmpty()) {
            throw new IllegalArgumentException("Cluster nodes cannot be null or empty");
        }
        
        if (!clusterNodes.contains(nodeId)) {
            throw new IllegalArgumentException("Node ID must be in cluster nodes list");
        }
        
        if (electionTimeoutMs <= 0) {
            throw new IllegalArgumentException("Election timeout must be positive");
        }
        
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("Heartbeat interval must be positive");
        }
        
        if (raftDir == null) {
            throw new IllegalArgumentException("Raft directory cannot be null");
        }
        
        if (maxLogSegments <= 0) {
            throw new IllegalArgumentException("Max log segments must be positive");
        }
        
        if (maxSegmentSize <= 0) {
            throw new IllegalArgumentException("Max segment size must be positive");
        }
        
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("Max batch size must be positive");
        }
        
        this.enabled = enabled;
        this.nodeId = nodeId;
        this.clusterNodes = new ArrayList<>(clusterNodes);
        this.electionTimeoutMs = electionTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.raftDir = raftDir;
        this.maxLogSegments = maxLogSegments;
        this.maxSegmentSize = maxSegmentSize;
        this.maxBatchSize = maxBatchSize;
        this.quorumRequired = quorumRequired;
    }
    
    /**
     * Check if Raft is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }
    
    /**
     * Get this node's ID.
     */
    public String getNodeId() {
        return nodeId;
    }
    
    /**
     * Get cluster node IDs.
     */
    public List<String> getClusterNodes() {
        return new ArrayList<>(clusterNodes);
    }
    
    /**
     * Get election timeout in milliseconds.
     */
    public int getElectionTimeoutMs() {
        return electionTimeoutMs;
    }
    
    /**
     * Get heartbeat interval in milliseconds.
     */
    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }
    
    /**
     * Get Raft data directory.
     */
    public Path getRaftDir() {
        return raftDir;
    }
    
    /**
     * Get maximum number of log segments.
     */
    public int getMaxSegments() {
        return maxLogSegments;
    }
    
    /**
     * Get maximum log segment size in bytes.
     */
    public int getMaxSegmentSize() {
        return maxSegmentSize;
    }
    
    /**
     * Get maximum batch size for log replication.
     */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }
    
    /**
     * Check if quorum is required for operations.
     */
    public boolean isQuorumRequired() {
        return quorumRequired;
    }
    
    /**
     * Calculate quorum size.
     */
    public int getQuorumSize() {
        return (clusterNodes.size() / 2) + 1;
    }
    
    /**
     * Check if this node can operate without quorum.
     */
    public boolean canOperateWithoutQuorum() {
        return !quorumRequired || clusterNodes.size() == 1;
    }
    
    /**
     * Create a test configuration for a single node.
     */
    public static RaftConfig createSingleNodeConfig() {
        return new RaftConfig(
            true,
            "node-1",
            List.of("node-1"),
            3000,
            1000,
            Paths.get("./data/raft"),
            5,
            32 * 1024 * 1024, // 32MB
            50,
            false // Single node doesn't need quorum
        );
    }
    
    /**
     * Create a test configuration for a 3-node cluster.
     */
    public static RaftConfig createThreeNodeConfig() {
        return new RaftConfig(
            true,
            "node-1",
            List.of("node-1", "node-2", "node-3"),
            3000,
            1000,
            Paths.get("./data/raft"),
            10,
            64 * 1024 * 1024, // 64MB
            100,
            true // 3-node cluster needs quorum
        );
    }
    
    /**
     * Create a test configuration for a 5-node cluster.
     */
    public static RaftConfig createFiveNodeConfig() {
        return new RaftConfig(
            true,
            "node-1",
            List.of("node-1", "node-2", "node-3", "node-4", "node-5"),
            3000,
            1000,
            Paths.get("./data/raft"),
            15,
            64 * 1024 * 1024, // 64MB
            100,
            true // 5-node cluster needs quorum
        );
    }
    
    /**
     * Validate configuration.
     */
    public void validate() {
        if (!enabled) {
            return; // No validation needed if disabled
        }
        
        // Check if node ID is valid
        if (nodeId == null || nodeId.trim().isEmpty()) {
            throw new IllegalStateException("Raft enabled but node ID is not set");
        }
        
        // Check if cluster is properly configured
        if (clusterNodes == null || clusterNodes.isEmpty()) {
            throw new IllegalStateException("Raft enabled but cluster nodes not configured");
        }
        
        // Check if this node is in the cluster
        if (!clusterNodes.contains(nodeId)) {
            throw new IllegalStateException("Raft node ID " + nodeId + " not found in cluster nodes");
        }
        
        // Check timing parameters
        if (electionTimeoutMs <= heartbeatIntervalMs) {
            throw new IllegalStateException("Election timeout must be greater than heartbeat interval");
        }
        
        // Check quorum requirements
        int quorumSize = getQuorumSize();
        if (quorumRequired && clusterNodes.size() < 3) {
            throw new IllegalStateException("Quorum requires at least 3 nodes");
        }
        
        if (quorumRequired && quorumSize > clusterNodes.size()) {
            throw new IllegalStateException("Quorum size " + quorumSize + " exceeds cluster size " + clusterNodes.size());
        }
    }
    
    /**
     * Get configuration summary for logging.
     */
    public String getSummary() {
        return String.format("RaftConfig{enabled=%s, nodeId=%s, clusterNodes=%s, " +
                           "electionTimeoutMs=%d, heartbeatIntervalMs=%d, quorumRequired=%s, " +
                           "quorumSize=%d}",
            enabled, nodeId, clusterNodes, electionTimeoutMs, heartbeatIntervalMs, 
            quorumRequired, getQuorumSize());
    }
}