package diesel.replication.raft;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft state machine managing follower/candidate/leader states (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Manages Raft node state transitions with thread-safe operations and persistence.
 * Handles the three main Raft states: FOLLOWER, CANDIDATE, LEADER with proper
 * transitions based on timeouts and RPC messages.
 * 
 * <p>State transitions:
 * <ul>
 *   <li>FOLLOWER → CANDIDATE: When election timeout occurs</li>
 *   <li>CANDIDATE → FOLLOWER: When receiving heartbeat from higher term</li>
 *   <li>CANDIDATE → LEADER: When receiving majority votes</li>
 *   <li>LEADER → FOLLOWER: When receiving higher term</li>
 * </ul>
 */
public class RaftState implements AutoCloseable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftState.class);
    
    // State file
    private static final String STATE_FILE = "raft-state.dat";
    
    // Raft states
    public enum RaftNodeState {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }
    
    // Configuration
    private final RaftConfig config;
    private final Path raftDir;
    
    // State variables
    private final AtomicReference<RaftNodeState> currentState = new AtomicReference<>(RaftNodeState.FOLLOWER);
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicLong votedFor = new AtomicLong(-1);
    private final AtomicReference<String> leaderId = new AtomicReference<>(null);
    private final AtomicLong lastHeartbeatTime = new AtomicLong(0);
    
    // State change listeners
    private final StateChangeListener stateChangeListener;
    
    public RaftState(RaftConfig config, StateChangeListener stateChangeListener) {
        this.config = config;
        this.raftDir = config.getRaftDir();
        this.stateChangeListener = stateChangeListener;
        
        // Ensure raft directory exists
        try {
            Files.createDirectories(raftDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create raft directory: " + raftDir, e);
        }
    }
    
    /**
     * Start the state manager and load persisted state.
     */
    public void start() throws IOException {
        loadPersistedState();
        LOGGER.info("Raft state started with state: {}, term: {}, votedFor: {}", 
            currentState.get(), currentTerm.get(), votedFor.get());
    }
    
    /**
     * Stop the state manager.
     */
    @Override
    public void close() {
        persistState();
    }
    
    /**
     * Get current state.
     */
    public RaftNodeState getCurrentState() {
        return currentState.get();
    }
    
    /**
     * Get current term.
     */
    public long getCurrentTerm() {
        return currentTerm.get();
    }
    
    /**
     * Get voted-for candidate.
     */
    public long getVotedFor() {
        return votedFor.get();
    }
    
    /**
     * Get leader ID.
     */
    public String getLeaderId() {
        return leaderId.get();
    }
    
    /**
     * Get last heartbeat time.
     */
    public long getLastHeartbeatTime() {
        return lastHeartbeatTime.get();
    }
    
    /**
     * Become follower state.
     */
    public synchronized void becomeFollower(long newTerm, String newLeaderId) {
        RaftNodeState previousState = currentState.getAndSet(RaftNodeState.FOLLOWER);
        
        if (newTerm > currentTerm.get()) {
            currentTerm.set(newTerm);
            votedFor.set(-1); // Reset vote when term changes
        }
        
        leaderId.set(newLeaderId);
        lastHeartbeatTime.set(System.currentTimeMillis());
        
        LOGGER.info("Became follower for term {}, leader: {}", currentTerm.get(), newLeaderId);
        
        notifyStateChange(previousState, RaftNodeState.FOLLOWER);
    }
    
    /**
     * Become candidate state.
     */
    public synchronized void becomeCandidate() {
        RaftNodeState previousState = currentState.getAndSet(RaftNodeState.CANDIDATE);
        
        currentTerm.incrementAndGet(); // Start new election term
        votedFor.set(1L); // Vote for ourselves (nodeId as long)
        leaderId.set(null); // No leader during election
        lastHeartbeatTime.set(System.currentTimeMillis());
        
        LOGGER.info("Became candidate for term {}", currentTerm.get());
        
        notifyStateChange(previousState, RaftNodeState.CANDIDATE);
    }
    
    /**
     * Become leader state.
     */
    public synchronized void becomeLeader() {
        RaftNodeState previousState = currentState.getAndSet(RaftNodeState.LEADER);
        
        // Term remains the same, but we're now the leader
        leaderId.set(config.getNodeId());
        lastHeartbeatTime.set(System.currentTimeMillis());
        
        LOGGER.info("Became leader for term {}", currentTerm.get());
        
        notifyStateChange(previousState, RaftNodeState.LEADER);
    }
    
    /**
     * Update term and step down if necessary.
     */
    public synchronized void updateTerm(long newTerm) {
        if (newTerm > currentTerm.get()) {
            RaftNodeState previousState = currentState.get();
            currentState.set(RaftNodeState.FOLLOWER);
            
            currentTerm.set(newTerm);
            votedFor.set(-1); // Reset vote when term changes
            leaderId.set(null); // No leader until we get heartbeat
            
            LOGGER.info("Stepped down to follower for term {}", newTerm);
            
            notifyStateChange(previousState, RaftNodeState.FOLLOWER);
        }
    }
    
    /**
     * Record vote for candidate.
     */
    public synchronized void recordVote(long candidateId) {
        votedFor.set(candidateId);
        persistState();
    }
    
    /**
     * Update leader information from heartbeat.
     */
    public synchronized void updateLeaderInfo(String newLeaderId) {
        if (!newLeaderId.equals(leaderId.get())) {
            RaftNodeState previousState = currentState.get();
            
            if (currentState.get() == RaftNodeState.CANDIDATE && newLeaderId != null) {
                // Step down from candidate to follower
                currentState.set(RaftNodeState.FOLLOWER);
            }
            
            leaderId.set(newLeaderId);
            lastHeartbeatTime.set(System.currentTimeMillis());
            
            LOGGER.info("Updated leader info: {}", newLeaderId);
            
            if (previousState != currentState.get()) {
                notifyStateChange(previousState, currentState.get());
            }
        }
    }
    
    /**
     * Update last heartbeat time.
     */
    public synchronized void updateHeartbeatTime() {
        lastHeartbeatTime.set(System.currentTimeMillis());
    }
    
    /**
     * Check if state is stale (no heartbeat received).
     */
    public synchronized boolean isStale(long staleThresholdMs) {
        long currentTime = System.currentTimeMillis();
        return (currentTime - lastHeartbeatTime.get()) > staleThresholdMs;
    }
    
    /**
     * Check if this node is the leader.
     */
    public boolean isLeader() {
        return currentState.get() == RaftNodeState.LEADER;
    }
    
    /**
     * Check if this node is a candidate.
     */
    public boolean isCandidate() {
        return currentState.get() == RaftNodeState.CANDIDATE;
    }
    
    /**
     * Check if this node is a follower.
     */
    public boolean isFollower() {
        return currentState.get() == RaftNodeState.FOLLOWER;
    }
    
    /**
     * Get state information for debugging.
     */
    public synchronized StateInfo getStateInfo() {
        return new StateInfo(
            currentState.get(),
            currentTerm.get(),
            votedFor.get(),
            leaderId.get(),
            lastHeartbeatTime.get()
        );
    }
    
    /**
     * Load persisted state from disk.
     */
    private synchronized void loadPersistedState() throws IOException {
        Path stateFile = raftDir.resolve(STATE_FILE);
        
        if (Files.exists(stateFile)) {
            try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(stateFile)))) {
                String magic = dis.readUTF();
                if (!"RAFT-STATE".equals(magic)) {
                    throw new IOException("Invalid state file format");
                }
                
                RaftNodeState persistedState = RaftNodeState.values()[dis.readInt()];
                currentTerm.set(dis.readLong());
                votedFor.set(dis.readLong());
                leaderId.set(dis.readUTF());
                lastHeartbeatTime.set(dis.readLong());
                
                currentState.set(persistedState);
                
                LOGGER.info("Loaded persisted state: {}, term: {}, votedFor: {}, leader: {}", 
                    currentState.get(), currentTerm.get(), votedFor.get(), leaderId.get());
            }
        } else {
            LOGGER.info("No existing state file found, starting with fresh state");
        }
    }
    
    /**
     * Persist current state to disk.
     */
    private synchronized void persistState() {
        Path stateFile = raftDir.resolve(STATE_FILE);
        
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(stateFile)))) {
            dos.writeUTF("RAFT-STATE");
            dos.writeInt(currentState.get().ordinal());
            dos.writeLong(currentTerm.get());
            dos.writeLong(votedFor.get());
            dos.writeUTF(leaderId.get() != null ? leaderId.get() : "");
            dos.writeLong(lastHeartbeatTime.get());
        } catch (IOException e) {
            LOGGER.error("Failed to persist Raft state", e);
        }
    }
    
    /**
     * Notify state change listener.
     */
    private void notifyStateChange(RaftNodeState previousState, RaftNodeState newState) {
        if (stateChangeListener != null) {
            try {
                stateChangeListener.onStateChange(previousState, newState, currentTerm.get());
            } catch (Exception e) {
                LOGGER.error("Error notifying state change listener", e);
            }
        }
    }
    
    /**
     * State change listener interface.
     */
    public interface StateChangeListener {
        void onStateChange(RaftNodeState previousState, RaftNodeState newState, long term);
    }
    
    /**
     * State information for debugging.
     */
    public static class StateInfo {
        private final RaftNodeState currentState;
        private final long currentTerm;
        private final long votedFor;
        private final String leaderId;
        private final long lastHeartbeatTime;
        
        public StateInfo(RaftNodeState currentState, long currentTerm, long votedFor, 
                        String leaderId, long lastHeartbeatTime) {
            this.currentState = currentState;
            this.currentTerm = currentTerm;
            this.votedFor = votedFor;
            this.leaderId = leaderId;
            this.lastHeartbeatTime = lastHeartbeatTime;
        }
        
        // Getters
        public RaftNodeState getCurrentState() { return currentState; }
        public long getCurrentTerm() { return currentTerm; }
        public long getVotedFor() { return votedFor; }
        public String getLeaderId() { return leaderId; }
        public long getLastHeartbeatTime() { return lastHeartbeatTime; }
        
        @Override
        public String toString() {
            return String.format("StateInfo{state=%s, term=%d, votedFor=%d, leader=%s, lastHeartbeat=%d}", 
                currentState, currentTerm, votedFor, leaderId, lastHeartbeatTime);
        }
    }
}