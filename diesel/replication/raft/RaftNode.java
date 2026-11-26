package diesel.replication.raft;

import diesel.replication.raft.messages.*;
import diesel.replication.raft.election.ElectionTimeout;
import diesel.replication.raft.election.VoteCounter;
import diesel.wal.WALEntry;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Raft consensus node implementation (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Implements the Raft consensus algorithm with leader election and log replication.
 * Manages three states: FOLLOWER, CANDIDATE, LEADER with transitions based on
 * timeouts and RPC messages.
 * 
 * <p>Key features:
 * <ul>
 *   <li>Leader election with randomized election timeouts</li>
 *   <li>Log replication with consistency guarantees</li>
 *   <li>Quorum-based write coordination</li>
 *   <li>Automatic failover detection and promotion</li>
 *   <li>Split-brain prevention</li>
 * </ul>
 */
public class RaftNode implements AutoCloseable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);
    
    // Raft states
    public enum RaftNodeState {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }
    
    // Configuration
    private final RaftConfig config;
    private final RaftLog raftLog;
    private final RaftNetwork network;
    
    // State variables
    private final AtomicReference<RaftNodeState> state = new AtomicReference<>(RaftNodeState.FOLLOWER);
    private final AtomicLong currentTerm = new AtomicLong(0);
    private final AtomicLong votedFor = new AtomicLong(-1);
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);
    private final AtomicReference<String> leaderId = new AtomicReference<>(null);
    
    // Leader-specific state
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();
    
    // Election management
    private final ElectionTimeout electionTimeout;
    private final VoteCounter voteCounter;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    
    // Health monitoring
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean hasQuorum = new AtomicBoolean(false);
    
    // Timers
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTimer;
    
    public RaftNode(RaftConfig config, RaftLog raftLog, RaftNetwork network) {
        this.config = config;
        this.raftLog = raftLog;
        this.network = network;
        this.electionTimeout = new ElectionTimeout(config);
        this.voteCounter = new VoteCounter(config.getClusterNodes().size());
        
        // Initialize cluster node tracking
        for (String nodeId : config.getClusterNodes()) {
            nextIndex.put(nodeId, (long) raftLog.getLastIndex() + 1);
            matchIndex.put(nodeId, 0L);
        }
    }
    
    /**
     * Start the Raft node and begin the election process.
     */
    public void start() throws IOException {
        if (running.getAndSet(true)) {
            return;
        }
        
        LOGGER.info("Starting Raft node: {}", config.getNodeId());
        
        // Load persisted state
        loadPersistedState();
        
        // Start network listener
        network.start(this::handleRaftMessage);
        
        // Start election timeout
        scheduleElectionTimeout();
        
        running.set(true);
    }
    
    /**
     * Stop the Raft node gracefully.
     */
    @Override
    public void close() {
        if (!running.getAndSet(false)) {
            return;
        }
        
        LOGGER.info("Stopping Raft node: {}", config.getNodeId());
        
        // Cancel timers
        if (electionTimer != null) {
            electionTimer.cancel(true);
        }
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(true);
        }
        
        // Shutdown executor
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Stop network
        network.stop();
        
        // Persist final state
        persistState();
    }
    
    /**
     * Handle incoming Raft messages from other nodes.
     */
    private void handleRaftMessage(RaftMessage message) {
        if (!running.get()) {
            return;
        }
        
        try {
            switch (message.getType()) {
                case APPEND_ENTRIES_REQUEST:
                    handleAppendEntries((AppendEntriesRequest) message);
                    break;
                case APPEND_ENTRIES_RESPONSE:
                    handleAppendEntriesResponse((AppendEntriesResponse) message);
                    break;
                case REQUEST_VOTE_REQUEST:
                    handleRequestVote((RequestVoteRequest) message);
                    break;
                case REQUEST_VOTE_RESPONSE:
                    handleRequestVoteResponse((RequestVoteResponse) message);
                    break;
                case HEARTBEAT:
                    handleHeartbeat((Heartbeat) message);
                    break;
                default:
                    LOGGER.warn("Unknown message type: {}", message.getType());
            }
        } catch (Exception e) {
            LOGGER.error("Error handling Raft message: {}", message, e);
        }
    }
    
    /**
     * Handle AppendEntries RPC from leader.
     */
    private void handleAppendEntries(AppendEntriesRequest request) {
        // Validate term
        if (request.getTerm() < currentTerm.get()) {
            sendResponse(request, new AppendEntriesResponse(config.getNodeId(), currentTerm.get(), false, 0));
            return;
        }
        
        // Step down if newer term
        if (request.getTerm() > currentTerm.get()) {
            stepDown(request.getTerm());
        }
        
        // Reset election timeout
        electionTimeout.reset();
        
        // Update leader
        leaderId.set(request.getNodeId());
        
        // Validate log consistency
        if (request.getPrevLogIndex() > 0 && 
            request.getPrevLogIndex() > raftLog.getLastIndex()) {
            sendResponse(request, new AppendEntriesResponse(config.getNodeId(), currentTerm.get(), false, 0));
            return;
        }
        
        if (request.getPrevLogIndex() > 0 && 
            raftLog.getEntry(request.getPrevLogIndex()).getTerm() != request.getPrevLogTerm()) {
            sendResponse(request, new AppendEntriesResponse(config.getNodeId(), currentTerm.get(), false, 0));
            return;
        }
        
        // Append new entries
        long lastIndex = raftLog.getLastIndex();
        for (long i = lastIndex + 1; i <= request.getPrevLogIndex() + request.getEntries().size(); i++) {
            int entryIndex = (int) (i - request.getPrevLogIndex() - 1);
            if (entryIndex < request.getEntries().size()) {
                raftLog.appendEntry(request.getEntries().get(entryIndex));
            }
        }
        
        // Update commit index
        long newCommitIndex = Math.min(request.getCommitIndex(), raftLog.getLastIndex());
        if (newCommitIndex > commitIndex.get()) {
            commitIndex.set(newCommitIndex);
            applyCommittedEntries();
        }
        
        sendResponse(request, new AppendEntriesResponse(config.getNodeId(), currentTerm.get(), true, raftLog.getLastIndex()));
    }
    
    /**
     * Handle AppendEntries response from follower.
     */
    private void handleAppendEntriesResponse(AppendEntriesResponse response) {
        if (state.get() != RaftNodeState.LEADER || response.getTerm() != currentTerm.get()) {
            return;
        }
        
        // Update match index
        String followerId = response.getNodeId();
        if (response.isSuccess()) {
            matchIndex.put(followerId, response.getLastMatchIndex());
            nextIndex.put(followerId, Math.min(nextIndex.get(followerId) + 1, response.getLastMatchIndex() + 1));
        } else {
            nextIndex.put(followerId, Math.max(nextIndex.get(followerId) - 1, 1));
        }
        
        // Check if we can commit more entries
        checkCommit();
    }
    
    /**
     * Handle RequestVote RPC from candidate.
     */
    private void handleRequestVote(RequestVoteRequest request) {
        // Validate term
        if (request.getTerm() < currentTerm.get()) {
            sendResponse(request, new RequestVoteResponse(config.getNodeId(), currentTerm.get(), false));
            return;
        }
        
        // Step down if newer term
        if (request.getTerm() > currentTerm.get()) {
            stepDown(request.getTerm());
        }
        
        // Check if we've already voted in this term
        if (votedFor.get() != -1 && votedFor.get() != request.getCandidateId()) {
            sendResponse(request, new RequestVoteResponse(config.getNodeId(), currentTerm.get(), false));
            return;
        }
        
        // Check candidate's log
        RaftLogEntry lastLogEntry = raftLog.getLastEntry();
        boolean logUpToDate = request.getLastLogIndex() > raftLog.getLastIndex() ||
                            (request.getLastLogIndex() == raftLog.getLastIndex() &&
                             request.getLastLogTerm() >= lastLogEntry.getTerm());
        
        if (logUpToDate) {
            votedFor.set(request.getCandidateId());
            persistState();
            sendResponse(request, new RequestVoteResponse(config.getNodeId(), currentTerm.get(), true));
            
            // Reset election timeout (we're following this candidate)
            electionTimeout.reset();
        } else {
            sendResponse(request, new RequestVoteResponse(config.getNodeId(), currentTerm.get(), false));
        }
    }
    
    /**
     * Handle RequestVote response from other nodes.
     */
    private void handleRequestVoteResponse(RequestVoteResponse response) {
        if (state.get() != RaftNodeState.CANDIDATE || response.getTerm() != currentTerm.get()) {
            return;
        }
        
        if (response.isVoteGranted()) {
            if (voteCounter.receiveVote(response.getNodeId())) {
                becomeLeader();
            }
        }
    }
    
    /**
     * Handle heartbeat from leader.
     */
    private void handleHeartbeat(Heartbeat heartbeat) {
        if (heartbeat.getTerm() < currentTerm.get()) {
            return;
        }
        
        if (heartbeat.getTerm() > currentTerm.get()) {
            stepDown(heartbeat.getTerm());
        }
        
        // Reset election timeout
        electionTimeout.reset();
        
        // Update leader
        leaderId.set(heartbeat.getNodeId());
    }
    
    /**
     * Start election process.
     */
    private void startElection() {
        if (state.get() != RaftNodeState.FOLLOWER) {
            return;
        }
        
        LOGGER.info("Starting election for node: {}", config.getNodeId());
        
        // Become candidate
        state.set(RaftNodeState.CANDIDATE);
        currentTerm.incrementAndGet();
        votedFor.set(1L);
        
        // Vote for ourselves
        voteCounter.reset();
        voteCounter.receiveVote(config.getNodeId());
        
        // Persist state
        persistState();
        
        // Request votes from other nodes
        RaftLogEntry lastLogEntry = raftLog.getLastEntry();
        RequestVoteRequest request = new RequestVoteRequest(
            config.getNodeId(),
            currentTerm.get(),
            1L, // candidateId as long
            lastLogEntry != null ? lastLogEntry.getIndex() : 0,
            lastLogEntry != null ? lastLogEntry.getTerm() : 0
        );
        
        for (String nodeId : config.getClusterNodes()) {
            if (!nodeId.equals(config.getNodeId())) {
                network.sendRequestVote(nodeId, request);
            }
        }
        
        // Schedule next election timeout
        scheduleElectionTimeout();
    }
    
    /**
     * Become leader after winning election.
     */
    private void becomeLeader() {
        LOGGER.info("Node {} became leader for term {}", config.getNodeId(), currentTerm.get());
        
        state.set(RaftNodeState.LEADER);
        leaderId.set(config.getNodeId());
        
        // Reset next indices for all followers
        for (String nodeId : config.getClusterNodes()) {
            if (!nodeId.equals(config.getNodeId())) {
                nextIndex.put(nodeId, (long) raftLog.getLastIndex() + 1);
                matchIndex.put(nodeId, 0L);
            }
        }
        
        // Start sending heartbeats
        startHeartbeats();
        
        // Schedule next heartbeat
        scheduleHeartbeat();
    }
    
    /**
     * Step down to follower state.
     */
    private void stepDown(long newTerm) {
        LOGGER.info("Node {} stepping down to follower for term {}", config.getNodeId(), newTerm);
        
        state.set(RaftNodeState.FOLLOWER);
        currentTerm.set(newTerm);
        votedFor.set(-1);
        leaderId.set(null);
        
        // Cancel leader timers
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(true);
        }
        
        // Reset election timeout
        electionTimeout.reset();
        scheduleElectionTimeout();
        
        persistState();
    }
    
    /**
     * Start sending heartbeats to followers.
     */
    private void startHeartbeats() {
        Heartbeat heartbeat = new Heartbeat(
            config.getNodeId(),
            currentTerm.get()
        );
        
        for (String nodeId : config.getClusterNodes()) {
            if (!nodeId.equals(config.getNodeId())) {
                network.sendHeartbeat(nodeId, heartbeat);
            }
        }
    }
    
    /**
     * Send AppendEntries to followers.
     */
    private void sendAppendEntries(String followerId) {
        long prevLogIndex = nextIndex.get(followerId) - 1;
        RaftLogEntry prevLogEntry = raftLog.getEntry(prevLogIndex);
        
        List<RaftLogEntry> entries = new ArrayList<>();
        long nextLogIndex = prevLogIndex + 1;
        while (nextLogIndex <= raftLog.getLastIndex() && entries.size() < config.getMaxBatchSize()) {
            entries.add(raftLog.getEntry(nextLogIndex));
            nextLogIndex++;
        }
        
        AppendEntriesRequest request = new AppendEntriesRequest(
            config.getNodeId(),
            currentTerm.get(),
            prevLogIndex,
            prevLogEntry != null ? prevLogEntry.getTerm() : 0,
            entries,
            commitIndex.get()
        );
        
        network.sendAppendEntries(followerId, request);
    }
    
    /**
     * Check if we can commit more entries.
     */
    private void checkCommit() {
        int quorumSize = (config.getClusterNodes().size() / 2) + 1;
        long newCommitIndex = commitIndex.get();
        
        for (long i = commitIndex.get() + 1; i <= raftLog.getLastIndex(); i++) {
            int matchCount = 1; // Count ourselves
            for (String nodeId : config.getClusterNodes()) {
                if (matchIndex.getOrDefault(nodeId, 0L) >= i) {
                    matchCount++;
                }
            }
            
            if (matchCount >= quorumSize) {
                newCommitIndex = i;
            } else {
                break;
            }
        }
        
        if (newCommitIndex > commitIndex.get()) {
            commitIndex.set(newCommitIndex);
            applyCommittedEntries();
        }
    }
    
    /**
     * Apply committed entries to state machine.
     */
    private void applyCommittedEntries() {
        while (lastApplied.get() < commitIndex.get()) {
            lastApplied.incrementAndGet();
            RaftLogEntry entry = raftLog.getEntry(lastApplied.get());
            
            // Apply entry to state machine
            applyEntry(entry);
        }
    }
    
    /**
     * Apply a single entry to the state machine.
     */
    private void applyEntry(RaftLogEntry entry) {
        // Implementation depends on the specific command type
        LOGGER.debug("Applying entry {} to state machine", entry.getIndex());
    }
    
    /**
     * Send response to RPC request.
     */
    private void sendResponse(RaftMessage request, RaftMessage response) {
        if (request instanceof AppendEntriesRequest) {
            network.sendAppendEntriesResponse(request.getNodeId(), (AppendEntriesResponse) response);
        } else if (request instanceof RequestVoteRequest) {
            network.sendRequestVoteResponse(request.getNodeId(), (RequestVoteResponse) response);
        }
    }
    
    /**
     * Schedule election timeout.
     */
    private void scheduleElectionTimeout() {
        if (electionTimer != null) {
            electionTimer.cancel(true);
        }
        
        long timeout = electionTimeout.getTimeout();
        electionTimer = executor.schedule(() -> {
            if (running.get() && state.get() == RaftNodeState.FOLLOWER) {
                startElection();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Schedule heartbeat.
     */
    private void scheduleHeartbeat() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(true);
        }
        
        heartbeatTimer = executor.scheduleAtFixedRate(() -> {
            if (running.get() && state.get() == RaftNodeState.LEADER) {
                startHeartbeats();
                
                // Send AppendEntries to followers
                for (String nodeId : config.getClusterNodes()) {
                    if (!nodeId.equals(config.getNodeId())) {
                        sendAppendEntries(nodeId);
                    }
                }
            }
        }, config.getHeartbeatIntervalMs(), config.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
    }
    
    /**
     * Load persisted state from disk.
     */
    private void loadPersistedState() throws IOException {
        raftLog.loadPersistedState();
        currentTerm.set(raftLog.getCurrentTerm());
        votedFor.set(raftLog.getVotedFor());
    }
    
    /**
     * Persist current state to disk.
     */
    private void persistState() {
        try {
            raftLog.persistState(currentTerm.get(), votedFor.get());
        } catch (IOException e) {
            LOGGER.error("Failed to persist Raft state", e);
        }
    }
    
    /**
     * Check if this node has quorum.
     */
    public boolean hasQuorum() {
        if (state.get() != RaftNodeState.LEADER) {
            return false;
        }
        
        int quorumSize = (config.getClusterNodes().size() / 2) + 1;
        int matchCount = 1; // Count ourselves
        
        for (String nodeId : config.getClusterNodes()) {
            if (matchIndex.getOrDefault(nodeId, 0L) >= commitIndex.get()) {
                matchCount++;
            }
        }
        
        hasQuorum.set(matchCount >= quorumSize);
        return hasQuorum.get();
    }
    
    /**
     * Execute a write operation (only allowed for leader with quorum).
     */
    public boolean executeWrite(String command) {
        if (state.get() != RaftNodeState.LEADER || !hasQuorum()) {
            return false;
        }
        
        RaftLogEntry entry = new RaftLogEntry(
            currentTerm.get(),
            raftLog.getLastIndex() + 1,
            command
        );
        
        raftLog.appendEntry(entry);
        return true;
    }
    
    // Public getters
    public RaftNodeState getState() {
        return state.get();
    }
    
    public long getCurrentTerm() {
        return currentTerm.get();
    }
    
    public String getLeaderId() {
        return leaderId.get();
    }
    
    public RaftConfig getConfig() {
        return config;
    }
    
    public RaftLog getRaftLog() {
        return raftLog;
    }
    
    public boolean isLeader() {
        return state.get() == RaftNodeState.LEADER;
    }
    
    public boolean isRunning() {
        return running.get();
    }
}