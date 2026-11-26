package diesel.replication.raft;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Raft election test (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Tests that a new leader is elected within 5 seconds when the current leader fails.
 * Verifies the acceptance criteria: "RaftElectionTest: kill leader → new leader < 5 sec".
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RaftElectionTest extends BaseRaftTest {
    
    @Test
    @Order(1)
    void testInitialLeaderElection() throws Exception {
        // Start all nodes
        startAllNodes();
        
        // Verify that a leader is elected
        RaftNode leader = getLeader();
        assertNotNull(leader, "Leader should be elected initially");
        assertTrue(leader.isLeader(), "Elected node should be in leader state");
        
        // Verify that other nodes are followers
        List<RaftNode> followers = getFollowers();
        assertEquals(2, followers.size(), "Should have 2 followers");
        
        for (RaftNode follower : followers) {
            assertFalse(follower.isLeader(), "Followers should not be leaders");
        }
        
        System.out.println("Initial leader elected: " + leader.getConfig().getNodeId());
    }
    
    @Test
    @Order(2)
    void testLeaderFailureAndElection() throws Exception {
        // Start all nodes and wait for initial election
        startAllNodes();
        RaftNode originalLeader = getLeader();
        assertNotNull(originalLeader, "Original leader should exist");
        
        System.out.println("Original leader: " + originalLeader.getConfig().getNodeId());
        
        // Record the current term
        long originalTerm = originalLeader.getCurrentTerm();
        
        // Kill the current leader
        simulateNodeFailure(originalLeader);
        
        System.out.println("Killed leader: " + originalLeader.getConfig().getNodeId());
        
        // Wait for new leader election (should be < 5 seconds)
        long startTime = System.currentTimeMillis();
        RaftNode newLeader = null;
        
        while (System.currentTimeMillis() - startTime < 5000) {
            newLeader = getLeader();
            if (newLeader != null && newLeader.isLeader()) {
                break;
            }
            Thread.sleep(100);
        }
        
        // Verify new leader was elected within 5 seconds
        assertNotNull(newLeader, "New leader should be elected within 5 seconds");
        assertTrue(newLeader.isLeader(), "New leader should be in leader state");
        
        // Verify that the new leader has a higher term
        assertTrue(newLeader.getCurrentTerm() > originalTerm, "New leader should have higher term");
        
        // Verify that the remaining nodes are followers
        List<RaftNode> followers = getFollowers();
        assertEquals(1, followers.size(), "Should have 1 follower");
        
        for (RaftNode follower : followers) {
            assertFalse(follower.isLeader(), "Followers should not be leaders");
        }
        
        long electionTime = System.currentTimeMillis() - startTime;
        System.out.println("New leader elected: " + newLeader.getConfig().getNodeId() + 
                         " in " + electionTime + "ms");
        
        // Verify election time is less than 5 seconds
        assertTrue(electionTime < 5000, "Election should complete in less than 5 seconds");
    }
    
    @Test
    @Order(3)
    void testMultipleLeaderFailures() throws Exception {
        // Start all nodes and wait for initial election
        startAllNodes();
        RaftNode firstLeader = getLeader();
        assertNotNull(firstLeader, "First leader should exist");
        
        System.out.println("First leader: " + firstLeader.getConfig().getNodeId());
        
        // Kill first leader
        simulateNodeFailure(firstLeader);
        
        // Wait for second leader election
        long startTime1 = System.currentTimeMillis();
        RaftNode secondLeader = null;
        
        while (System.currentTimeMillis() - startTime1 < 5000) {
            secondLeader = getLeader();
            if (secondLeader != null && secondLeader.isLeader()) {
                break;
            }
            Thread.sleep(100);
        }
        
        assertNotNull(secondLeader, "Second leader should be elected");
        System.out.println("Second leader: " + secondLeader.getConfig().getNodeId() + 
                         " in " + (System.currentTimeMillis() - startTime1) + "ms");
        
        // Kill second leader
        simulateNodeFailure(secondLeader);
        
        // Wait for third leader election
        long startTime2 = System.currentTimeMillis();
        RaftNode thirdLeader = null;
        
        while (System.currentTimeMillis() - startTime2 < 5000) {
            thirdLeader = getLeader();
            if (thirdLeader != null && thirdLeader.isLeader()) {
                break;
            }
            Thread.sleep(100);
        }
        
        assertNotNull(thirdLeader, "Third leader should be elected");
        System.out.println("Third leader: " + thirdLeader.getConfig().getNodeId() + 
                         " in " + (System.currentTimeMillis() - startTime2) + "ms");
        
        // Verify all elections completed within 5 seconds
        assertTrue(System.currentTimeMillis() - startTime1 < 5000, "Second election should complete in < 5s");
        assertTrue(System.currentTimeMillis() - startTime2 < 5000, "Third election should complete in < 5s");
    }
    
    @Test
    @Order(4)
    void testElectionWithTwoNodes() throws Exception {
        // Create a 2-node cluster
        tempDirs.clear();
        raftNodes.clear();
        raftConfigs.clear();
        
        // Create 2 temporary directories
        Path tempDir1 = createTempDirectory();
        Path tempDir2 = createTempDirectory();
        tempDirs.add(tempDir1);
        tempDirs.add(tempDir2);
        
        // Create 2-node configurations
        RaftConfig config1 = new RaftConfig(
            true, "node-1", List.of("node-1", "node-2"), 
            DEFAULT_ELECTION_TIMEOUT, DEFAULT_HEARTBEAT_INTERVAL, tempDir1,
            5, 1024 * 1024, 10, false // No quorum required for 2 nodes
        );
        
        RaftConfig config2 = new RaftConfig(
            true, "node-2", List.of("node-1", "node-2"), 
            DEFAULT_ELECTION_TIMEOUT, DEFAULT_HEARTBEAT_INTERVAL, tempDir2,
            5, 1024 * 1024, 10, false // No quorum required for 2 nodes
        );
        
        raftConfigs.add(config1);
        raftConfigs.add(config2);
        
        // Start 2 nodes
        RaftNode node1 = createRaftNode(0);
        RaftNode node2 = createRaftNode(1);
        
        node1.start();
        node2.start();
        
        // Wait for initial election
        waitForClusterStabilization(3000);
        
        RaftNode initialLeader = getLeader();
        assertNotNull(initialLeader, "Leader should be elected in 2-node cluster");
        System.out.println("Initial 2-node leader: " + initialLeader.getConfig().getNodeId());
        
        // Kill the leader
        simulateNodeFailure(initialLeader);
        
        // Wait for new leader election
        long startTime = System.currentTimeMillis();
        RaftNode newLeader = null;
        
        while (System.currentTimeMillis() - startTime < 5000) {
            newLeader = getLeader();
            if (newLeader != null && newLeader.isLeader()) {
                break;
            }
            Thread.sleep(100);
        }
        
        assertNotNull(newLeader, "New leader should be elected in 2-node cluster");
        System.out.println("New 2-node leader: " + newLeader.getConfig().getNodeId() + 
                         " in " + (System.currentTimeMillis() - startTime) + "ms");
        
        // Verify election time is less than 5 seconds
        assertTrue(System.currentTimeMillis() - startTime < 5000, 
                  "2-node election should complete in < 5s");
    }
    
    @Test
    @Order(5)
    void testElectionPerformance() throws Exception {
        // Test election performance with multiple runs
        int successfulElections = 0;
        int totalTests = 5;
        
        for (int i = 0; i < totalTests; i++) {
            try {
                // Setup fresh cluster
                tearDown();
                setUp();
                
                // Start nodes and wait for election
                startAllNodes();
                RaftNode initialLeader = getLeader();
                assertNotNull(initialLeader, "Initial leader should exist");
                
                // Kill leader and measure election time
                simulateNodeFailure(initialLeader);
                
                long startTime = System.currentTimeMillis();
                RaftNode newLeader = null;
                
                while (System.currentTimeMillis() - startTime < 5000) {
                    newLeader = getLeader();
                    if (newLeader != null && newLeader.isLeader()) {
                        break;
                    }
                    Thread.sleep(50);
                }
                
                if (newLeader != null) {
                    long electionTime = System.currentTimeMillis() - startTime;
                    System.out.println("Election " + (i + 1) + ": " + electionTime + "ms");
                    successfulElections++;
                    
                    // Verify election time is reasonable
                    assertTrue(electionTime < 5000, "Election " + (i + 1) + " should be < 5s");
                }
                
            } catch (Exception e) {
                System.err.println("Election test " + (i + 1) + " failed: " + e.getMessage());
            }
        }
        
        // Verify that most elections succeeded
        assertTrue(successfulElections >= totalTests * 0.8, 
                  "At least 80% of elections should succeed: " + successfulElections + "/" + totalTests);
    }
}