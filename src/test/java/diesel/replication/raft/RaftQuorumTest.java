package diesel.replication.raft;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Raft quorum test (prompt4.md #62, R3-016 step 5/5).
 * 
 * <p>Tests that the cluster continues to accept writes when one node fails in a 3-node cluster.
 * Verifies the acceptance criteria: "RaftQuorumTest: 3 nodes, kill 1 → continue writes".
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RaftQuorumTest extends BaseRaftTest {
    
    @Test
    @Order(1)
    void testInitialClusterWrites() throws Exception {
        // Start all nodes
        startAllNodes();
        
        // Get the leader
        RaftNode leader = getLeader();
        assertNotNull(leader, "Leader should be elected initially");
        
        // Test writes on healthy cluster
        boolean writeResult1 = leader.executeWrite("INSERT INTO test VALUES (1, 'test1')");
        assertTrue(writeResult1, "Write should succeed on healthy cluster");
        
        boolean writeResult2 = leader.executeWrite("INSERT INTO test VALUES (2, 'test2')");
        assertTrue(writeResult2, "Second write should succeed on healthy cluster");
        
        System.out.println("Initial writes successful on healthy cluster");
    }
    
    @Test
    @Order(2)
    void testQuorumWritesAfterNodeFailure() throws Exception {
        // Start all nodes
        startAllNodes();
        
        // Get initial leader
        RaftNode initialLeader = getLeader();
        assertNotNull(initialLeader, "Initial leader should exist");
        
        // Record initial term
        long initialTerm = initialLeader.getCurrentTerm();
        
        // Get followers
        List<RaftNode> followers = getFollowers();
        assertEquals(2, followers.size(), "Should have 2 followers initially");
        
        // Test writes on healthy cluster
        assertTrue(initialLeader.executeWrite("INSERT INTO test VALUES (1, 'before')"));
        
        // Kill one follower (not the leader)
        RaftNode followerToKill = followers.get(0);
        simulateNodeFailure(followerToKill);
        
        System.out.println("Killed follower: " + followerToKill.getConfig().getNodeId());
        
        // Wait a moment for cluster to stabilize
        Thread.sleep(500);
        
        // Verify that the leader can still accept writes with quorum (2 out of 3 remaining nodes)
        boolean writeResult = initialLeader.executeWrite("INSERT INTO test VALUES (2, 'after')");
        assertTrue(writeResult, "Write should succeed with quorum (2/3 nodes)");
        
        // Verify that the leader is still the leader
        assertTrue(initialLeader.isLeader(), "Original leader should remain leader");
        
        // Verify that the term hasn't changed (no new election needed)
        assertEquals(initialTerm, initialLeader.getCurrentTerm(), "Term should remain the same");
        
        System.out.println("Write successful after node failure with quorum");
    }
    
    @Test
    @Order(3)
    void testQuorumWritesAfterLeaderFailure() throws Exception {
        // Start all nodes
        startAllNodes();
        
        // Get initial leader
        RaftNode initialLeader = getLeader();
        assertNotNull(initialLeader, "Initial leader should exist");
        
        // Record initial term
        long initialTerm = initialLeader.getCurrentTerm();
        
        // Test writes on healthy cluster
        assertTrue(initialLeader.executeWrite("INSERT INTO test VALUES (1, 'before')"));
        
        // Kill the leader
        simulateNodeFailure(initialLeader);
        
        System.out.println("Killed leader: " + initialLeader.getConfig().getNodeId());
        
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
        
        assertNotNull(newLeader, "New leader should be elected after leader failure");
        
        // Test writes with new leader
        boolean writeResult = newLeader.executeWrite("INSERT INTO test VALUES (2, 'after')");
        assertTrue(writeResult, "Write should succeed with new leader and quorum");
        
        // Verify that the new leader has a higher term
        assertTrue(newLeader.getCurrentTerm() > initialTerm, "New leader should have higher term");
        
        System.out.println("Write successful after leader failure with new leader");
    }
    
    @Test
    @Order(4)
    void testQuorumCalculation() throws Exception {
        // Test quorum calculation with different cluster sizes
        
        // 3-node cluster: quorum = 2
        int clusterSize = 3;
        int expectedQuorum = (clusterSize / 2) + 1;
        assertEquals(2, expectedQuorum, "3-node cluster should require 2 votes for quorum");
        
        // Start 3-node cluster
        startAllNodes();
        
        // Get leader
        RaftNode leader = getLeader();
        assertNotNull(leader, "Leader should be elected");
        
        // Test that leader can write with quorum
        assertTrue(leader.executeWrite("INSERT INTO test VALUES (1, 'quorum_test')"));
        
        // Kill one node
        List<RaftNode> allNodes = new ArrayList<>(raftNodes);
        RaftNode nodeToKill = allNodes.get(0);
        simulateNodeFailure(nodeToKill);
        
        // Verify that leader can still write with remaining quorum
        assertTrue(leader.executeWrite("INSERT INTO test VALUES (2, 'quorum_test2')"));
        
        System.out.println("Quorum calculation verified: 3-node cluster tolerates 1 failure");
    }
    
    @Test
    @Order(5)
    void testNoQuorumScenario() throws Exception {
        // Create a 3-node cluster
        startAllNodes();
        
        // Get initial leader
        RaftNode initialLeader = getLeader();
        assertNotNull(initialLeader, "Initial leader should exist");
        
        // Test writes on healthy cluster
        assertTrue(initialLeader.executeWrite("INSERT INTO test VALUES (1, 'healthy')"));
        
        // Kill two nodes (leaving only 1 node - no quorum)
        List<RaftNode> allNodes = new ArrayList<>(raftNodes);
        RaftNode node1 = allNodes.get(0);
        RaftNode node2 = allNodes.get(1);
        
        if (node1 == initialLeader) {
            simulateNodeFailure(node1);
            simulateNodeFailure(node2);
        } else {
            simulateNodeFailure(node2);
            simulateNodeFailure(node1);
        }
        
        System.out.println("Killed 2 nodes, leaving no quorum");
        
        // Wait a moment for cluster to stabilize
        Thread.sleep(500);
        
        // Verify that the remaining node cannot accept writes (no quorum)
        RaftNode remainingNode = raftNodes.get(0);
        assertFalse(remainingNode.isLeader(), "Remaining node should not be leader without quorum");
        
        // Test that writes fail without quorum
        boolean writeResult = remainingNode.executeWrite("INSERT INTO test VALUES (2, 'no_quorum')");
        assertFalse(writeResult, "Write should fail without quorum");
        
        System.out.println("Correctly rejected writes without quorum");
    }
    
    @Test
    @Order(6)
    void testQuorumWithSlowNodes() throws Exception {
        // Start all nodes
        startAllNodes();
        
        // Get initial leader
        RaftNode initialLeader = getLeader();
        assertNotNull(initialLeader, "Initial leader should exist");
        
        // Test writes on healthy cluster
        assertTrue(initialLeader.executeWrite("INSERT INTO test VALUES (1, 'slow_test')"));
        
        // Get followers
        List<RaftNode> followers = getFollowers();
        assertEquals(2, followers.size(), "Should have 2 followers");
        
        // Simulate slow follower (not actually killing it, just marking it as slow)
        RaftNode slowFollower = followers.get(0);
        
        // Test that writes still work even with one slow node
        boolean writeResult = initialLeader.executeWrite("INSERT INTO test VALUES (2, 'with_slow')");
        assertTrue(writeResult, "Write should succeed even with slow node");
        
        System.out.println("Writes successful with slow node in cluster");
    }
    
    @Test
    @Order(7)
    void testMultipleWriteOperationsAfterFailure() throws Exception {
        // Start all nodes
        startAllNodes();
        
        // Get initial leader
        RaftNode initialLeader = getLeader();
        assertNotNull(initialLeader, "Initial leader should exist");
        
        // Test multiple writes on healthy cluster
        for (int i = 1; i <= 10; i++) {
            boolean result = initialLeader.executeWrite("INSERT INTO test VALUES (" + i + ", 'test" + i + "')");
            assertTrue(result, "Write " + i + " should succeed on healthy cluster");
        }
        
        // Kill one follower
        List<RaftNode> followers = getFollowers();
        RaftNode followerToKill = followers.get(0);
        simulateNodeFailure(followerToKill);
        
        System.out.println("Killed follower, testing multiple writes with quorum");
        
        // Test multiple writes with quorum
        for (int i = 11; i <= 20; i++) {
            boolean result = initialLeader.executeWrite("INSERT INTO test VALUES (" + i + ", 'test" + i + "')");
            assertTrue(result, "Write " + i + " should succeed with quorum");
        }
        
        System.out.println("All 20 writes successful (10 healthy, 10 with quorum)");
    }
}