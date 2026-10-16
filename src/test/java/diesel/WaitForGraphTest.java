package diesel;

import diesel.concurrency.WaitForGraph;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WaitForGraph cycle detection and victim selection.
 * <p>
 * Tests cycle detection in various graph configurations, victim selection,
 * and graph rebuilding.
 */
@Tag("concurrency")
class WaitForGraphTest {

    @Test
    @Timeout(5)
    void emptyGraphHasNoCycle() {
        WaitForGraph graph = new WaitForGraph();
        assertTrue(graph.isEmpty());
        assertEquals(0, graph.nodeCount());
        assertEquals(0, graph.edgeCount());
        assertTrue(graph.findCycle().isEmpty());
    }

    @Test
    @Timeout(5)
    void singleNodeNoCycle() {
        WaitForGraph graph = new WaitForGraph();
        graph.rebuild(Map.of(1L, Set.of()));
        
        assertEquals(1, graph.nodeCount());
        assertEquals(0, graph.edgeCount());
        assertTrue(graph.findCycle().isEmpty());
    }

    @Test
    @Timeout(5)
    void linearChainNoCycle() {
        WaitForGraph graph = new WaitForGraph();
        // 1→2→3 (linear chain, no cycle)
        Map<Long, Set<Long>> edges = Map.of(
            1L, Set.of(2L),
            2L, Set.of(3L),
            3L, Set.of()
        );
        graph.rebuild(edges);
        
        assertEquals(3, graph.nodeCount());
        assertEquals(2, graph.edgeCount());
        assertTrue(graph.findCycle().isEmpty());
    }

    @Test
    @Timeout(5)
    void simpleTwoNodeCycle() {
        WaitForGraph graph = new WaitForGraph();
        // 1→2→1 (cycle)
        Map<Long, Set<Long>> edges = Map.of(
            1L, Set.of(2L),
            2L, Set.of(1L)
        );
        graph.rebuild(edges);
        
        assertEquals(2, graph.nodeCount());
        assertEquals(2, graph.edgeCount());
        
        List<Long> cycle = graph.findCycle();
        assertFalse(cycle.isEmpty());
        assertEquals(3, cycle.size()); // 1→2→1 or 2→1→2
        assertTrue(cycle.get(0).equals(cycle.get(cycle.size() - 1))); // Start and end same
    }

    @Test
    @Timeout(5)
    void threeNodeCycle() {
        WaitForGraph graph = new WaitForGraph();
        // 1→2→3→1 (3-node cycle)
        Map<Long, Set<Long>> edges = Map.of(
            1L, Set.of(2L),
            2L, Set.of(3L),
            3L, Set.of(1L)
        );
        graph.rebuild(edges);
        
        List<Long> cycle = graph.findCycle();
        assertFalse(cycle.isEmpty());
        assertEquals(4, cycle.size()); // 1→2→3→1 or similar
        
        // Verify it's a valid cycle
        assertTrue(cycle.get(0).equals(cycle.get(cycle.size() - 1)));
        
        // Victim should be minimum txid in cycle
        long victim = WaitForGraph.selectVictim(cycle);
        assertEquals(1L, victim); // Minimum of {1,2,3}
    }

    @Test
    @Timeout(5)
    void complexGraphWithMultipleCycles() {
        WaitForGraph graph = new WaitForGraph();
        // Graph with two separate cycles: 1→2→1 and 3→4→3
        Map<Long, Set<Long>> edges = Map.of(
            1L, Set.of(2L),
            2L, Set.of(1L),
            3L, Set.of(4L),
            4L, Set.of(3L),
            5L, Set.of()  // Node 5 not in cycle
        );
        graph.rebuild(edges);
        
        // Should find first cycle (1→2→1)
        List<Long> cycle = graph.findCycle();
        assertFalse(cycle.isEmpty());
        assertEquals(3, cycle.size());
        
        // Victim should be minimum in the found cycle
        long victim = WaitForGraph.selectVictim(cycle);
        assertEquals(1L, victim);
    }

    @Test
    @Timeout(5)
    void graphWithSelfLoopIgnored() {
        WaitForGraph graph = new WaitForGraph();
        // 1→1 (self-loop), 2→3 (no cycle)
        Map<Long, Set<Long>> edges = Map.of(
            1L, Set.of(1L),  // Self-loop - should be ignored
            2L, Set.of(3L),
            3L, Set.of()
        );
        graph.rebuild(edges);
        
        // No cycles detected (self-loops ignored)
        assertTrue(graph.findCycle().isEmpty());
    }

    @Test
    @Timeout(5)
    void diamondGraphNoCycle() {
        WaitForGraph graph = new WaitForGraph();
        // Diamond: 1→2, 1→3, 2→4, 3→4 (no cycle)
        Map<Long, Set<Long>> edges = Map.of(
            1L, Set.of(2L, 3L),
            2L, Set.of(4L),
            3L, Set.of(4L),
            4L, Set.of()
        );
        graph.rebuild(edges);
        
        assertTrue(graph.findCycle().isEmpty());
        assertEquals(4, graph.nodeCount());
        assertEquals(4, graph.edgeCount());
    }

    @Test
    @Timeout(5)
    void rebuildGraph() {
        WaitForGraph graph = new WaitForGraph();
        
        // Initial graph: 1→2
        graph.rebuild(Map.of(1L, Set.of(2L), 2L, Set.of()));
        assertEquals(1, graph.edgeCount());
        assertTrue(graph.findCycle().isEmpty());
        
        // Rebuild with cycle: 1→2→1
        graph.rebuild(Map.of(1L, Set.of(2L), 2L, Set.of(1L)));
        assertEquals(2, graph.edgeCount());
        
        List<Long> cycle = graph.findCycle();
        assertFalse(cycle.isEmpty());
        
        // Rebuild with no edges
        graph.rebuild(Map.of(1L, Set.of(), 2L, Set.of()));
        assertEquals(0, graph.edgeCount());
        assertTrue(graph.findCycle().isEmpty());
    }

    @Test
    @Timeout(5)
    void snapshotReturnsImmutableView() {
        WaitForGraph graph = new WaitForGraph();
        graph.rebuild(Map.of(1L, Set.of(2L), 2L, Set.of()));
        
        Map<Long, Set<Long>> snapshot = graph.snapshot();
        
        // Verify snapshot is unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> {
            snapshot.keySet().add(3L);
        });
        
        // Verify original graph is unchanged
        assertEquals(2, graph.nodeCount());
        assertEquals(1, graph.edgeCount());
    }

    @Test
    @Timeout(5)
    void victimSelection() {
        // Test victim selection with various cycle configurations
        List<List<Long>> testCycles = Arrays.asList(
            List.of(1L, 2L, 1L),    // 2-node cycle, victim=1
            List.of(5L, 3L, 8L, 5L), // 3-node cycle, victim=3
            List.of(10L, 20L, 30L, 40L, 10L) // 4-node cycle, victim=10
        );
        
        for (List<Long> cycle : testCycles) {
            long victim = WaitForGraph.selectVictim(cycle);
            long expected = Collections.min(cycle);
            assertEquals(expected, victim, "Victim should be minimum txid in cycle: " + cycle);
        }
    }

    @Test
    @Timeout(5)
    void victimSelectionThrowsOnEmptyCycle() {
        assertThrows(IllegalArgumentException.class, () -> {
            WaitForGraph.selectVictim(Collections.emptyList());
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            WaitForGraph.selectVictim(null);
        });
    }

    @Test
    @Timeout(5)
    void clearGraph() {
        WaitForGraph graph = new WaitForGraph();
        graph.rebuild(Map.of(1L, Set.of(2L), 2L, Set.of(1L)));
        
        assertFalse(graph.isEmpty());
        assertEquals(2, graph.nodeCount());
        
        graph.clear();
        
        assertTrue(graph.isEmpty());
        assertEquals(0, graph.nodeCount());
        assertEquals(0, graph.edgeCount());
        assertTrue(graph.findCycle().isEmpty());
    }
}