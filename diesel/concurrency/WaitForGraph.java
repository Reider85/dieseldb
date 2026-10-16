package diesel.concurrency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wait-for graph for deadlock detection.
 * <p>
 * Represents a directed graph where edges A→B indicate that transaction A is waiting
 * for a lock held by transaction B. The graph supports cycle detection and victim selection.
 */
public final class WaitForGraph {

    // Adjacency list: txid -> set of txids it waits for
    private volatile Map<Long, Set<Long>> adjacency;

    /**
     * Creates a new empty wait-for graph.
     */
    public WaitForGraph() {
        this.adjacency = new ConcurrentHashMap<>();
    }

    /**
     * Rebuilds the graph with new edges (atomic replacement).
     * 
     * @param edges map where keys are waiting txids and values are sets of txids they wait for
     */
    public void rebuild(Map<Long, Set<Long>> edges) {
        Map<Long, Set<Long>> newAdjacency = new ConcurrentHashMap<>();
        for (Map.Entry<Long, Set<Long>> entry : edges.entrySet()) {
            Set<Long> targets = new HashSet<>(entry.getValue());
            targets.remove(entry.getKey()); // self-loops are not deadlocks
            newAdjacency.put(entry.getKey(), targets);
        }
        this.adjacency = newAdjacency;
    }

    /**
     * Returns a snapshot of the current adjacency list.
     */
    public Map<Long, Set<Long>> snapshot() {
        Map<Long, Set<Long>> snapshot = new HashMap<>();
        for (Map.Entry<Long, Set<Long>> entry : adjacency.entrySet()) {
            snapshot.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return Collections.unmodifiableMap(snapshot);
    }

    /**
     * Finds a cycle in the graph using iterative DFS.
     * Returns the first cycle found, or an empty list if no cycle exists.
     * 
     * @return list of txids forming a cycle path, or empty list if no cycle found
     */
    public List<Long> findCycle() {
        // Color map for cycle detection: WHITE=unvisited, GRAY=visiting, BLACK=finished
        // Must include neighbor-only nodes (targets of edges are not adjacency keys)
        Map<Long, Integer> colors = new HashMap<>();
        for (Long node : adjacency.keySet()) {
            colors.put(node, 0); // WHITE
            for (Long neighbor : adjacency.get(node)) {
                colors.putIfAbsent(neighbor, 0); // WHITE
            }
        }

        // Track the current DFS path for cycle reconstruction
        LinkedList<Long> path = new LinkedList<>();

        for (Long node : adjacency.keySet()) {
            if (colors.getOrDefault(node, 0) == 0) { // WHITE
                List<Long> cycle = dfsFindCycle(node, colors, path);
                if (!cycle.isEmpty()) {
                    return cycle;
                }
            }
        }
        return Collections.emptyList();
    }

    /**
     * Iterative DFS to find a cycle starting from the given node.
     */
    private List<Long> dfsFindCycle(Long start, Map<Long, Integer> colors, LinkedList<Long> path) {
        Stack<DfsNode> stack = new Stack<>();
        stack.push(new DfsNode(start, 0));

        while (!stack.isEmpty()) {
            DfsNode current = stack.peek();
            Long node = current.node;
            int index = current.index;

Integer nodeColor = colors.getOrDefault(node, 0);
        if (nodeColor == 0) { // WHITE - first visit
            colors.put(node, 1); // GRAY
            path.addLast(node);

                // Visit neighbors
                Set<Long> neighbors = adjacency.getOrDefault(node, Collections.emptySet());
                List<Long> neighborsList = new ArrayList<>(neighbors);
                
                if (index < neighborsList.size()) {
                    Long neighbor = neighborsList.get(index);
                    current.index++; // advance to next neighbor for next time
                    stack.push(new DfsNode(neighbor, 0));
                    
                    // Check for cycle: if we see a GRAY node, we found a cycle
                    Integer neighborColor = colors.getOrDefault(neighbor, 0);
                    if (neighborColor == 1) { // GRAY
                        // Cycle found: extract path from neighbor to current and close it
                        int cycleStart = path.indexOf(neighbor);
                        List<Long> cycle = new ArrayList<>(path.subList(cycleStart, path.size()));
                        cycle.add(neighbor); // close: ... -> neighbor
                        return cycle;
                    }
                } else {
                    // All neighbors visited, finish this node
                    colors.put(node, 2); // BLACK
                    path.removeLast();
                    stack.pop();
                }
            } else if (nodeColor == 1) { // GRAY - already in stack
                // Back edge already handled, just pop
                path.removeLast();
                stack.pop();
            } else if (nodeColor == 2) { // BLACK - finished
                stack.pop();
            }
        }
        
        return Collections.emptyList();
    }

    /**
     * Helper class for iterative DFS with node and neighbor index.
     */
    private static class DfsNode {
        final Long node;
        int index;

        DfsNode(Long node, int index) {
            this.node = node;
            this.index = index;
        }
    }

    /**
     * Selects the victim transaction from a cycle.
     * The victim is the transaction with the minimum txid (oldest transaction).
     * 
     * @param cycle list of txids forming a cycle
     * @return the victim txid
     * @throws IllegalArgumentException if cycle is empty or null
     */
    public static long selectVictim(List<Long> cycle) {
        if (cycle == null || cycle.isEmpty()) {
            throw new IllegalArgumentException("Cycle cannot be empty");
        }
        
        long victim = cycle.get(0);
        for (long txid : cycle) {
            if (txid < victim) {
                victim = txid;
            }
        }
        return victim;
    }

    /**
     * Returns the number of nodes in the graph.
     */
    public int nodeCount() {
        return adjacency.size();
    }

    /**
     * Returns the number of edges in the graph.
     */
    public int edgeCount() {
        int count = 0;
        for (Set<Long> neighbors : adjacency.values()) {
            count += neighbors.size();
        }
        return count;
    }

    /**
     * Checks if the graph is empty (no nodes).
     */
    public boolean isEmpty() {
        return adjacency.isEmpty();
    }

    /**
     * Clears all nodes and edges.
     */
    public void clear() {
        adjacency.clear();
    }
}