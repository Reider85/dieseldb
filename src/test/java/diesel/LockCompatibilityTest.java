package diesel;

import diesel.concurrency.LockCompatibility;
import diesel.concurrency.LockMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for LockCompatibility matrix.
 * <p>
 * Tests all 25 combinations of lock mode compatibility to ensure
 * the matrix matches standard database lock semantics.
 */
@Tag("concurrency")
class LockCompatibilityTest {

    @Test
    void compatibilityMatrix() {
        // Test each combination explicitly
        assertTrue(LockCompatibility.isCompatible(LockMode.IS, LockMode.IS));
        assertTrue(LockCompatibility.isCompatible(LockMode.IS, LockMode.IX));
        assertTrue(LockCompatibility.isCompatible(LockMode.IS, LockMode.S));
        assertFalse(LockCompatibility.isCompatible(LockMode.IS, LockMode.X));
        assertTrue(LockCompatibility.isCompatible(LockMode.IS, LockMode.SIX));

        assertTrue(LockCompatibility.isCompatible(LockMode.IX, LockMode.IS));
        assertTrue(LockCompatibility.isCompatible(LockMode.IX, LockMode.IX));
        assertFalse(LockCompatibility.isCompatible(LockMode.IX, LockMode.S));
        assertFalse(LockCompatibility.isCompatible(LockMode.IX, LockMode.X));
        assertFalse(LockCompatibility.isCompatible(LockMode.IX, LockMode.SIX));

        assertTrue(LockCompatibility.isCompatible(LockMode.S, LockMode.IS));
        assertFalse(LockCompatibility.isCompatible(LockMode.S, LockMode.IX));
        assertTrue(LockCompatibility.isCompatible(LockMode.S, LockMode.S));
        assertFalse(LockCompatibility.isCompatible(LockMode.S, LockMode.X));
        assertFalse(LockCompatibility.isCompatible(LockMode.S, LockMode.SIX));

        assertFalse(LockCompatibility.isCompatible(LockMode.X, LockMode.IS));
        assertFalse(LockCompatibility.isCompatible(LockMode.X, LockMode.IX));
        assertFalse(LockCompatibility.isCompatible(LockMode.X, LockMode.S));
        assertFalse(LockCompatibility.isCompatible(LockMode.X, LockMode.X));
        assertFalse(LockCompatibility.isCompatible(LockMode.X, LockMode.SIX));

        assertTrue(LockCompatibility.isCompatible(LockMode.SIX, LockMode.IS));
        assertFalse(LockCompatibility.isCompatible(LockMode.SIX, LockMode.IX));
        assertFalse(LockCompatibility.isCompatible(LockMode.SIX, LockMode.S));
        assertFalse(LockCompatibility.isCompatible(LockMode.SIX, LockMode.X));
        assertFalse(LockCompatibility.isCompatible(LockMode.SIX, LockMode.SIX));
    }

    @Test
    void compatibilityWithAll() {
        // Test compatibility with multiple held locks
        List<LockMode> heldModes = Arrays.asList(LockMode.IS, LockMode.S);
        
        // IS should be compatible with both
        assertTrue(LockCompatibility.isCompatibleWithAll(LockMode.IS, heldModes));
        
        // IX should be compatible with IS but not S
        assertFalse(LockCompatibility.isCompatibleWithAll(LockMode.IX, heldModes));
        
// S should be compatible with both IS and S
        assertTrue(LockCompatibility.isCompatibleWithAll(LockMode.S, heldModes));
        
        // X should be compatible with neither
        assertFalse(LockCompatibility.isCompatibleWithAll(LockMode.X, heldModes));
    }

    @Test
    void symmetryTest() {
        // Test that compatibility is symmetric for compatible pairs
        assertTrue(LockCompatibility.isCompatible(LockMode.IS, LockMode.S));
        assertTrue(LockCompatibility.isCompatible(LockMode.S, LockMode.IS));
        
        assertTrue(LockCompatibility.isCompatible(LockMode.IS, LockMode.IX));
        assertTrue(LockCompatibility.isCompatible(LockMode.IX, LockMode.IS));
        
        assertTrue(LockCompatibility.isCompatible(LockMode.IS, LockMode.SIX));
        assertTrue(LockCompatibility.isCompatible(LockMode.SIX, LockMode.IS));
        
        // Test that incompatibility is symmetric for incompatible pairs
        assertFalse(LockCompatibility.isCompatible(LockMode.IX, LockMode.S));
        assertFalse(LockCompatibility.isCompatible(LockMode.S, LockMode.IX));
        
        assertFalse(LockCompatibility.isCompatible(LockMode.IX, LockMode.SIX));
        assertFalse(LockCompatibility.isCompatible(LockMode.SIX, LockMode.IX));
        
        assertFalse(LockCompatibility.isCompatible(LockMode.S, LockMode.X));
        assertFalse(LockCompatibility.isCompatible(LockMode.X, LockMode.S));
    }

    @Test
    void selfCompatibility() {
        // All modes should be compatible with themselves
        assertTrue(LockCompatibility.isCompatible(LockMode.IS, LockMode.IS));
        assertTrue(LockCompatibility.isCompatible(LockMode.IX, LockMode.IX));
        assertTrue(LockCompatibility.isCompatible(LockMode.S, LockMode.S));
        assertFalse(LockCompatibility.isCompatible(LockMode.X, LockMode.X)); // X is incompatible with everything
        assertFalse(LockCompatibility.isCompatible(LockMode.SIX, LockMode.SIX));
    }

    @Test
    void integrationWithLockManager() {
        // Test that the compatibility matrix works correctly with LockManager
        diesel.concurrency.LockManager manager = new diesel.concurrency.LockManager();
        final long tx1 = 1L;
        final long tx2 = 2L;
        
        // Tx1 acquires IS lock
        assertTrue(manager.tryAcquire("table:USERS", LockMode.IS, tx1));
        
        // Tx2 should be able to acquire IS (compatible)
        assertTrue(manager.tryAcquire("table:USERS", LockMode.IS, tx2));
        
        // Tx2 should be able to acquire S (compatible with IS per standard MGL matrix)
        assertTrue(manager.tryAcquire("table:USERS", LockMode.S, tx2));
        
        // Tx2 should be able to acquire IX (compatible with IS)
        assertTrue(manager.tryAcquire("table:USERS", LockMode.IX, tx2));
        
        // Tx2 should NOT be able to acquire X (incompatible with IS and IX)
        assertFalse(manager.tryAcquire("table:USERS", LockMode.X, tx2));
        
        // Clean up
        manager.release(tx1);
        manager.release(tx2);
    }
}