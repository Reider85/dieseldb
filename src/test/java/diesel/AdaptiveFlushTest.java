package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import diesel.storage.page.AdaptiveFlushStrategy;

/**
 * Unit tests for AdaptiveFlushStrategy (prompt4.md #20, R3-005 step 1/3).
 */
@Tag("smoke")
public class AdaptiveFlushTest {

    @Test
    void testInitialInterval() {
        AdaptiveFlushStrategy strategy = new AdaptiveFlushStrategy(200);
        assertEquals(200, strategy.getCurrentIntervalMs());
        assertEquals(200, strategy.getBaseIntervalMs());
        assertEquals(0, strategy.getDirtyPageCount());
        assertEquals(0, strategy.getCapacityPages());
        assertEquals(0.0, strategy.getCurrentDirtyRatio());
    }

    @Test
    void testAggressiveThreshold() {
        AdaptiveFlushStrategy strategy = new AdaptiveFlushStrategy(200);
        
        // 30% dirty ratio (> 25%) should trigger aggressive interval
        int interval = strategy.updateAndGetCurrentInterval(30, 100);
        assertEquals(100, interval); // 200 * 0.5 = 100
        assertEquals(30, strategy.getDirtyPageCount());
        assertEquals(100, strategy.getCapacityPages());
        assertEquals(0.30, strategy.getCurrentDirtyRatio());
    }

    @Test
    void testLazyThreshold() {
        AdaptiveFlushStrategy strategy = new AdaptiveFlushStrategy(200);
        
        // 3% dirty ratio (< 5%) should trigger lazy interval
        int interval = strategy.updateAndGetCurrentInterval(3, 100);
        assertEquals(400, interval); // 200 * 2 = 400
        assertEquals(3, strategy.getDirtyPageCount());
        assertEquals(100, strategy.getCapacityPages());
        assertEquals(0.03, strategy.getCurrentDirtyRatio());
    }

    @Test
    void testNormalRange() {
        AdaptiveFlushStrategy strategy = new AdaptiveFlushStrategy(200);
        
        // 15% dirty ratio (between 5% and 25%) should use base interval
        int interval = strategy.updateAndGetCurrentInterval(15, 100);
        assertEquals(200, interval);
        assertEquals(15, strategy.getDirtyPageCount());
        assertEquals(100, strategy.getCapacityPages());
        assertEquals(0.15, strategy.getCurrentDirtyRatio());
    }

    @Test
    void testIntervalClamping() {
        // Aggressive clamp: small base, ×0.5 would fall below MIN_INTERVAL_MS
        AdaptiveFlushStrategy small = new AdaptiveFlushStrategy(15);
        assertEquals(10, small.updateAndGetCurrentInterval(90, 100)); // (int)(15*0.5)=7 -> MIN 10
        assertEquals(AdaptiveFlushStrategy.MIN_INTERVAL_MS, small.getCurrentIntervalMs());

        // Lazy clamp: large base, ×2 would exceed MAX_INTERVAL_MS
        AdaptiveFlushStrategy large = new AdaptiveFlushStrategy(40_000);
        assertEquals(60_000, large.updateAndGetCurrentInterval(1, 100)); // 80000 -> MAX 60000
        assertEquals(AdaptiveFlushStrategy.MAX_INTERVAL_MS, large.getCurrentIntervalMs());
    }

    @Test
    void testZeroCapacity() {
        AdaptiveFlushStrategy strategy = new AdaptiveFlushStrategy(200);
        
        // Zero capacity should return base interval
        int interval = strategy.updateAndGetCurrentInterval(0, 0);
        assertEquals(200, interval);
        assertEquals(0, strategy.getDirtyPageCount());
        assertEquals(0, strategy.getCapacityPages());
        assertEquals(0.0, strategy.getCurrentDirtyRatio());
    }

    @Test
    void testNegativePages() {
        AdaptiveFlushStrategy strategy = new AdaptiveFlushStrategy(200);
        
        // Negative dirty count: ratio clamps to 0 -> < 5% -> lazy interval
        int interval = strategy.updateAndGetCurrentInterval(-5, 100);
        assertEquals(400, interval);
        assertEquals(-5, strategy.getDirtyPageCount()); // Records the actual value
        assertEquals(100, strategy.getCapacityPages());
        assertEquals(0.0, strategy.getCurrentDirtyRatio()); // Ratio is clamped to 0
    }

    @Test
    void testMultipleUpdates() {
        AdaptiveFlushStrategy strategy = new AdaptiveFlushStrategy(200);
        
        // Start with normal range
        assertEquals(200, strategy.updateAndGetCurrentInterval(15, 100));
        
        // Switch to aggressive
        assertEquals(100, strategy.updateAndGetCurrentInterval(30, 100));
        
        // Switch to lazy
        assertEquals(400, strategy.updateAndGetCurrentInterval(3, 100));
        
        // Back to normal
        assertEquals(200, strategy.updateAndGetCurrentInterval(15, 100));
        
        // Verify state tracking
        assertEquals(15, strategy.getDirtyPageCount());
        assertEquals(100, strategy.getCapacityPages());
        assertEquals(0.15, strategy.getCurrentDirtyRatio());
    }

    @Test
    void testToString() {
        AdaptiveFlushStrategy strategy = new AdaptiveFlushStrategy(200);
        strategy.updateAndGetCurrentInterval(30, 100); // 30% > 25% -> aggressive
        
        String str = strategy.toString();
        assertTrue(str.contains("base=200ms"));
        assertTrue(str.contains("current=100ms"));
        assertTrue(str.contains("dirty=30/100"));
        assertTrue(str.contains("0.30"));
    }

    @Test
    void testConstructorValidation() {
        assertThrows(IllegalArgumentException.class, () -> {
            new AdaptiveFlushStrategy(0);
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            new AdaptiveFlushStrategy(-100);
        });
    }
}