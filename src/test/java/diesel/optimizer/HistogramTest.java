package diesel.optimizer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("query")
class HistogramTest {
    @Test
    void equiDepthProducesEvenlySpacedBoundsForOneHundredBuckets() {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            values.add(i);
        }
        List<Object> bounds = Histogram.equiDepth(values, 100);
        assertEquals(101, bounds.size());
        assertEquals(0, bounds.get(0));
        assertEquals(10, bounds.get(1));
        assertEquals(500, bounds.get(50));
        assertEquals(990, bounds.get(99));
        assertEquals(999, bounds.get(100));
    }

    @Test
    void equiDepthCollapsesBucketsForSmallInputs() {
        List<Object> values = List.of(1, 2, 3, 4, 5);
        List<Object> bounds = Histogram.equiDepth(values, 100);
        assertEquals(6, bounds.size());
        assertEquals(values, bounds.subList(0, 5));
        assertEquals(5, bounds.get(5));
    }

    @Test
    void equiDepthHandlesEmptyAndNullInputs() {
        assertTrue(Histogram.equiDepth(null, 100).isEmpty());
        assertTrue(Histogram.equiDepth(List.of(), 100).isEmpty());
    }

    @Test
    void compareHandlesMixedComparableTypes() {
        assertTrue(Histogram.compare(2, 10) < 0);
        assertTrue(Histogram.compare(10L, 2L) > 0);
        assertTrue(Histogram.compare("A", "B") < 0);
        assertTrue(Histogram.compare(1, "1") == 0);
    }
}
