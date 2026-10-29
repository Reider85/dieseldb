package diesel.observability;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #44: MetricsRegistry — counters, gauges, registry semantics.
 * <p>
 * Uses metric names unique to this class so parallel test classes never
 * interfere (the registry is JVM-wide).
 */
@Tag("smoke")
public class MetricsRegistryTest {

    private static final String COUNTER_NAME = "diesel_test_registry_counter";

    @Test
    void oneMillionIncrementsOnTenThreadsSumExactly() throws InterruptedException {
        String name = COUNTER_NAME + "_contention";
        int threads = 10;
        int incrementsPerThread = 100_000; // 1M total (acceptance criterion)
        Counter counter = MetricsRegistry.getInstance().counter(name);

        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                    for (int i = 0; i < incrementsPerThread; i++) {
                        counter.increment();
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                } finally {
                    done.countDown();
                }
            }, "metrics-counter-worker-" + t);
            worker.setDaemon(true);
            worker.start();
        }
        start.countDown();
        assertTrue(done.await(60, java.util.concurrent.TimeUnit.SECONDS),
                "counter workers must finish within 60s");
        if (failure.get() != null) {
            throw new AssertionError("worker failed", failure.get());
        }
        // LongAdder is exact in sum() once all increments are visible; the
        // incrementing threads have joined the latch, so a lost update would
        // be a real race, not a visibility delay.
        assertEquals((long) threads * incrementsPerThread, counter.get(),
                "1M increments on 10 threads must sum exactly (no lost updates)");
    }

    @Test
    void sameNameAndLabelsReturnsSameInstance() {
        MetricsRegistry registry = MetricsRegistry.getInstance();
        Counter a = registry.counter(COUNTER_NAME + "_identity", "type", "A");
        Counter b = registry.counter(COUNTER_NAME + "_identity", "type", "A");
        assertSame(a, b, "same name+labels must reuse one instance");
        Counter other = registry.counter(COUNTER_NAME + "_identity", "type", "B");
        assertNotSame(a, other, "different labels must create distinct counters");
        assertEquals("type=A", a.getLabels());
        assertEquals(COUNTER_NAME + "_identity{type=A}", a.key());
    }

    @Test
    void addRejectsNegativeDelta() {
        Counter counter = MetricsRegistry.getInstance().counter(COUNTER_NAME + "_monotonic");
        counter.increment();
        assertThrows(IllegalArgumentException.class, () -> counter.add(-1),
                "counters are monotonic");
        counter.add(5);
        assertEquals(6L, counter.get());
    }

    @Test
    void oddLabelPairsAreRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> MetricsRegistry.renderLabels("onlyKey"),
                "label pairs must be alternating keys and values");
        assertEquals("", MetricsRegistry.renderLabels());
        assertEquals("a=1,b=2", MetricsRegistry.renderLabels("a", "1", "b", "2"));
    }

    @Test
    void gaugeIsSupplierBackedAndLatestRegistrationWins() {
        String name = COUNTER_NAME.replace("counter", "gauge");
        int[] slot = {7};
        MetricsRegistry registry = MetricsRegistry.getInstance();
        registry.registerGauge(name, () -> slot[0]);
        assertEquals(7.0, registry.getGauge(name).getValue(), 1e-9);

        // Replace semantics: a fresh Database/server supersedes a closed one.
        registry.registerGauge(name, () -> slot[0] * 2.0);
        assertEquals(14.0, registry.getGauge(name).getValue(), 1e-9,
                "latest gauge registration must win");

        slot[0] = 3;
        assertEquals(6.0, registry.getGauge(name).getValue(), 1e-9,
                "gauge value must be sampled on every read");
    }

    @Test
    void unregisteredGaugeReturnsNull() {
        assertEquals(null, MetricsRegistry.getInstance().getGauge(COUNTER_NAME + "_never_registered"));
    }

    @Test
    void exporterViewsExposeRegisteredMetrics() {
        String base = COUNTER_NAME + "_views";
        MetricsRegistry registry = MetricsRegistry.getInstance();
        Counter counter = registry.counter(base + "_c");
        Histogram histogram = registry.histogram(base + "_h");
        registry.registerGauge(base + "_g", () -> 42.0);

        assertTrue(registry.counters().contains(counter), "counters() must expose the counter");
        assertTrue(registry.histograms().contains(histogram), "histograms() must expose the histogram");
        boolean gaugeSeen = registry.gauges().stream().anyMatch(g -> g.getName().equals(base + "_g"));
        assertTrue(gaugeSeen, "gauges() must expose the gauge");

        counter.increment();
        assertFalse(registry.counters().isEmpty());
    }
}
