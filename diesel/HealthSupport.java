package diesel;

import diesel.observability.Gauge;
import diesel.observability.HealthCheck;
import diesel.observability.LivenessCheck;
import diesel.observability.MetricsRegistry;
import diesel.observability.ReadinessCheck;
import diesel.storage.page.BufferPool;
import diesel.storage.page.PageManager;
import diesel.wal.WALWriter;

import java.io.File;

/**
 * Public facade for health probes (prompt4 #46): wires the runtime state of a
 * live {@link Database} into the supplier-based liveness and readiness checks
 * of {@link HealthCheck}.
 *
 * <p>Lives in the {@code diesel} package because {@code Database} is
 * package-private; {@code DatabaseServer} calls this facade instead of reaching
 * into the engine directly (same pattern as {@link PitrSupport} for #43).
 *
 * <p>Liveness: dead WAL writer thread (only when WAL is enabled) or free heap
 * below {@code oomFreeHeapPercent}. Readiness: ARIES startup recovery still
 * running or a cold buffer pool (fewer than {@code coldStartMinPages} resident
 * pages).
 */
public final class HealthSupport {

    private HealthSupport() {
        // utility class
    }

    /**
     * Builds a health check wired to a live database with the default
     * thresholds.
     *
     * @param database the served database
     * @return a health check bound to this database's runtime state
     */
    public static HealthCheck forDatabase(Database database) {
        return forDatabase(database, HealthCheck.DEFAULT_OOM_FREE_HEAP_PERCENT,
                HealthCheck.DEFAULT_COLDSTART_MIN_PAGES);
    }

    /**
     * Builds a health check wired to a live database.
     *
     * <p>Thresholds are passed in (instead of read from {@code ConfigLoader}
     * here) because callers own configuration; {@code DatabaseServer} supplies
     * the values.
     *
     * @param database           the served database
     * @param oomFreeHeapPercent free-heap floor (percent of max heap)
     * @param coldStartMinPages  minimum resident pages for a warm buffer pool
     * @return a health check bound to this database's runtime state
     */
    public static HealthCheck forDatabase(Database database,
                                          double oomFreeHeapPercent,
                                          int coldStartMinPages) {
        boolean walEnabled = database.isWALEnabled();
        LivenessCheck liveness = new LivenessCheck(
                () -> {
                    WALWriter writer = database.getWalWriter();
                    // A missing writer while WAL is enabled is a startup race,
                    // not evidence of death — only an exited thread is fatal.
                    return writer == null || writer.isWriterAlive();
                },
                walEnabled,
                HealthSupport::freeHeapPercent,
                oomFreeHeapPercent);

        ReadinessCheck readiness = new ReadinessCheck(
                () -> {
                    RecoveryManager recovery = database.getRecoveryManager();
                    return recovery != null && !recovery.isRecoveryCompleted();
                },
                () -> residentPages(database),
                coldStartMinPages);

        return new HealthCheck(
                liveness,
                readiness,
                () -> readGauge(MetricsRegistry.TRANSACTIONS_ACTIVE),
                () -> diskFreeMb(database.getDataDir()),
                () -> readGauge(MetricsRegistry.WAL_LAG_MS),
                System.nanoTime());
    }

    /**
     * Free heap as a percentage of the configured max heap. A nearly exhausted
     * heap is the only JVM-level signal we can act on before an
     * {@link OutOfMemoryError} takes the process down.
     */
    private static double freeHeapPercent() {
        Runtime runtime = Runtime.getRuntime();
        long max = runtime.maxMemory();
        if (max <= 0) {
            return 100.0;
        }
        long used = runtime.totalMemory() - runtime.freeMemory();
        long free = max - used;
        return free * 100.0 / max;
    }

    /** Reads a JVM-wide gauge, defaulting to 0 when it is not registered yet. */
    private static double readGauge(String name) {
        Gauge gauge = MetricsRegistry.getInstance().getGauge(name);
        return gauge == null ? 0.0 : gauge.getValue();
    }

    /** Free space on the database volume in MB (-1 when the path is unusable). */
    private static double diskFreeMb(String dataDir) {
        try {
            return new File(dataDir).getUsableSpace() / (1024.0 * 1024.0);
        } catch (RuntimeException e) {
            return -1.0;
        }
    }

    /** Resident pages, or {@link Integer#MAX_VALUE} when no pool exists (warm). */
    private static int residentPages(Database database) {
        PageManager pageManager = database.getPageManager();
        if (pageManager == null) {
            return Integer.MAX_VALUE;
        }
        BufferPool pool = pageManager.getBufferPool();
        return pool == null ? Integer.MAX_VALUE : pool.getResidentPages();
    }
}
