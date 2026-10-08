package diesel;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Background write-behind flusher for auto-commit DML (prompt4 R3-005
 * adjacent: keeps whole-file storage rewrites off the writer thread).
 *
 * <p>When {@code diesel.persist.background} is enabled (default), auto-commit
 * mutations only mark the table dirty; this daemon rewrites the data file on a
 * short interval so INSERT latency is parse+insert rather than a synchronous
 * Avro/CSV/TSV/JSONL whole-file save. Explicit durability points are
 * unchanged: COMMIT, DDL, {@code loadTablesFromDisk}, {@code Database.close},
 * the JVM shutdown hook, and {@code diesel.persist.deferred=false} /
 * {@code diesel.persist.background=false} still flush synchronously.
 */
final class PersistFlusher {

    private static final Logger LOGGER = Logger.getLogger(PersistFlusher.class.getName());

    static final String BACKGROUND_KEY = "diesel.persist.background";
    static final String INTERVAL_KEY = "diesel.persist.flush.interval.ms";

    private static final Object MONITOR = new Object();
    private static volatile Thread thread;
    private static volatile boolean running;

    private PersistFlusher() {
    }

    /** Starts the daemon if background persistence is enabled. Idempotent. */
    static void ensureStarted() {
        if (!backgroundEnabled()) {
            return;
        }
        synchronized (MONITOR) {
            if (running) {
                return;
            }
            running = true;
            thread = new Thread(PersistFlusher::run, "diesel-persist-flusher");
            thread.setDaemon(true);
            thread.start();
            LOGGER.log(Level.FINE, "Persist flusher started (interval {0} ms)", flushIntervalMs());
        }
    }

    /** Wakes the flusher so a newly dirty table is written promptly. */
    static void signal() {
        ensureStarted();
        synchronized (MONITOR) {
            MONITOR.notifyAll();
        }
    }

    /** Stops the daemon and performs a final flush of every pending table. */
    static void shutdown() {
        Thread t;
        synchronized (MONITOR) {
            running = false;
            t = thread;
            thread = null;
            MONITOR.notifyAll();
        }
        if (t != null) {
            try {
                t.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        Table.flushAllPendingPersists();
    }

    /** True when background write-behind is on (sysprop overrides config). */
    static boolean backgroundEnabled() {
        return !"false".equalsIgnoreCase(Table.resolvePersistConfig(BACKGROUND_KEY, "true"));
    }

    /** Flush period in milliseconds (sysprop overrides config; min 1 ms). */
    static long flushIntervalMs() {
        try {
            long configured = Long.parseLong(
                    Table.resolvePersistConfig(INTERVAL_KEY, "50").trim());
            return configured > 0 ? configured : 50L;
        } catch (NumberFormatException e) {
            return 50L;
        }
    }

    private static void run() {
        while (running) {
            long interval = flushIntervalMs();
            synchronized (MONITOR) {
                if (!running) {
                    break;
                }
                try {
                    MONITOR.wait(interval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (!running) {
                break;
            }
            try {
                Table.flushAllPendingPersists();
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "Background persist flush failed", t);
            }
        }
    }
}
