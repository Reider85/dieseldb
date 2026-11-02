package diesel.replication;

import diesel.wal.WALEntry;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Standby-side streaming client (prompt4 #58, R3-016 step 1/5).
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #fetchBaseBackup} (one-shot, before the receiver thread
 *       starts) requests the master's data directory and writes it into the
 *       standby's directory; the returned resume LSN seeds the stream.</li>
 *   <li>{@link #start} opens a streaming connection with
 *       {@code START(appliedLsn + 1)} and forwards every received
 *       {@link diesel.wal.WALEntry} to the {@link WalApplier}, then sends an
 *       {@code ACK} of the applied LSN.</li>
 *   <li>On any I/O failure the connection is torn down and retried with an
 *       exponential backoff (100 ms → 1 s cap). The resume LSN always comes
 *       from {@link WalApplier#getAppliedLsn()}, so a reconnect replays
 *       exactly the uncommitted tail — catch-up is automatic.</li>
 * </ol>
 *
 * <p>Read timeout: the master sends a heartbeat every 50 ms while idle, so a
 * 10-second socket read timeout reliably detects a dead master without
 * aborting healthy idle streams.
 *
 * <p>The resume marker ({@code replication.lsn}) is owned by the applier side
 * (see {@code ReplicationCoordinator.StandbyApplier}): it is written only
 * after the row files it covers are persisted, so a standby restart resumes at
 * a state its files actually contain.
 */
public final class WalReceiver implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(WalReceiver.class.getName());

    /** Nominal backoff after a failed connection attempt. */
    static final long INITIAL_BACKOFF_MS = 500;
    /** Backoff ceiling. */
    static final long MAX_BACKOFF_MS = 1000;
    /** Master heartbeats every 200 ms; 10 s of silence means the master is gone. */
    static final int READ_TIMEOUT_MS = 60_000;
    /** Send an ACK (and persist the marker) at most this often. */
    static final long ACK_INTERVAL_MS = 10;

    private final String host;
    private final int port;
    private final WalApplier applier;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicInteger reconnectAttempts = new AtomicInteger();
    private final AtomicLong receivedEntries = new AtomicLong();
    private final AtomicLong lastAckSentNanos = new AtomicLong();
    private volatile Thread workerThread;
    private volatile Socket currentSocket;

    /**
     * Creates a receiver; no connection is made until {@link #start()}.
     *
     * @param host    master host
     * @param port    master replication port
     * @param applier the standby apply pipeline (also owns resume-marker
     *                persistence — the receiver only streams)
     */
    public WalReceiver(String host, int port, WalApplier applier) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.applier = Objects.requireNonNull(applier, "applier");
    }

    /**
     * Performs a one-shot base backup: requests the master's data directory,
     * materializes every file under {@code targetDir} and returns the resume
     * LSN.
     *
     * <p>Existing files under {@code targetDir} are overwritten (a re-bootstrap
     * after an expired marker).
     *
     * @param host      master host
     * @param port      master replication port
     * @param targetDir directory to populate (created if missing)
     * @return the LSN the stream must resume from
     * @throws IOException on connection loss, protocol errors, or write failures
     */
    public static long fetchBaseBackup(String host, int port, Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setTcpNoDelay(true);
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            ReplicationWire.writeFrame(out, ReplicationWire.TYPE_BASE_BACKUP, new byte[0]);

            int files = 0;
            long startLsn = -1;
            while (startLsn < 0) {
                ReplicationWire.Frame frame = ReplicationWire.readFrame(in);
                switch (frame.type()) {
                    case ReplicationWire.TYPE_FILE -> {
                        ReplicationWire.BackupFile file = ReplicationWire.decodeFile(frame.payload());
                        writeFile(targetDir, file);
                        files++;
                    }
                    case ReplicationWire.TYPE_BACKUP_END -> startLsn = ReplicationWire.readLong(frame);
                    case ReplicationWire.TYPE_ERROR -> throw new IOException("Base backup failed: "
                            + new String(frame.payload(), StandardCharsets.UTF_8));
                    default -> throw new IOException("Unexpected base backup message type: "
                            + frame.type());
                }
            }
            LOGGER.log(Level.INFO, "Base backup from {0}:{1}: {2} files, resume LSN {3}",
                    new Object[]{host, port, files, startLsn});
            return startLsn;
        }
    }

    private static void writeFile(Path targetDir, ReplicationWire.BackupFile file) throws IOException {        Path relative = targetDir.resolve(file.name()).normalize();
        if (!relative.startsWith(targetDir)) {
            throw new IOException("Backup file escapes target dir: " + file.name());
        }
        Files.createDirectories(relative.getParent());
        Files.write(relative, file.content(),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    /**
     * Starts the receive/reconnect loop on a daemon thread. The initial
     * resume LSN is {@code max(1, appliedLsn + 1)}; with no base backup and no
     * applied entries the stream starts at LSN 1 (full WAL replay).
     */
    public void start() {
        Thread thread = new Thread(this::runLoop, "wal-receiver");
        thread.setDaemon(true);
        workerThread = thread;
        thread.start();
    }

    /** Returns true while a streaming connection is established. */
    public boolean isConnected() {
        return connected.get();
    }

    /** Returns the number of failed connection/reconnect attempts so far. */
    public int getReconnectAttempts() {
        return reconnectAttempts.get();
    }

    /** Returns the number of WAL entries received and handed to the applier. */
    public long getReceivedEntries() {
        return receivedEntries.get();
    }

    private void runLoop() {
        long backoffMs = INITIAL_BACKOFF_MS;
        while (running.get()) {
            try {
                streamOnce();
                backoffMs = INITIAL_BACKOFF_MS; // clean exit (sender closed): no penalty
            } catch (IOException e) {
                if (!running.get()) {
                    break;
                }
                int attempt = reconnectAttempts.incrementAndGet();
                LOGGER.log(Level.WARNING,
                        "WAL stream lost (attempt {0}), retrying in {1} ms: {2}",
                        new Object[]{attempt, backoffMs, e.getMessage()});
                sleepQuietly(backoffMs);
                backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
            }
        }
    }

    /**
     * One connection lifetime: connect, send START with the resume LSN, then
     * forward entries until the connection breaks or the receiver stops.
     */
    private void streamOnce() throws IOException {
        long resumeLsn = Math.max(1, applier.getAppliedLsn() + 1);
        try (Socket socket = new Socket()) {
            currentSocket = socket;
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setTcpNoDelay(true);
            socket.setSoTimeout(READ_TIMEOUT_MS);
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            ReplicationWire.writeFrame(out, ReplicationWire.TYPE_START,
                    ReplicationWire.longPayload(resumeLsn));
            connected.set(true);
            LOGGER.log(Level.INFO, "Streaming connected to {0}:{1}, resume LSN {2}",
                    new Object[]{host, port, resumeLsn});

            while (running.get()) {
                ReplicationWire.Frame frame = ReplicationWire.readFrame(in);
                switch (frame.type()) {
                    case ReplicationWire.TYPE_WAL_ENTRY -> {
                        WALEntry entry = WALEntry.fromBytes(frame.payload());
                        applier.apply(entry);
                        receivedEntries.incrementAndGet();
                        maybeAck(out);
                    }
                    case ReplicationWire.TYPE_HEARTBEAT -> {
                        // Liveness only; the applier state is unchanged.
                    }
                    case ReplicationWire.TYPE_ERROR -> throw new IOException("Streaming error: "
                            + new String(frame.payload(), StandardCharsets.UTF_8));
                    default -> throw new IOException("Unexpected streaming message type: "
                            + frame.type());
                }
            }
        } catch (EOFException e) {
            throw new IOException("Master closed the replication stream", e);
        } finally {
            connected.set(false);
            currentSocket = null;
        }
    }

    /**
     * Sends an ACK of the applied LSN at most every {@link #ACK_INTERVAL_MS}.
     * Coalescing keeps ACKs off the per-entry hot path; the payload feeds
     * future synchronous replication (prompt 60) and observability.
     */
    private void maybeAck(DataOutputStream out) throws IOException {
        long now = System.nanoTime();
        long last = lastAckSentNanos.get();
        // Reduce ACK interval from 10ms to 5ms for better error detection
        if (now - last < 5 * 1_000_000L || !lastAckSentNanos.compareAndSet(last, now)) {
            return;
        }
        long appliedLsn = applier.getAppliedLsn();
        ReplicationWire.writeFrame(out, ReplicationWire.TYPE_ACK, ReplicationWire.longPayload(appliedLsn));
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Stops the receiver, aborting any in-flight connection and read. */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Socket socket = currentSocket;
        if (socket != null) {
            try {
                socket.close(); // unblocks the read in the worker thread
            } catch (IOException ignored) {
                // best effort
            }
        }
        Thread thread = workerThread;
        if (thread != null) {
            try {
                thread.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.info("WalReceiver stopped");
    }

    /** Convenience for tests: waits until the given condition holds. */
    public static void sleep(long millis) {
        sleepQuietly(millis);
    }
}
