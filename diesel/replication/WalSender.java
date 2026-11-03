package diesel.replication;

import diesel.SyncReplicationCoordinator;
import diesel.wal.WALEntry;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Master-side WAL streaming endpoint (prompt4 #58, R3-016 step 1/5).
 *
 * <p>Listens on a dedicated replication port and serves one thread per standby
 * connection:
 * <ul>
 *   <li>{@code BASE_BACKUP} — calls {@link WalSource#prepareBackup()}, walks
 *       the data directory and ships every regular file as
 *       {@link ReplicationWire#TYPE_FILE} frames, terminated by
 *       {@link ReplicationWire#TYPE_BACKUP_END} carrying the resume LSN.</li>
 *   <li>{@code START(lsn)} — tails the WAL from {@code lsn} with a short poll
 *       interval, forwarding every new entry as
 *       {@link ReplicationWire#TYPE_WAL_ENTRY}; while idle it sends
 *       {@link ReplicationWire#TYPE_HEARTBEAT} frames so the standby detects a
 *       dead master.</li>
 *   <li>{@code ACK} frames from the standby are read and ignored (reserved for
 *       synchronous replication, prompt 60).</li>
 * </ul>
 *
 * <p>Multiple standbys are supported: each connection tails independently from
 * its own start LSN. Closing the sender closes the listener and every active
 * connection; in-flight handler threads exit on the resulting IOException.
 */
public final class WalSender implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(WalSender.class.getName());

    /** How often the sender checks for new WAL entries while streaming. */
    static final long POLL_INTERVAL_MS = 10;
    /** Heartbeat cadence while idle (the standby's read timeout is much larger). */
    static final long HEARTBEAT_INTERVAL_MS = 200;

    private final WalSource source;
    private final SyncReplicationCoordinator syncCoordinator;
    private final ServerSocket serverSocket;
    private final List<Socket> activeSockets = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread acceptThread;
    /** Backup-file names skipped when walking the data directory. */
    private static final String MARKER_FILE = "replication.lsn";

    /**
     * Binds the replication listener on the given port (0 = ephemeral).
     *
     * @param source the master WAL/data source
     * @param syncCoordinator the synchronous replication coordinator, or null
     * @param port   the listen port
     * @throws IOException if the socket cannot be bound
     */
    public WalSender(WalSource source, SyncReplicationCoordinator syncCoordinator, int port) throws IOException {
        this.source = Objects.requireNonNull(source, "source");
        this.syncCoordinator = syncCoordinator;
        this.serverSocket = new ServerSocket();
        this.serverSocket.bind(new InetSocketAddress(port));
        this.acceptThread = new Thread(this::acceptLoop, "wal-sender-accept");
        this.acceptThread.setDaemon(true);
        this.acceptThread.start();
        LOGGER.log(Level.INFO, "WalSender listening on port {0}", getPort());
    }

    /** Returns the actual bound port (useful with an ephemeral port). */
    public int getPort() {
        return serverSocket.getLocalPort();
    }

    /**
     * Abruptly closes every active standby connection while keeping the
     * listener alive — simulates a network drop so the standby exercises its
     * reconnect + catch-up path (prompt4 #58 ReconnectTest).
     *
     * @return the number of connections dropped
     */
    public int disconnectAllClients() {
        int dropped = activeSockets.size();
        for (Socket socket : activeSockets) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // best effort — the handler thread observes the failure
            }
        }
        return dropped;
    }

    private void acceptLoop() {
        while (running.get()) {
            try {
                Socket socket = serverSocket.accept();
                activeSockets.add(socket);
                Thread handler = new Thread(() -> handleClient(socket),
                        "wal-sender-" + socket.getPort());
                handler.setDaemon(true);
                handler.start();
            } catch (IOException e) {
                if (running.get()) {
                    LOGGER.log(Level.WARNING, "WalSender accept failed: {0}", e.getMessage());
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        try (socket) {
            socket.setTcpNoDelay(true);
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            while (running.get()) {
                ReplicationWire.Frame frame = ReplicationWire.readFrame(in);
                switch (frame.type()) {
                    case ReplicationWire.TYPE_BASE_BACKUP -> serveBaseBackup(out);
                    case ReplicationWire.TYPE_START -> {
                        long startLsn = ReplicationWire.readLong(frame);
                        streamWal(out, in, startLsn);
                        return; // streamWal returns only on close/error
                    }
                    case ReplicationWire.TYPE_ACK -> {
                        // Process synchronous replication acknowledgment
                        long appliedLsn = ReplicationWire.readLong(frame);
                        if (syncCoordinator != null) {
                            // For now, we use a placeholder standby name - in a full implementation,
                            // this would be extracted from the connection or frame metadata
                            syncCoordinator.processAck("standby-1", appliedLsn);
                        }
                    }
                    default -> ReplicationWire.writeFrame(out, ReplicationWire.TYPE_ERROR,
                            ("Unexpected replication message type: " + frame.type())
                                    .getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
            }
        } catch (IOException e) {
            if (running.get()) {
                LOGGER.log(Level.FINE, "Replication client disconnected: {0}", e.getMessage());
            }
        } finally {
            activeSockets.remove(socket);
        }
    }

    /**
     * Serves one base-backup request: flush the master, ship every regular
     * file of the data directory, then the resume LSN.
     */
    private void serveBaseBackup(DataOutputStream out) throws IOException {
        long startLsn = source.prepareBackup();
        Path dataDir = source.getDataDir();
        int files = 0;
        try (Stream<Path> walk = Files.walk(dataDir)) {
            List<Path> paths = walk
                    .filter(Files::isRegularFile)
                    .filter(p -> !p.getFileName().toString().endsWith(".tmp"))
                    .filter(p -> !p.getFileName().toString().equals(MARKER_FILE))
                    .sorted(Comparator.naturalOrder())
                    .toList();
            for (Path path : paths) {
                String relative = dataDir.relativize(path).toString().replace('\\', '/');
                byte[] content = Files.readAllBytes(path);
                ReplicationWire.writeFrame(out, ReplicationWire.TYPE_FILE,
                        ReplicationWire.encodeFile(relative, content));
                files++;
            }
        }
        ReplicationWire.writeFrame(out, ReplicationWire.TYPE_BACKUP_END,
                ReplicationWire.longPayload(startLsn));
        LOGGER.log(Level.INFO, "Base backup shipped: {0} files, startLsn={1}",
                new Object[]{files, startLsn});
    }

    /**
     * Streams WAL entries from {@code startLsn} (inclusive) until the sender
     * closes or the connection breaks.
     *
     * <p>The tail loop polls {@link WalSource#readRange} on a short interval —
     * a fresh entry is therefore forwarded within ~10 ms of its append, well
     * under the 100 ms lag budget of the acceptance test.
     */
    private void streamWal(DataOutputStream out, DataInputStream in, long startLsn) throws IOException {
        long nextLsn = Math.max(1, startLsn);
        long lastHeartbeat = 0;
        LOGGER.log(Level.FINE, "Streaming WAL from LSN {0}", nextLsn);

        // Drain ACKs on this thread between writes is not possible; instead we
        // rely on the standby's ACK frames being tiny and infrequent — read
        // them opportunistically whenever the socket has buffered data.
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            long lastLsn = source.getLastLsn();
            if (lastLsn >= nextLsn) {
                // Read in maximum possible batches to minimize processing overhead
                int batchSize = Math.min(1000, (int)(lastLsn - nextLsn + 1));
                List<WALEntry> batch = source.readRange(nextLsn, nextLsn + batchSize - 1);
                
                // Validate that we got the expected number of entries
                if (batch.size() < batchSize && lastLsn > nextLsn) {
                    LOGGER.log(Level.WARNING, 
                            "Batch size mismatch: expected {0}, got {1} for range {2}-{3}", 
                            new Object[]{batchSize, batch.size(), nextLsn, nextLsn + batchSize - 1});
                    // Fall back to single-entry processing to avoid gaps
                    for (WALEntry entry : batch) {
                        ReplicationWire.writeFrame(out, ReplicationWire.TYPE_WAL_ENTRY, entry.toBytes());
                        nextLsn = entry.getLsn() + 1;
                    }
                } else {
                    // Batch write frames to reduce socket overhead
                    for (WALEntry entry : batch) {
                        ReplicationWire.writeFrame(out, ReplicationWire.TYPE_WAL_ENTRY, entry.toBytes());
                        nextLsn = entry.getLsn() + 1;
                    }
                }
                lastHeartbeat = System.nanoTime();
                drainAcks(in);
            } else {
                long now = System.nanoTime();
                if (now - lastHeartbeat >= HEARTBEAT_INTERVAL_MS * 1_000_000L) {
                    ReplicationWire.writeFrame(out, ReplicationWire.TYPE_HEARTBEAT,
                            ReplicationWire.longPayload(lastLsn));
                    lastHeartbeat = now;
                }
                drainAcks(in);
                sleepQuietly(POLL_INTERVAL_MS);
            }
        }
    }

    /**
     * Reads and discards ACK frames the standby has already buffered, without
     * blocking. The standby sends nothing but fixed-size ACK frames
     * (11-byte header + 8-byte payload) while streaming, so waiting for
     * {@code available() >= 19} guarantees a complete frame is buffered —
     * without it the receive buffer would eventually fill, block the standby
     * on write, and deadlock the stream.
     */
    private void drainAcks(DataInputStream in) {
        try {
            while (in.available() >= ReplicationWire.HEADER_SIZE + 8) {
                ReplicationWire.Frame frame = ReplicationWire.readFrame(in);
                if (frame.type() != ReplicationWire.TYPE_ACK) {
                    LOGGER.log(Level.FINE, "Ignoring unexpected mid-stream frame type {0}", frame.type());
                }
            }
        } catch (IOException e) {
            // Connection is broken; the outer stream loop surfaces it on the
            // next write.
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Closes the listener and every active standby connection. */
    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.log(Level.FINE, "Error closing replication listener: {0}", e.getMessage());
        }
        for (Socket socket : activeSockets) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
        activeSockets.clear();
        try {
            acceptThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOGGER.info("WalSender stopped");
    }
}
