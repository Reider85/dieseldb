package diesel;

import diesel.replication.StreamingApplier;
import diesel.replication.WalApplier;
import diesel.replication.WalReceiver;
import diesel.replication.WalSender;
import diesel.replication.WalSource;
import diesel.wal.WALEntry;
import diesel.wal.WALManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Binds physical streaming replication to the engine (prompt4 #58,
 * R3-016 step 1/5).
 *
 * <p>Lives in {@code diesel} (not {@code diesel.replication}) because
 * {@code Database} is package-private — the same split as
 * {@link DatabaseRecoverySink} and {@link DatabasePitrSink}. It exposes:
 * <ul>
 *   <li>{@link #startMaster} — a {@link WalSender} over this database's WAL
 *       and data directory, including the quiesced base-backup preparation.</li>
 *   <li>{@link #startStandby} — bootstrap (base backup when the local
 *       directory is empty, resume marker otherwise), a
 *       {@link StreamingApplier} over {@link DatabasePitrSink}, the
 *       {@link WalReceiver} stream, hot-standby mode, and periodic
 *       file+marker persistence so a standby restart resumes exactly where
 *       its files end.</li>
 * </ul>
 *
 * <p><b>Base-backup consistency contract.</b> {@link DatabaseWalSource#prepareBackup}
 * waits until no transaction is active, flushes every table
 * ({@link Database#saveTablesToDisk}) and only then snapshots the resume LSN.
 * With a quiesced master the copied files and the resume LSN describe the same
 * state; transactions racing a non-quiesced backup are tolerated by the
 * applier's value guards (documented limitation — bootstrap a production
 * standby from a quiet master).
 */
public final class ReplicationCoordinator implements AutoCloseable {

    private static final Logger LOGGER = Logger.getLogger(ReplicationCoordinator.class.getName());

    /** Resume marker file inside the standby data directory. */
    static final String MARKER_FILE = "replication.lsn";
    /** Persist files+marker every N applied entries (bounds restart replay). */
    static final int PERSIST_EVERY_ENTRIES = 100;

    private final Database database;

    private WalSender sender;
    private WalReceiver receiver;
    private StandbyApplier standbyApplier;

    private ReplicationCoordinator(Database database) {
        this.database = Objects.requireNonNull(database, "database");
    }

    // ── master side ─────────────────────────────────────────────────────

    /**
     * Starts a {@link WalSender} serving this database's WAL on the given
     * port.
     *
     * @param database the master database (WAL must be enabled)
     * @param port     replication listen port (0 = ephemeral)
     * @return the coordinator wrapping the running sender
     * @throws IOException if the port cannot be bound or WAL is unavailable
     */
    public static ReplicationCoordinator startMaster(Database database, int port) throws IOException {
        ReplicationCoordinator coordinator = new ReplicationCoordinator(database);
        if (!database.isWALEnabled() || database.getWalManager() == null) {
            throw new IOException("Physical replication requires wal.enabled=true on the master");
        }
        coordinator.sender = new WalSender(new DatabaseWalSource(database), port);
        return coordinator;
    }

    /** Returns the sender's actual listen port (master side only). */
    public int getMasterPort() {
        if (sender == null) {
            throw new IllegalStateException("Not a master coordinator");
        }
        return sender.getPort();
    }

    /**
     * Drops every active standby connection without stopping the listener —
     * test hook that simulates a network partition (prompt4 #58 ReconnectTest).
     *
     * @return the number of connections dropped
     */
    public int disconnectAllPeers() {
        if (sender == null) {
            throw new IllegalStateException("Not a master coordinator");
        }
        return sender.disconnectAllClients();
    }

    /**
     * One-shot base backup used by a standby that has no local data yet.
     *
     * @param host      master host
     * @param port      master replication port
     * @param targetDir standby data directory
     * @return the resume LSN (also persisted as the standby's marker)
     * @throws IOException on connection or write failure
     */
    public static long bootstrapStandby(String host, int port, Path targetDir) throws IOException {
        long startLsn = WalReceiver.fetchBaseBackup(host, port, targetDir);
        writeMarker(targetDir, startLsn);
        return startLsn;
    }

    /**
     * Fetches a base backup only when the target directory holds no table yet
     * — called by {@code DatabaseServer} <em>before</em> the catalog and tables
     * are loaded so a fresh standby boots from master state instead of an
     * empty directory.
     *
     * @param host      master host
     * @param port      master replication port
     * @param targetDir standby data directory
     * @return the resume LSN, or -1 when local data already exists
     * @throws IOException on connection or write failure
     */
    public static long bootstrapStandbyIfEmpty(String host, int port, Path targetDir)
            throws IOException {
        if (Files.isDirectory(targetDir) && hasTableFiles(targetDir)) {
            return -1;
        }
        return bootstrapStandby(host, port, targetDir);
    }

    // ── standby side ────────────────────────────────────────────────────

    /**
     * Starts streaming replication into the given standby database.
     *
     * <p>Bootstrap decision:
     * <ul>
     *   <li>no {@code .table} files locally → base backup from the master
     *       (marker written at the backup LSN);</li>
     *   <li>files + marker → resume at {@code marker + 1};</li>
     *   <li>files without marker → full WAL replay from LSN 1 (idempotent
     *       apply makes the overlap safe).</li>
     * </ul>
     *
     * @param database the standby database (opened on the target directory)
     * @param host     master host
     * @param port     master replication port
     * @return the coordinator wrapping the running receiver
     * @throws IOException if the initial base backup fails (later reconnects
     *                     are handled by the receiver's retry loop)
     */
    public static ReplicationCoordinator startStandby(Database database, String host, int port)
            throws IOException {
        ReplicationCoordinator coordinator = new ReplicationCoordinator(database);
        Path dataDir = Path.of(database.getDataDir());

        long initialAppliedLsn;
        if (!hasTableFiles(dataDir)) {
            initialAppliedLsn = bootstrapStandby(host, port, dataDir);
            LOGGER.log(Level.INFO, "Standby bootstrapped from base backup, resume LSN {0}",
                    initialAppliedLsn);
        } else {
            long marker = readMarker(dataDir);
            initialAppliedLsn = Math.max(0, marker);
            LOGGER.log(Level.INFO, "Standby local data found, resume marker {0}", initialAppliedLsn);
        }

        StreamingApplier applier = new StreamingApplier(new DatabasePitrSink(database));
        applier.setInitialLsn(initialAppliedLsn);
        coordinator.standbyApplier = new StandbyApplier(database, applier);
        coordinator.receiver = new WalReceiver(host, port, coordinator.standbyApplier);
        database.setHotStandbyMode(true);
        coordinator.receiver.start();
        return coordinator;
    }

    /** Returns the standby receiver (standby side only). */
    public WalReceiver getReceiver() {
        if (receiver == null) {
            throw new IllegalStateException("Not a standby coordinator");
        }
        return receiver;
    }

    /** Returns the streaming applier with its diagnostics (standby side). */
    public StreamingApplier getApplier() {
        if (standbyApplier == null) {
            throw new IllegalStateException("Not a standby coordinator");
        }
        return standbyApplier.delegate;
    }

    /** Returns true while the standby holds a live streaming connection. */
    public boolean isConnected() {
        return receiver != null && receiver.isConnected();
    }

    @Override
    public void close() {
        if (receiver != null) {
            receiver.close();
        }
        if (standbyApplier != null) {
            standbyApplier.persistNow();
        }
        if (sender != null) {
            sender.close();
        }
        if (receiver != null || sender != null) {
            database.setHotStandbyMode(false);
        }
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private static boolean hasTableFiles(Path dataDir) {
        try (var files = Files.list(dataDir)) {
            return files.anyMatch(p -> p.getFileName().toString().endsWith(ErrorMessages.TABLE_EXTENSION));
        } catch (IOException e) {
            return false; // unreadable/missing dir → treat as empty → base backup
        }
    }

    static long readMarker(Path dataDir) {
        Path marker = dataDir.resolve(MARKER_FILE);
        if (!Files.isRegularFile(marker)) {
            return -1;
        }
        try {
            return Long.parseLong(Files.readString(marker).trim());
        } catch (IOException | NumberFormatException e) {
            LOGGER.log(Level.WARNING, "Unreadable replication marker {0}: {1}",
                    new Object[]{marker, e.getMessage()});
            return -1;
        }
    }

    static void writeMarker(Path dataDir, long lsn) {
        try {
            Files.createDirectories(dataDir);
            Files.writeString(dataDir.resolve(MARKER_FILE), Long.toString(lsn));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to write replication marker: {0}", e.getMessage());
        }
    }

    /**
     * Master-side {@link WalSource} over {@link Database}'s WAL and data
     * directory.
     */
    private record DatabaseWalSource(Database database) implements WalSource {

        @Override
        public long getLastLsn() {
            WALManager wal = database.getWalManager();
            return wal == null ? 0 : wal.getLastLsn();
        }

        @Override
        public List<WALEntry> readRange(long fromInclusive, long toInclusive) throws IOException {
            WALManager wal = database.getWalManager();
            if (wal == null) {
                return List.of();
            }
            return wal.readRange(fromInclusive, toInclusive);
        }

        @Override
        public Path getDataDir() {
            return Path.of(database.getDataDir());
        }

        @Override
        public long prepareBackup() throws IOException {
            // Quiesce first: with no active transaction the flushed files and
            // the LSN snapshotted below describe the same state, so nothing is
            // neither missing from the files nor skipped by the stream.
            long deadline = System.nanoTime() + 5_000_000_000L;
            while (database.getActiveTransactionCount() > 0) {
                if (System.nanoTime() > deadline) {
                    LOGGER.log(Level.WARNING,
                            "Base backup: master not quiesced after 5s, proceeding anyway");
                    break;
                }
                WalReceiver.sleep(10);
            }
            database.saveTablesToDisk();
            var pages = database.getPageManager();
            if (pages != null) {
                try {
                    pages.flush();
                } catch (IOException e) {
                    LOGGER.log(Level.FINE, "Page flush during backup failed: {0}", e.getMessage());
                }
            }
            WALManager wal = database.getWalManager();
            return wal == null ? 0 : wal.getLastLsn();
        }
    }

    /**
     * Standby-side {@link WalApplier}: delegates to {@link StreamingApplier}
     * and periodically persists the row files together with the resume
     * marker — in that order, so the marker never claims a state the files do
     * not contain (a restart replays at most the un-persisted tail).
     */
    private static final class StandbyApplier implements WalApplier {

        private final Database database;
        private final StreamingApplier delegate;
        private long entriesSincePersist;
        private boolean dirty;

        private StandbyApplier(Database database, StreamingApplier delegate) {
            this.database = database;
            this.delegate = delegate;
        }

        @Override
        public void apply(WALEntry entry) throws IOException {
            delegate.apply(entry);
            dirty = true;
            if (++entriesSincePersist >= PERSIST_EVERY_ENTRIES) {
                persistNow();
            }
        }

        @Override
        public long getAppliedLsn() {
            return delegate.getAppliedLsn();
        }

        /**
         * Persists the row files first, then the resume marker — the marker
         * therefore never claims a state the files do not contain, and a
         * restart replays at most the un-persisted tail (idempotent guards
         * absorb any overlap).
         */
        private void persistNow() {
            if (!dirty) {
                return;
            }
            try {
                database.saveTablesToDisk();
                writeMarker(Path.of(database.getDataDir()), delegate.getAppliedLsn());
                entriesSincePersist = 0;
                dirty = false;
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Standby persist failed: {0}", e.getMessage());
            }
        }
    }
}
