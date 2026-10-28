package diesel.recovery;

import diesel.wal.CommitPayload;
import diesel.wal.DmlPayload;
import diesel.wal.WALEntry;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Point-in-time recovery driver (prompt 4 #43 / R3-011 step 4/4): replays the
 * WAL window after a backup up to a target timestamp so the restored database
 * reflects the committed state at that instant, aborting every transaction
 * that was still active (or committed later) at the target.
 *
 * <p>Algorithm (pure WAL logic, engine state lives behind {@link PitrSink} —
 * the {@code ARIESAlgorithm}/{@code DatabaseRecoverySink} split):
 * <ol>
 *   <li><b>Window.</b> {@code readRange(lastBackupLsn + 1, lastLsn)} — every
 *       record written after the backup;</li>
 *   <li><b>Classification.</b> A transaction is <i>visible</i> when its COMMIT
 *       payload carries {@code commitTimestamp <= target}; committed-later,
 *       aborted and never-committed (active) transactions are <i>hidden</i>.
 *       Legacy COMMIT payloads without a timestamp (pre-PITR WAL segments)
 *       are treated as visible with a warning — documented deviation;</li>
 *   <li><b>Apply (forward LSN order).</b> Visible INSERT/UPDATE after-images
 *       are applied; INSERTs build a remap table (source row index → restored
 *       append position) because hidden transactions' skipped rows shift every
 *       later append. Visible COMMIT payloads finalize the transaction
 *       (tombstones + commit-CSN stamping + tracker registration);</li>
 *   <li><b>Hide (reverse LSN order).</b> Hidden INSERT rows physically present
 *       in the restored files (a write-behind flush persisted them before the
 *       backup) are tombstoned — the {@code deletedRows} bitset is serialized,
 *       unlike transient version metadata, so the hide survives reopen — and
 *       hidden UPDATEs restore their before-image;</li>
 *   <li><b>Abort.</b> Every hidden transaction is registered ABORTED in the
 *       status tracker.</li>
 * </ol>
 *
 * <p>Guards mirror the crash-recovery sink policy: range checks and lenient
 * value comparisons mean a mismatched row is skipped with a warning instead of
 * being corrupted. Transaction-accurate restore target: state = all commits
 * whose commit timestamp is at or before the target; per-record timestamps do
 * not exist in the WAL format.
 */
public final class PointInTimeRecovery {

    private static final Logger LOGGER = Logger.getLogger(PointInTimeRecovery.class.getName());

    private PointInTimeRecovery() {
        // utility class
    }

    /**
     * Replays the WAL window after {@code lastBackupLsn} up to
     * {@code targetTimestampMs} through the given sink.
     *
     * @param wal                the open WAL manager holding the post-backup segments
     * @param lastBackupLsn      the backup's high-water LSN (records at or below are skipped)
     * @param targetTimestampMs  the restore target in epoch millis
     * @param sink               the engine-bound applier
     * @return the pass statistics
     * @throws IOException if reading the WAL or applying a record fails
     */
    public static PitrResult recover(WALManager wal, long lastBackupLsn,
                                     long targetTimestampMs, PitrSink sink)
            throws IOException {
        Objects.requireNonNull(wal, "wal");
        Objects.requireNonNull(sink, "sink");

        List<WALEntry> window = readWindow(wal, lastBackupLsn);
        if (window.isEmpty()) {
            LOGGER.info(() -> "PITR: no WAL records after LSN " + lastBackupLsn
                    + " — restored state equals the backup");
            return new PitrResult(targetTimestampMs, wal.getLastLsn(), 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        // ── Classification pass ─────────────────────────────────────────
        Map<Long, CommitPayload> commits = new HashMap<>();
        Set<Long> hidden = new LinkedHashSet<>();
        long maxTxid = 0;
        long maxCommitCsn = 0;
        long legacyCommits = 0;

        for (WALEntry entry : window) {
            long txid = entry.getTxid();
            maxTxid = Math.max(maxTxid, txid);
            WALOpcode op = entry.getOp();
            if (op == WALOpcode.COMMIT) {
                CommitPayload payload = decodeCommit(entry);
                if (payload == null) {
                    hidden.add(txid); // undecodable: never apply its effects
                    continue;
                }
                long commitTs = payload.getCommitTimestampMs();
                boolean visible;
                if (commitTs == 0) {
                    // Legacy pre-PITR payload: no timestamp, cannot decide.
                    visible = true;
                    legacyCommits++;
                    LOGGER.warning(() -> "PITR: legacy COMMIT without timestamp for txid "
                            + txid + " (LSN " + entry.getLsn() + ") — treated as visible");
                } else {
                    visible = commitTs <= targetTimestampMs;
                }
                if (visible) {
                    commits.put(txid, payload);
                    hidden.remove(txid);
                    maxCommitCsn = Math.max(maxCommitCsn, payload.getCommitCsn());
                } else {
                    commits.remove(txid);
                    hidden.add(txid);
                }
            } else if (op == WALOpcode.ABORT) {
                commits.remove(txid);
                hidden.add(txid);
            }
        }
        // Transactions seen in DML but without COMMIT/ABORT in the window are
        // active at the target — hidden (never committed at/before target).
        for (WALEntry entry : window) {
            WALOpcode op = entry.getOp();
            if (op == WALOpcode.INSERT || op == WALOpcode.UPDATE || op == WALOpcode.DELETE) {
                long txid = entry.getTxid();
                if (!commits.containsKey(txid)) {
                    hidden.add(txid);
                }
            }
        }

        // ── Apply pass (forward LSN order, visible transactions) ────────
        Map<String, Map<Integer, Integer>> remap = new HashMap<>();
        long appliedInserts = 0;
        long appliedUpdates = 0;
        long commitTombstones = 0;
        long skipped = 0;

        for (WALEntry entry : window) {
            long txid = entry.getTxid();
            if (hidden.contains(txid)) {
                continue;
            }
            WALOpcode op = entry.getOp();
            switch (op) {
                case INSERT -> {
                    DmlPayload after = decodeDml(entry.getAfterImage(), entry.getLsn());
                    if (after == null) {
                        skipped++;
                        continue;
                    }
                    int sourceIndex = after.getRowIndex();
                    int actual = sink.applyInsert(txid, after.getTableName(),
                            resolve(remap, after.getTableName(), sourceIndex), after.getValues());
                    if (actual >= 0) {
                        remap.computeIfAbsent(after.getTableName(), k -> new HashMap<>())
                                .put(sourceIndex, actual);
                    }
                    appliedInserts++;
                }
                case UPDATE -> {
                    DmlPayload before = decodeDml(entry.getBeforeImage(), entry.getLsn());
                    DmlPayload after = decodeDml(entry.getAfterImage(), entry.getLsn());
                    if (before == null || after == null) {
                        skipped++;
                        continue;
                    }
                    int applied = sink.applyUpdate(txid, after.getTableName(),
                            resolve(remap, after.getTableName(), after.getRowIndex()),
                            before.getValues(), after.getValues());
                    if (applied > 0) {
                        appliedUpdates++;
                    } else {
                        skipped++;
                    }
                }
                case COMMIT -> {
                    CommitPayload payload = commits.get(txid);
                    if (payload == null) {
                        continue; // reclassified mid-pass; defensive
                    }
                    CommitPayload remapped = remapPayload(payload, remap);
                    commitTombstones += sink.onCommit(remapped);
                }
                default -> {
                    // BEGIN/CHECKPOINT/PAGE_IMAGE/SAVEPOINT/TRUNCATE: no PITR effect
                }
            }
        }

        // ── Hide pass (reverse LSN order, hidden transactions) ──────────
        long hiddenTombstones = 0;
        long hiddenRestores = 0;
        for (int i = window.size() - 1; i >= 0; i--) {
            WALEntry entry = window.get(i);
            long txid = entry.getTxid();
            if (!hidden.contains(txid)) {
                continue;
            }
            WALOpcode op = entry.getOp();
            try {
                if (op == WALOpcode.INSERT) {
                    DmlPayload after = decodeDml(entry.getAfterImage(), entry.getLsn());
                    if (after == null) {
                        continue;
                    }
                    hiddenTombstones += sink.hideInsert(txid, after.getTableName(),
                            after.getRowIndex(), after.getValues());
                } else if (op == WALOpcode.UPDATE) {
                    DmlPayload before = decodeDml(entry.getBeforeImage(), entry.getLsn());
                    DmlPayload after = decodeDml(entry.getAfterImage(), entry.getLsn());
                    if (before == null || after == null) {
                        continue;
                    }
                    hiddenRestores += sink.hideUpdate(txid, after.getTableName(),
                            after.getRowIndex(), before.getValues(), after.getValues());
                }
            } catch (IOException e) {
                // A hide failure must not abort the pass: the row stays as the
                // backup left it (same tolerance as the crash-recovery undo).
                LOGGER.log(Level.WARNING, "PITR: hide failed for txid " + txid
                        + " at LSN " + entry.getLsn() + ": " + e.getMessage(), e);
                skipped++;
            }
        }

        // ── Abort hidden transactions in the tracker ────────────────────
        for (Long txid : hidden) {
            sink.onAbort(txid);
        }

        PitrResult result = new PitrResult(targetTimestampMs, wal.getLastLsn(),
                commits.size(), hidden.size(), appliedInserts, appliedUpdates,
                hiddenTombstones, hiddenRestores, commitTombstones,
                skipped, legacyCommits, maxTxid, maxCommitCsn);
        LOGGER.info(() -> "PITR done: " + result);
        return result;
    }

    /** Reads the post-backup WAL window in ascending LSN order. */
    private static List<WALEntry> readWindow(WALManager wal, long lastBackupLsn) throws IOException {
        long endLsn = wal.getLastLsn();
        if (endLsn <= lastBackupLsn) {
            return List.of();
        }
        return new ArrayList<>(wal.readRange(lastBackupLsn + 1, endLsn));
    }

    /** Resolves a source row index through the insert-remap table (identity when unknown). */
    private static int resolve(Map<String, Map<Integer, Integer>> remap, String table, int sourceIndex) {
        Map<Integer, Integer> byTable = remap.get(table);
        if (byTable == null) {
            return sourceIndex;
        }
        return byTable.getOrDefault(sourceIndex, sourceIndex);
    }

    /** Rebuilds a commit payload with remapped row indexes. */
    private static CommitPayload remapPayload(CommitPayload payload,
                                              Map<String, Map<Integer, Integer>> remap) {
        return new CommitPayload(payload.getTxid(), payload.getCommitCsn(),
                remapRows(payload.getModifiedRows(), remap),
                remapRows(payload.getDeletedRows(), remap),
                payload.getCommitTimestampMs());
    }

    private static Map<String, java.util.Collection<Integer>> remapRows(
            Map<String, int[]> rows, Map<String, Map<Integer, Integer>> remap) {
        if (rows.isEmpty()) {
            return new java.util.LinkedHashMap<>();
        }
        Map<String, java.util.Collection<Integer>> out = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, int[]> entry : rows.entrySet()) {
            int[] indexes = entry.getValue();
            java.util.List<Integer> mapped = new java.util.ArrayList<>(indexes.length);
            for (int i = 0; i < indexes.length; i++) {
                mapped.add(resolve(remap, entry.getKey(), indexes[i]));
            }
            out.put(entry.getKey(), mapped);
        }
        return out;
    }

    /** Decodes a COMMIT payload; {@code null} on a legacy/corrupt record. */
    private static CommitPayload decodeCommit(WALEntry entry) {
        if (!entry.hasAfterImage()) {
            return null;
        }
        try {
            return CommitPayload.deserialize(entry.getAfterImage());
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "PITR: skipping undecodable COMMIT at LSN "
                    + entry.getLsn() + ": " + e.getMessage());
            return null;
        }
    }

    /** Decodes a DML payload; {@code null} on a legacy/corrupt record. */
    private static DmlPayload decodeDml(byte[] data, long lsn) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            return DmlPayload.deserialize(data);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "PITR: skipping undecodable DML payload at LSN "
                    + lsn + ": " + e.getMessage());
            return null;
        }
    }
}
