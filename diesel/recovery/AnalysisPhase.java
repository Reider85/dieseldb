package diesel.recovery;

import diesel.wal.WALEntry;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import diesel.wal.WALSegment;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ARIES analysis phase: scans the WAL from the last checkpoint to the end of the
 * log and rebuilds the set of committed and active transactions.
 *
 * <p>Algorithm (ARIES, prompt 4 #17 / R3-004 step 2/4):
 * <ol>
 *   <li>Determine the scan window: {@code startLsn = checkpoint.lastLSN + 1}
 *       (the checkpoint entry itself is included), {@code endLsn = wal.getLastLsn()}
 *       captured once, so a concurrently growing WAL does not change the snapshot.</li>
 *   <li>Seed the active set from {@link CheckpointRecord#getActiveTxids()} (empty
 *       when there is no checkpoint).</li>
 *   <li>Scan every WAL entry with {@code startLsn <= lsn <= endLsn}:
 *     <ul>
 *       <li>{@code BEGIN} / DML ({@code INSERT/UPDATE/DELETE/TRUNCATE}) → txid becomes active
 *           (unless it already committed);</li>
 *       <li>{@code COMMIT} → txid moves to the committed set and leaves the active set;</li>
 *       <li>{@code ABORT} → txid leaves the active set (never committed);</li>
 *       <li>{@code CHECKPOINT} → the active set is re-seeded from the checkpoint's
 *           embedded active-txid list (fuzzy-checkpoint semantics; the committed set
 *           keeps accumulating).</li>
 *     </ul>
 *   </li>
 *   <li>Return {@code {committed, active, lastLSN}}.</li>
 * </ol>
 *
 * <p>Scanning is done segment-by-segment ({@link WALSegment#readAll()}) so only one
 * segment body is materialized at a time — memory stays bounded by the segment size
 * even for multi-gigabyte WALs.
 *
 * <p>Consumers: the redo phase (steps with {@code checkpoint.lastLSN < lsn <= lastLSN})
 * and the undo phase (rolls back the {@code active} set) — prompts 4 #18 and #19.
 *
 * @see AnalysisResult
 * @see CheckpointRecord
 */
public final class AnalysisPhase {

    private static final Logger LOGGER = Logger.getLogger(AnalysisPhase.class.getName());

    private AnalysisPhase() {
        // utility class
    }

    /**
     * Runs the analysis phase using the checkpoint referenced by {@code checkpoint.ptr}
     * (or a full-log scan when no checkpoint exists).
     *
     * @param wal the open WAL manager to scan
     * @return the analysis result
     * @throws IOException if reading the checkpoint pointer or WAL segments fails
     */
    public static AnalysisResult analyze(WALManager wal) throws IOException {
        Objects.requireNonNull(wal, "wal");
        return analyze(wal, wal.loadCheckpointRecord());
    }

    /**
     * Runs the analysis phase from the given checkpoint to the end of the log.
     *
     * @param wal        the open WAL manager to scan
     * @param checkpoint the checkpoint to start from, or {@code null} to scan the
     *                   whole log from the beginning
     * @return the analysis result
     * @throws IOException if reading WAL segments fails
     */
    public static AnalysisResult analyze(WALManager wal, CheckpointRecord checkpoint) throws IOException {
        Objects.requireNonNull(wal, "wal");

        // Snapshot the end of the log once: entries appended after this point are
        // not part of this analysis run.
        long endLsn = wal.getLastLsn();
        long startLsn = checkpoint != null ? checkpoint.getLastLSN() + 1 : 1L;

        Set<Long> active = new LinkedHashSet<>();
        Set<Long> committed = new LinkedHashSet<>();
        if (checkpoint != null) {
            for (long txid : checkpoint.getActiveTxids()) {
                active.add(txid);
            }
        }

        // Segment-by-segment scan: only one segment body is materialized at a time.
        for (WALSegment segment : wal.getSegments().values()) {
            if (segment.getFirstLsn() > endLsn && segment.getFirstLsn() != 0) {
                break; // segment created after our end-of-log snapshot
            }
            List<WALEntry> entries = segment.readAll();
            for (WALEntry entry : entries) {
                long lsn = entry.getLsn();
                if (lsn > endLsn) {
                    break;
                }
                if (lsn < startLsn) {
                    continue;
                }
                apply(entry, committed, active);
            }
        }

        return new AnalysisResult(committed, active, endLsn);
    }

    /**
     * Folds a single WAL entry into the analysis sets.
     *
     * @param entry     the WAL entry (within the scan window)
     * @param committed the evolving committed set
     * @param active    the evolving active set
     */
    private static void apply(WALEntry entry, Set<Long> committed, Set<Long> active) {
        WALOpcode op = entry.getOp();
        long txid = entry.getTxid();

        if (op == WALOpcode.CHECKPOINT) {
            // Fuzzy-checkpoint semantics: the checkpoint's embedded active list
            // replaces the active set; the committed set keeps accumulating.
            try {
                CheckpointRecord record = CheckpointRecord.fromBytes(entry.getAfterImage());
                active.clear();
                for (long checkpointTxid : record.getActiveTxids()) {
                    active.add(checkpointTxid);
                }
            } catch (CheckpointFormatException e) {
                LOGGER.log(Level.WARNING, "Skipping corrupt CHECKPOINT record at LSN "
                        + entry.getLsn() + ": " + e.getMessage());
            }
            return;
        }

        if (txid <= 0) {
            return; // system record with no transaction context
        }

        switch (op) {
            case COMMIT -> {
                committed.add(txid);
                active.remove(txid);
            }
            case ABORT -> active.remove(txid);
            default -> {
                // BEGIN and every DML record make the transaction active
                // (defensive: logs written before BEGIN emission only carry DML).
                if (!committed.contains(txid)) {
                    active.add(txid);
                }
            }
        }
    }
}
