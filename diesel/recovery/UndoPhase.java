package diesel.recovery;

import diesel.wal.DmlPayload;
import diesel.wal.WALEntry;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import diesel.wal.WALSegment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ARIES undo phase: reverses every logical DML record of a transaction that
 * is still active (no COMMIT in the log), so uncommitted work becomes
 * invisible after a crash (prompt 4 #19 / R3-004 step 4/4).
 *
 * <p>Algorithm:
 * <ol>
 *   <li>Take the active set produced by {@link AnalysisPhase}.</li>
 *   <li>Scan the WAL segments in <b>reverse LSN order</b> (newest segment
 *       first, entries within a segment backwards). Unlike analysis/redo the
 *       undo pass covers the <b>whole log</b>, not just the post-checkpoint
 *       window: an active transaction's pre-checkpoint operations are just as
 *       uncommitted and must be reversed too.</li>
 *   <li>For each {@code INSERT}/{@code UPDATE}/{@code DELETE} record whose
 *       txid is in the active set, decode the {@link DmlPayload} images and
 *       deliver them to the {@link MvccUndoSink}:
 *     <ul>
 *       <li>{@code INSERT} → after-image → {@link MvccUndoSink#onInsertUndo};</li>
 *       <li>{@code UPDATE} → before + after images →
 *           {@link MvccUndoSink#onUpdateUndo};</li>
 *       <li>{@code DELETE} → before-image → {@link MvccUndoSink#onDeleteUndo}.</li>
 *     </ul>
 *   </li>
 *   <li>Everything else ({@code BEGIN}, {@code ABORT}, {@code COMMIT},
 *       {@code CHECKPOINT}, {@code PAGE_IMAGE}, DML of committed transactions,
 *       undecodable payloads) is counted as ignored — undo never fails the
 *       recovery pass.</li>
 * </ol>
 *
 * <p>Reverse delivery guarantees that chained changes to the same row unwind
 * correctly: the newest change is reversed first, so an insert-then-update
 * chain restores the inserted values and finally hides the row.
 *
 * <p><b>No CLRs.</b> Unlike classical ARIES, this phase writes no
 * Compensation Log Records: undo runs exactly once at startup, before any
 * client is accepted, so a crash cannot interrupt it halfway (documented
 * deviation, doc/wal/undo.md).
 *
 * <p>Memory stays bounded by one segment body: only the entries of the
 * current segment are materialized at a time.
 *
 * @see UndoResult
 * @see MvccUndoSink
 */
public final class UndoPhase {

    private static final Logger LOGGER = Logger.getLogger(UndoPhase.class.getName());

    private UndoPhase() {
        // utility class
    }

    /**
     * Runs the undo phase: reverses the logical DML records of every active
     * transaction, newest first.
     *
     * @param wal          the open WAL manager
     * @param activeTxids  the active set produced by the analysis phase
     * @param sink         receiver of the logical undo operations
     * @return the undo result
     * @throws IOException if reading the WAL or the sink fails
     */
    public static UndoResult undo(WALManager wal, Set<Long> activeTxids, MvccUndoSink sink)
            throws IOException {
        Objects.requireNonNull(wal, "wal");
        Objects.requireNonNull(activeTxids, "activeTxids");
        Objects.requireNonNull(sink, "sink");

        if (activeTxids.isEmpty()) {
            // Nothing is active: the whole log can be skipped without reading it.
            return new UndoResult(0, 0, 0, 0, wal.getLastLsn());
        }

        long endLsn = wal.getLastLsn();
        long undoneInserts = 0;
        long undoneUpdates = 0;
        long undoneDeletes = 0;
        long ignored = 0;

        // Reverse segment order: newest segment number first.
        List<Integer> segmentNumbers = new ArrayList<>(wal.getSegments().keySet());
        Collections.sort(segmentNumbers, Comparator.reverseOrder());

        for (Integer segmentNumber : segmentNumbers) {
            WALSegment segment = wal.getSegments().get(segmentNumber);
            if (segment.getFirstLsn() > endLsn && segment.getFirstLsn() != 0) {
                continue; // segment created after our end-of-log snapshot
            }
            List<WALEntry> entries = segment.readAll();
            // Entries within a segment are LSN-ascending: walk them backwards.
            for (int i = entries.size() - 1; i >= 0; i--) {
                WALEntry entry = entries.get(i);
                long lsn = entry.getLsn();
                if (lsn > endLsn) {
                    continue; // appended after the snapshot
                }
                if (!activeTxids.contains(entry.getTxid())) {
                    // Committed/aborted/system transaction: scanned, no undo work.
                    ignored++;
                    continue;
                }
                switch (entry.getOp()) {
                    case INSERT -> {
                        if (!entry.hasAfterImage()) {
                            LOGGER.log(Level.WARNING,
                                    "Skipping INSERT record without after-image at LSN " + lsn);
                            ignored++;
                            continue;
                        }
                        DmlPayload payload = decode(entry.getAfterImage(), lsn);
                        if (payload == null) {
                            ignored++;
                            continue;
                        }
                        sink.onInsertUndo(entry.getTxid(), payload.getTableName(),
                                payload.getRowIndex(), payload.getValues());
                        undoneInserts++;
                    }
                    case UPDATE -> {
                        if (!entry.hasBeforeImage() || !entry.hasAfterImage()) {
                            LOGGER.log(Level.WARNING,
                                    "Skipping UPDATE record with missing image at LSN " + lsn);
                            ignored++;
                            continue;
                        }
                        DmlPayload before = decode(entry.getBeforeImage(), lsn);
                        DmlPayload after = decode(entry.getAfterImage(), lsn);
                        if (before == null || after == null) {
                            ignored++;
                            continue;
                        }
                        sink.onUpdateUndo(entry.getTxid(), after.getTableName(),
                                after.getRowIndex(), before.getValues(), after.getValues());
                        undoneUpdates++;
                    }
                    case DELETE -> {
                        if (!entry.hasBeforeImage()) {
                            LOGGER.log(Level.WARNING,
                                    "Skipping DELETE record without before-image at LSN " + lsn);
                            ignored++;
                            continue;
                        }
                        DmlPayload payload = decode(entry.getBeforeImage(), lsn);
                        if (payload == null) {
                            ignored++;
                            continue;
                        }
                        sink.onDeleteUndo(entry.getTxid(), payload.getTableName(),
                                payload.getRowIndex(), payload.getValues());
                        undoneDeletes++;
                    }
                    default -> {
                        // BEGIN/ABORT/COMMIT/CHECKPOINT/PAGE_IMAGE: no undo work.
                        ignored++;
                    }
                }
            }
        }

        return new UndoResult(undoneInserts, undoneUpdates, undoneDeletes, ignored, endLsn);
    }

    /**
     * Decodes a DML payload, treating a corrupt record as ignorable rather
     * than fatal (same policy as the redo phase's COMMIT payloads).
     *
     * @param data the encoded payload
     * @param lsn  the record LSN (for the warning message)
     * @return the decoded payload, or {@code null} when undecodable
     */
    private static DmlPayload decode(byte[] data, long lsn) {
        try {
            return DmlPayload.deserialize(data);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Skipping undecodable DML payload at LSN "
                    + lsn + ": " + e.getMessage());
            return null;
        }
    }
}
