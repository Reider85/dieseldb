package diesel.recovery;

import diesel.storage.page.PageManager;
import diesel.wal.CommitPayload;
import diesel.wal.WALEntry;
import diesel.wal.WALManager;
import diesel.wal.WALOpcode;
import diesel.wal.WALSegment;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ARIES redo phase: replays every WAL record with
 * {@code checkpoint.lastLSN < lsn <= lastLSN} so the database reflects the
 * committed state before undo runs (prompt 4 #18 / R3-004 step 3/4).
 *
 * <p>Algorithm:
 * <ol>
 *   <li>Determine the window: {@code startLsn = checkpoint.lastLSN + 1},
 *       {@code endLsn = wal.getLastLsn()} captured once (same snapshot rules
 *       as {@link AnalysisPhase}); no checkpoint → the whole log.</li>
 *   <li>Scan segment-by-segment ({@link WALSegment#readAll()}), so only one
 *       segment body is materialized at a time — memory stays bounded by the
 *       segment size even for multi-gigabyte WALs.</li>
 *   <li>For each record in the window:
 *     <ul>
 *       <li>{@code PAGE_IMAGE} → {@link PageManager#applyRedo(WALEntry)}: the
 *           after-image overwrites the page and stamps the page LSN. The
 *           LSN check inside {@code applyRedo} makes the pass idempotent —
 *           re-running redo on an already-redone page is a no-op;</li>
 *       <li>{@code COMMIT} with a payload → decoded as {@link CommitPayload}
 *           and handed to the optional {@link MvccRedoSink}, which restores
 *           the pending xmin/xmax version state of the committed rows (when no
 *           sink is attached the record counts as ignored);</li>
 *       <li>everything else ({@code BEGIN}, DML, {@code ABORT},
 *           {@code CHECKPOINT}, payload-less {@code COMMIT}) → no physical
 *           effect in this phase, counted as ignored.</li>
 *     </ul>
 *   </li>
 *   <li>{@link PageManager#flush()} makes every applied after-image durable
 *       before recovery proceeds to undo.</li>
 * </ol>
 *
 * <p>Consumers: crash-recovery tests directly, and {@code RecoveryManager}
 * (prompt 4 #19), which orchestrates analysis → redo → undo at startup and
 * attaches the MVCC sink.
 *
 * @see RedoResult
 * @see MvccRedoSink
 */
public final class RedoPhase {

    private static final Logger LOGGER = Logger.getLogger(RedoPhase.class.getName());

    private RedoPhase() {
        // utility class
    }

    /**
     * Runs the redo phase using the checkpoint referenced by
     * {@code checkpoint.ptr} (or the whole log when no checkpoint exists).
     * Physical redo only — no MVCC sink is attached.
     *
     * @param wal   the open WAL manager
     * @param pages the page manager holding the pages to replay onto
     * @return the redo result
     * @throws IOException if reading the checkpoint, the WAL, or writing pages fails
     */
    public static RedoResult redo(WALManager wal, PageManager pages) throws IOException {
        Objects.requireNonNull(wal, "wal");
        return redo(wal, pages, wal.loadCheckpointRecord(), null);
    }

    /**
     * Runs the redo phase from the given checkpoint to the end of the log.
     * Physical redo only — no MVCC sink is attached.
     *
     * @param wal        the open WAL manager
     * @param pages      the page manager holding the pages to replay onto
     * @param checkpoint the checkpoint to start after, or {@code null} to
     *                   replay the whole log
     * @return the redo result
     * @throws IOException if reading the WAL or writing pages fails
     */
    public static RedoResult redo(WALManager wal, PageManager pages, CheckpointRecord checkpoint)
            throws IOException {
        return redo(wal, pages, checkpoint, null);
    }

    /**
     * Runs the redo phase with an MVCC sink attached: COMMIT records inside
     * the window are decoded and delivered to {@code mvccSink} so the
     * committed xmin/xmax version state can be restored.
     *
     * @param wal        the open WAL manager
     * @param pages      the page manager holding the pages to replay onto
     * @param checkpoint the checkpoint to start after, or {@code null} to
     *                   replay the whole log
     * @param mvccSink   receiver of replayed COMMIT payloads, or {@code null}
     *                   for physical redo only
     * @return the redo result
     * @throws IOException if reading the WAL, writing pages, or the sink fails
     */
    public static RedoResult redo(WALManager wal, PageManager pages, CheckpointRecord checkpoint,
                                  MvccRedoSink mvccSink) throws IOException {
        Objects.requireNonNull(wal, "wal");
        Objects.requireNonNull(pages, "pages");

        // Snapshot the end of the log once, exactly like the analysis phase.
        long endLsn = wal.getLastLsn();
        long startLsn = checkpoint != null ? checkpoint.getLastLSN() + 1 : 1L;

        long applied = 0;
        long skipped = 0;
        long commitsReplayed = 0;
        long ignored = 0;

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

                WALOpcode op = entry.getOp();
                if (op == WALOpcode.PAGE_IMAGE) {
                    if (!entry.hasAfterImage()) {
                        // Structurally valid record with a missing image: treat
                        // as corrupt, never install a partial page.
                        LOGGER.log(Level.WARNING, "Skipping PAGE_IMAGE record without after-image at LSN " + lsn);
                        ignored++;
                        continue;
                    }
                    if (pages.applyRedo(entry)) {
                        applied++;
                    } else {
                        skipped++;
                    }
                } else if (op == WALOpcode.COMMIT && mvccSink != null && entry.hasAfterImage()) {
                    CommitPayload payload;
                    try {
                        payload = CommitPayload.deserialize(entry.getAfterImage());
                    } catch (RuntimeException e) {
                        // Legacy or corrupt payload: physical redo must not fail.
                        LOGGER.log(Level.WARNING, "Skipping undecodable COMMIT payload at LSN "
                                + lsn + ": " + e.getMessage());
                        ignored++;
                        continue;
                    }
                    mvccSink.onCommit(payload);
                    commitsReplayed++;
                } else {
                    ignored++;
                }
            }
        }

        // Redone pages must survive a crash that happens before the next flush.
        pages.flush();

        return new RedoResult(applied, skipped, commitsReplayed, ignored, endLsn);
    }
}
