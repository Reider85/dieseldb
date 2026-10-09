package diesel.recovery;

import diesel.storage.page.PageManager;
import diesel.wal.WALManager;

import java.io.IOException;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * ARIES recovery orchestration: analysis → redo → undo (prompt 4 #19 /
 * R3-004 step 4/4).
 *
 * <p>Runs the three recovery phases in their canonical order against a
 * consistent end-of-log snapshot:
 * <ol>
 *   <li>{@link AnalysisPhase#analyze} rebuilds the committed and active
 *       transaction sets;</li>
 *   <li>{@link RedoPhase#redo} replays committed page after-images and
 *       delivers COMMIT payloads to the {@link MvccRedoSink};</li>
 *   <li>{@link UndoPhase#undo} reverses the logical DML records of every
 *       active transaction (newest first) via the {@link MvccUndoSink}.</li>
 * </ol>
 *
 * <p>Both sinks are optional ({@code null} = physical recovery only). The
 * production wiring in {@code RecoveryManager} attaches a
 * {@code DatabaseRecoverySink} implementing both interfaces.
 *
 * <p>Consumed by {@code RecoveryManager.recover()}, which is invoked from
 * {@code DatabaseServer.start()} before the server accepts client connections.
 */
public final class ARIESAlgorithm {

    private static final Logger LOGGER = Logger.getLogger(ARIESAlgorithm.class.getName());

    private ARIESAlgorithm() {
        // utility class
    }

    /**
     * Runs full ARIES recovery using the checkpoint referenced by
     * {@code checkpoint.ptr} (or the whole log when no checkpoint exists).
     *
     * @param wal      the open WAL manager
     * @param pages    the page manager to replay page images onto
     * @param redoSink receiver of replayed COMMIT payloads, or {@code null}
     * @param undoSink receiver of logical undo operations, or {@code null}
     * @return the combined recovery result
     * @throws IOException if reading the WAL/checkpoint, writing pages, or a
     *                     sink fails
     */
    public static RecoveryResult recover(WALManager wal, PageManager pages,
                                         MvccRedoSink redoSink, MvccUndoSink undoSink)
            throws IOException {
        Objects.requireNonNull(wal, "wal");
        return recover(wal, pages, wal.loadCheckpointRecord(), redoSink, undoSink);
    }

    /**
     * Runs full ARIES recovery from the given checkpoint to the end of the log.
     *
     * @param wal        the open WAL manager
     * @param pages      the page manager to replay page images onto
     * @param checkpoint the checkpoint to start after, or {@code null} to
     *                   recover the whole log
     * @param redoSink   receiver of replayed COMMIT payloads, or {@code null}
     * @param undoSink   receiver of logical undo operations, or {@code null}
     * @return the combined recovery result
     * @throws IOException if reading the WAL, writing pages, or a sink fails
     */
    public static RecoveryResult recover(WALManager wal, PageManager pages,
                                         CheckpointRecord checkpoint,
                                         MvccRedoSink redoSink, MvccUndoSink undoSink)
            throws IOException {
        Objects.requireNonNull(wal, "wal");
        Objects.requireNonNull(pages, "pages");

        LOGGER.fine(() -> "ARIES recovery starting (wal=" + wal.getSegments().size()
                + " segment(s), checkpoint=" + (checkpoint != null ? checkpoint.getLastLSN() : "none") + ")");

        AnalysisResult analysis = AnalysisPhase.analyze(wal, checkpoint);
        RedoResult redo = RedoPhase.redo(wal, pages, checkpoint, redoSink);
        UndoResult undo = undoSink != null
                ? UndoPhase.undo(wal, analysis.getActive(), undoSink)
                : new UndoResult(0, 0, 0, 0, analysis.getLastLSN());

        LOGGER.fine(() -> "ARIES recovery done: " + new RecoveryResult(analysis, redo, undo));
        return new RecoveryResult(analysis, redo, undo);
    }
}
