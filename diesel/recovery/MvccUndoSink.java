package diesel.recovery;

import java.io.IOException;
import java.util.Map;

/**
 * Callback that receives logical row-level undo operations during the ARIES
 * undo phase (prompt 4 #19).
 *
 * <p>One callback fires per decoded {@code INSERT}/{@code UPDATE}/
 * {@code DELETE} WAL record whose transaction is still active (no COMMIT in
 * the log). Delivery is in reverse LSN order so chained changes to the same
 * row unwind correctly (the newest change is reversed first).
 *
 * <p>Production wiring: {@code DatabaseRecoverySink} stamps the affected row
 * versions as aborted (xmin = aborted txid) so MVCC visibility hides them
 * from every reader; physical reclamation happens when the table vacuums.
 *
 * @see UndoPhase
 */
public interface MvccUndoSink {

    /**
     * Reverses an uncommitted INSERT: the row at {@code rowIndex} was inserted
     * by {@code txid} and must become invisible.
     *
     * @param txid     the still-active transaction id
     * @param table    the affected table name
     * @param rowIndex the raw row index
     * @param inserted the values that were inserted (after-image)
     * @throws IOException if the undo cannot be applied
     */
    void onInsertUndo(long txid, String table, int rowIndex, Map<String, Object> inserted)
            throws IOException;

    /**
     * Reverses an uncommitted UPDATE: the row at {@code rowIndex} currently
     * holds {@code after} and must be restored to {@code before}.
     *
     * @param txid     the still-active transaction id
     * @param table    the affected table name
     * @param rowIndex the raw row index
     * @param before   the pre-change values (before-image)
     * @param after    the post-change values (after-image)
     * @throws IOException if the undo cannot be applied
     */
    void onUpdateUndo(long txid, String table, int rowIndex,
                      Map<String, Object> before, Map<String, Object> after)
            throws IOException;

    /**
     * Reverses an uncommitted DELETE: the row at {@code rowIndex} must remain
     * alive with its {@code before} values.
     *
     * @param txid     the still-active transaction id
     * @param table    the affected table name
     * @param rowIndex the raw row index
     * @param before   the pre-delete values (before-image)
     * @throws IOException if the undo cannot be applied
     */
    void onDeleteUndo(long txid, String table, int rowIndex, Map<String, Object> before)
            throws IOException;
}
