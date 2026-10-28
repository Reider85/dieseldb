package diesel.recovery;

import diesel.wal.CommitPayload;

import java.io.IOException;
import java.util.Map;

/**
 * Receives the classified replay work of a point-in-time recovery pass
 * (prompt 4 #43).
 *
 * <p>{@code PointInTimeRecovery} scans the WAL window after the backup,
 * classifies every transaction as <i>visible</i> (committed at or before the
 * restore target) or <i>hidden</i> (committed after the target, aborted, or
 * still active), and drives this sink with the resulting row operations. The
 * sink owns the engine state; the recovery pass owns WAL decoding and index
 * bookkeeping — the same split as {@code ARIESAlgorithm} +
 * {@code DatabaseRecoverySink} in crash recovery.
 *
 * <p><b>Index contract.</b> Every {@code rowIndex} handed to the sink is
 * already translated through the replay's insert-remap table, so the sink
 * treats the indexes as positions in the <i>restored</i> table. Methods
 * return small counts (1 = applied, 0 = guarded/skipped) so the driver can
 * assemble a {@link PitrResult}; hides and applies are value-guarded and
 * never corrupt a mismatched row (the {@code DatabaseRecoverySink} policy).
 *
 * <p><b>Visibility model.</b> DieselDB persists committed tables at commit
 * time, so the restored files already contain everything committed before
 * the backup plus — via write-behind flushes — possibly the physical rows of
 * transactions that were uncommitted at backup time. Visible changes are
 * applied forward; hidden changes are made durable-invisible: hidden inserts
 * are tombstoned (the {@code deletedRows} bitset is serialized, unlike the
 * transient version metadata) and hidden updates restore their before-image.
 * A visible transaction's deletes are tombstoned by {@link #onCommit}, exactly
 * like the engine's own commit path.
 */
public interface PitrSink {

    /**
     * Applies a visible transaction's INSERT to the restored state.
     *
     * @param txid      the owning transaction id (informational)
     * @param table     the affected table name
     * @param rowIndex  the remapped row index in the restored table
     * @param after     the inserted row values (after-image)
     * @return the actual row index the values were stored at (the append
     *         position may differ from {@code rowIndex}; the recovery pass
     *         records it in the remap table)
     * @throws IOException if the apply fails
     */
    int applyInsert(long txid, String table, int rowIndex, Map<String, Object> after)
            throws IOException;

    /**
     * Applies a visible transaction's UPDATE to the restored state
     * (value-guarded: skipped when the row holds neither the before- nor the
     * after-image).
     *
     * @param txid      the owning transaction id (informational)
     * @param table     the affected table name
     * @param rowIndex  the remapped row index in the restored table
     * @param before    the pre-change values (before-image)
     * @param after     the post-change values (after-image)
     * @return 1 when the update was applied, 0 when guarded/skipped
     * @throws IOException if the apply fails
     */
    int applyUpdate(long txid, String table, int rowIndex,
                    Map<String, Object> before, Map<String, Object> after)
            throws IOException;

    /**
     * Makes a hidden transaction's INSERT durably invisible: a row physically
     * present in the restored files (persisted by a flush before the backup)
     * is tombstoned; an absent row is a no-op. Value-guarded: a row whose
     * values do not match the after-image is left untouched.
     *
     * @param txid      the owning transaction id (informational)
     * @param table     the affected table name
     * @param rowIndex  the remapped row index in the restored table
     * @param after     the inserted row values (after-image)
     * @return 1 when a row was tombstoned, 0 otherwise
     * @throws IOException if the hide fails
     */
    int hideInsert(long txid, String table, int rowIndex, Map<String, Object> after)
            throws IOException;

    /**
     * Makes a hidden transaction's UPDATE durably invisible: a row currently
     * holding the after-image is restored to the before-image; anything else
     * is left untouched (value-guard).
     *
     * @param txid      the owning transaction id (informational)
     * @param table     the affected table name
     * @param rowIndex  the remapped row index in the restored table
     * @param before    the pre-change values (before-image)
     * @param after     the post-change values (after-image)
     * @return 1 when the before-image was restored, 0 otherwise
     * @throws IOException if the hide fails
     */
    int hideUpdate(long txid, String table, int rowIndex,
                   Map<String, Object> before, Map<String, Object> after)
            throws IOException;

    /**
     * Finalizes a visible transaction: registers it as COMMITTED in the
     * transaction status tracker, tombstones its deleted rows and stamps its
     * modified rows with the commit CSN — the production commit semantics.
     * Row indexes inside the payload are already remapped.
     *
     * @param payload the (remapped) commit payload of a visible transaction
     * @return the number of rows tombstoned by the commit
     * @throws IOException if the commit application fails
     */
    int onCommit(CommitPayload payload) throws IOException;

    /**
     * Finalizes a hidden transaction: registers it as ABORTED in the
     * transaction status tracker so any lingering references stay invisible.
     *
     * @param txid the hidden transaction id
     * @throws IOException if the registration fails
     */
    void onAbort(long txid) throws IOException;
}
