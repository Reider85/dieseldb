package diesel.recovery;

import diesel.wal.CommitPayload;

import java.io.IOException;

/**
 * Receives COMMIT records replayed by the ARIES redo phase (prompt 4 #18,
 * task 4: MVCC integration).
 *
 * <p>Each payload carries the committing txid, the commit CSN and the exact
 * modified/deleted row indexes stamped at commit time. The consumer restores
 * the pending {@code xmin/xmax} version state of those rows (production
 * wiring: {@code RecoveryManager} marks the rows committed through
 * {@code Table.markRowCommitted} and feeds the transaction status tracker —
 * prompt 4 #19).
 *
 * <p>Implementations must be fast: the callback runs inline for every COMMIT
 * record inside the redo window.
 */
@FunctionalInterface
public interface MvccRedoSink {

    /**
     * Replays one COMMIT record.
     *
     * @param payload the decoded commit payload (txid, commit CSN, row indexes)
     * @throws IOException if the consumer cannot apply the commit; aborts the redo pass
     */
    void onCommit(CommitPayload payload) throws IOException;
}
