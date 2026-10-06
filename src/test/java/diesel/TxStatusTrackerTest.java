package diesel;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link TxStatusTracker} (prompt4.md step 3, vacuum planning):
 * oldest-active-txid bookkeeping and the CSN-based commit ordering that the
 * vacuum horizon relies on.
 */
@Tag("smoke")
@Tag("concurrency")
class TxStatusTrackerTest {

    @Test
    void oldestActiveTxidIsMaxValueWithoutActiveTransactions() {
        TxStatusTracker tracker = new TxStatusTracker();
        assertEquals(Long.MAX_VALUE, tracker.getOldestActiveTxid());
    }

    @Test
    void oldestActiveTxidReturnsMinimumOfActiveTransactions() {
        TxStatusTracker tracker = new TxStatusTracker();
        long first = tracker.registerTransaction();
        long second = tracker.registerTransaction();
        long third = tracker.registerTransaction();

        assertEquals(first, tracker.getOldestActiveTxid());

        tracker.markCommitted(second);
        assertEquals(first, tracker.getOldestActiveTxid());

        tracker.markAborted(first);
        assertEquals(third, tracker.getOldestActiveTxid());

        tracker.markCommitted(third);
        assertEquals(Long.MAX_VALUE, tracker.getOldestActiveTxid());
    }

    @Test
    void commitSequenceNumbersDoNotFollowTransactionIdOrder() {
        TxStatusTracker tracker = new TxStatusTracker();
        long earlyTx = tracker.registerTransaction();
        long lateTx = tracker.registerTransaction();

        long lateCsn = tracker.markCommitted(lateTx);
        long earlyCsn = tracker.markCommitted(earlyTx);

        assertTrue(lateCsn < earlyCsn,
                "a transaction that commits first must get the lower CSN even with a higher txid");
        assertTrue(tracker.isCommittedBefore(lateTx, earlyCsn),
                "row versions of the earlier-committing tx must be visible to the later snapshot");
        assertFalse(tracker.isCommittedBefore(earlyTx, lateCsn),
                "row versions committed after a snapshot must stay invisible to it");
    }

    @Test
    void isCommittedBeforeIsFalseForActiveAbortedAndUnknownTransactions() {
        TxStatusTracker tracker = new TxStatusTracker();
        long active = tracker.registerTransaction();
        long aborted = tracker.registerTransaction();
        tracker.markAborted(aborted);

        assertFalse(tracker.isCommittedBefore(active, Long.MAX_VALUE));
        assertFalse(tracker.isCommittedBefore(aborted, Long.MAX_VALUE));
        assertFalse(tracker.isCommittedBefore(999L, Long.MAX_VALUE), "unknown txid is not committed");
    }

    @Test
    void statusLookupReportsActiveCommittedAbortedAndUnknown() {
        TxStatusTracker tracker = new TxStatusTracker();
        long active = tracker.registerTransaction();
        long committed = tracker.registerTransaction();
        long aborted = tracker.registerTransaction();
        tracker.markCommitted(committed);
        tracker.markAborted(aborted);

        assertEquals(TxStatusTracker.TxStatus.ACTIVE, tracker.getStatus(active));
        assertEquals(TxStatusTracker.TxStatus.COMMITTED, tracker.getStatus(committed));
        assertEquals(TxStatusTracker.TxStatus.ABORTED, tracker.getStatus(aborted));
        assertNull(tracker.getStatus(4242L));
        assertTrue(tracker.isActive(active));
        assertFalse(tracker.isActive(committed));
    }

    @Test
    void currentCommitCsnTracksLastAssignedValue() {
        TxStatusTracker tracker = new TxStatusTracker();
        assertEquals(0, tracker.getCurrentCommitCsn());
        long tx = tracker.registerTransaction();
        tracker.markCommitted(tx);
        assertEquals(1, tracker.getCurrentCommitCsn());
        long other = tracker.registerTransaction();
        tracker.markCommitted(other);
        assertEquals(2, tracker.getCurrentCommitCsn());
    }
}
