package diesel.replication;

import diesel.wal.WALEntry;

import java.io.IOException;

/**
 * Standby-side consumer that applies streamed WAL entries (prompt4 #58).
 *
 * <p>Implemented by {@code diesel.ReplicationCoordinator} as a thin adapter over
 * {@link StreamingApplier}, which in turn drives the engine's
 * {@link diesel.recovery.PitrSink}. Keeping the interface in
 * {@code diesel.replication} lets the receiver stay free of package-private
 * engine types.
 */
public interface WalApplier {

    /**
     * Applies one received WAL entry (buffers it until its transaction
     * commits or aborts; see {@link StreamingApplier}).
     *
     * @param entry the entry, in ascending LSN order per connection
     * @throws IOException if the apply fails
     */
    void apply(WALEntry entry) throws IOException;

    /**
     * Returns the highest LSN whose effects are fully applied (all entries up
     * to it belong to transactions that already committed or aborted on the
     * standby). Used for the resume marker and reconnect.
     *
     * @return the applied LSN, or 0 when nothing was applied yet
     */
    long getAppliedLsn();
}
