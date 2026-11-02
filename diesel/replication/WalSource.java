package diesel.replication;

import diesel.wal.WALEntry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Master-side source that feeds {@link WalSender} (prompt4 #58).
 *
 * <p>Implemented by {@code diesel.ReplicationCoordinator} over the package-private
 * {@code Database}/{@code WALManager} pair — the replication package itself must
 * not depend on engine-internal types (same split as {@code PitrSink} vs
 * {@code DatabasePitrSink}).
 */
public interface WalSource {

    /**
     * Returns the LSN of the last entry appended to the master WAL.
     *
     * @return the last LSN, or 0 when the log is empty
     */
    long getLastLsn();

    /**
     * Reads WAL entries in the given inclusive LSN range.
     *
     * @param fromInclusive first LSN to return
     * @param toInclusive   last LSN to return
     * @return entries in ascending LSN order; empty when none match
     * @throws IOException if reading the WAL fails
     */
    List<WALEntry> readRange(long fromInclusive, long toInclusive) throws IOException;

    /**
     * Returns the data directory whose files form the base backup.
     *
     * @return the master data directory
     */
    Path getDataDir();

    /**
     * Prepares a consistent base backup: waits until no transaction is active,
     * flushes all tables and pending persists, then returns the resume LSN.
     *
     * <p>Contract: after this call every row of every committed transaction is
     * durable in the data directory, and every WAL entry with
     * {@code lsn <= returned value} is either reflected in those files or is
     * safely re-appliable (the standby applies via value-guarded
     * {@link diesel.recovery.PitrSink} operations).
     *
     * @return the backup start LSN; streaming resumes at {@code startLsn + 1}
     * @throws IOException if flushing fails
     */
    long prepareBackup() throws IOException;
}
