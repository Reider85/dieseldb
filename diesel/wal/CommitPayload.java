package diesel.wal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Payload of a WAL {@link WALOpcode#COMMIT} record (prompt 4 #18).
 *
 * <p>The wire format is the one historically written by
 * {@code Database.serializeCommitPayload}, which now delegates here so the
 * writer (commit path) and the ARIES redo phase share a single implementation:
 *
 * <pre>
 *   txid            (long,  8 bytes)
 *   commitCsn       (long,  8 bytes)
 *   modifiedTables  (int count, then per table: writeUTF name, int rowCount, int* rowIndex)
 *   deletedTables   (int count, then per table: writeUTF name, int rowCount, int* rowIndex)
 *   commitTimestamp (long,  8 bytes)  — prompt 4 #43, PITR; absent in legacy payloads
 * </pre>
 *
 * <p>The payload carries the MVCC bookkeeping of a commit: the rows whose
 * pending {@code xmin/xmax} version state must be stamped as committed when
 * the record is replayed by {@code diesel.recovery.RedoPhase} (task 4 of
 * prompt 4 #18).
 *
 * <p><b>PITR timestamp (prompt 4 #43).</b> The trailing {@code commitTimestamp}
 * epoch-millis field powers {@code diesel.recovery.PointInTimeRecovery}: a
 * transaction is visible in a point-in-time restore exactly when its commit
 * timestamp is at or before the restore target. Forward/backward wire
 * compatibility: the field is APPENDED after the trailing table map, so an
 * older reader simply leaves it unread (byte-array streams do not complain
 * about trailing bytes), and a newer reader decoding a legacy payload hits
 * EOF and records {@code 0} ("timestamp unknown" — PITR treats such commits
 * as visible at any target and logs a warning).
 */
public final class CommitPayload {

    private final long txid;
    private final long commitCsn;
    private final Map<String, int[]> modifiedRows;
    private final Map<String, int[]> deletedRows;
    private final long commitTimestampMs;

    /**
     * Creates a payload from row-index collections without a commit timestamp
     * (legacy semantics: timestamp unknown = 0).
     *
     * @param txid        the committing transaction id
     * @param commitCsn   the commit sequence number stamped on the rows
     * @param modifiedRows table name → inserted/updated row indexes
     * @param deletedRows  table name → deleted row indexes
     */
    public CommitPayload(long txid, long commitCsn,
                         Map<String, ? extends Collection<Integer>> modifiedRows,
                         Map<String, ? extends Collection<Integer>> deletedRows) {
        this(txid, commitCsn, modifiedRows, deletedRows, 0L);
    }

    /**
     * Creates a payload from row-index collections with a commit timestamp.
     *
     * @param txid        the committing transaction id
     * @param commitCsn   the commit sequence number stamped on the rows
     * @param modifiedRows table name → inserted/updated row indexes
     * @param deletedRows  table name → deleted row indexes
     * @param commitTimestampMs epoch-millis commit time for PITR, or 0 when unknown
     */
    public CommitPayload(long txid, long commitCsn,
                         Map<String, ? extends Collection<Integer>> modifiedRows,
                         Map<String, ? extends Collection<Integer>> deletedRows,
                         long commitTimestampMs) {
        this.txid = txid;
        this.commitCsn = commitCsn;
        this.modifiedRows = copyRows(modifiedRows);
        this.deletedRows = copyRows(deletedRows);
        this.commitTimestampMs = commitTimestampMs;
    }

    /**
     * Serializes a commit payload in the legacy Database commit format
     * (no commit timestamp — written only by pre-PITR callers/tests).
     *
     * @param txid        the committing transaction id
     * @param commitCsn   the commit sequence number
     * @param modifiedRows table name → inserted/updated row indexes
     * @param deletedRows  table name → deleted row indexes
     * @return the encoded payload bytes
     * @throws IOException if the payload cannot be written
     */
    public static byte[] serialize(long txid, long commitCsn,
                                   Map<String, ? extends Collection<Integer>> modifiedRows,
                                   Map<String, ? extends Collection<Integer>> deletedRows)
            throws IOException {
        return serialize(txid, commitCsn, modifiedRows, deletedRows, 0L);
    }

    /**
     * Serializes a commit payload including the PITR commit timestamp
     * (prompt 4 #43). The timestamp is appended after the trailing table map
     * so legacy readers keep working unchanged.
     *
     * @param txid        the committing transaction id
     * @param commitCsn   the commit sequence number
     * @param modifiedRows table name → inserted/updated row indexes
     * @param deletedRows  table name → deleted row indexes
     * @param commitTimestampMs epoch-millis commit time, or 0 when unknown
     * @return the encoded payload bytes
     * @throws IOException if the payload cannot be written
     */
    public static byte[] serialize(long txid, long commitCsn,
                                   Map<String, ? extends Collection<Integer>> modifiedRows,
                                   Map<String, ? extends Collection<Integer>> deletedRows,
                                   long commitTimestampMs)
            throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeLong(txid);
        dos.writeLong(commitCsn);

        writeTables(dos, modifiedRows);
        writeTables(dos, deletedRows);
        dos.writeLong(commitTimestampMs);

        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Deserializes a commit payload written by {@link #serialize}.
     *
     * @param data the encoded payload bytes
     * @return the decoded payload
     * @throws IllegalArgumentException if the payload is truncated or malformed
     *         (reported as an unchecked exception so recovery can skip a
     *         legacy/corrupt payload without aborting the physical redo pass)
     */
    public static CommitPayload deserialize(byte[] data) {
        Objects.requireNonNull(data, "data");
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            long txid = dis.readLong();
            long commitCsn = dis.readLong();
            Map<String, int[]> modified = readTables(dis);
            Map<String, int[]> deleted = readTables(dis);
            // PITR trailing field (prompt 4 #43): absent in legacy payloads.
            // EOF here means "pre-PITR payload", NOT truncation — the four
            // structural fields above are already complete.
            long commitTimestampMs = 0L;
            try {
                commitTimestampMs = dis.readLong();
            } catch (EOFException legacyNoTimestamp) {
                commitTimestampMs = 0L;
            }
            return new CommitPayload(txid, commitCsn, toCollections(modified),
                    toCollections(deleted), commitTimestampMs);
        } catch (EOFException e) {
            throw new IllegalArgumentException("Truncated commit payload (" + data.length + " bytes)", e);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed commit payload: " + e.getMessage(), e);
        }
    }

    private static void writeTables(DataOutputStream dos,
                                    Map<String, ? extends Collection<Integer>> tables) throws IOException {
        dos.writeInt(tables.size());
        for (Map.Entry<String, ? extends Collection<Integer>> entry : tables.entrySet()) {
            dos.writeUTF(entry.getKey());
            Collection<Integer> rows = entry.getValue();
            dos.writeInt(rows.size());
            for (Integer rowIndex : rows) {
                dos.writeInt(rowIndex);
            }
        }
    }

    private static Map<String, int[]> readTables(DataInputStream dis) throws IOException {
        int tableCount = dis.readInt();
        if (tableCount < 0) {
            throw new IllegalArgumentException("Negative table count: " + tableCount);
        }
        Map<String, int[]> tables = new LinkedHashMap<>();
        for (int i = 0; i < tableCount; i++) {
            String tableName = dis.readUTF();
            int rowCount = dis.readInt();
            if (rowCount < 0) {
                throw new IllegalArgumentException("Negative row count: " + rowCount);
            }
            int[] rows = new int[rowCount];
            for (int r = 0; r < rowCount; r++) {
                rows[r] = dis.readInt();
            }
            tables.put(tableName, rows);
        }
        return tables;
    }

    private static Map<String, int[]> copyRows(Map<String, ? extends Collection<Integer>> source) {
        Objects.requireNonNull(source, "rows");
        Map<String, int[]> copy = new LinkedHashMap<>();
        for (Map.Entry<String, ? extends Collection<Integer>> entry : source.entrySet()) {
            int[] rows = new int[entry.getValue().size()];
            int i = 0;
            for (Integer rowIndex : entry.getValue()) {
                rows[i++] = rowIndex;
            }
            copy.put(entry.getKey(), rows);
        }
        return copy;
    }

    private static Map<String, Collection<Integer>> toCollections(Map<String, int[]> rows) {
        Map<String, Collection<Integer>> converted = new LinkedHashMap<>();
        for (Map.Entry<String, int[]> entry : rows.entrySet()) {
            Collection<Integer> values = new ArrayList<>(entry.getValue().length);
            for (int rowIndex : entry.getValue()) {
                values.add(rowIndex);
            }
            converted.put(entry.getKey(), values);
        }
        return converted;
    }

    /**
     * Returns the committing transaction id.
     */
    public long getTxid() {
        return txid;
    }

    /**
     * Returns the commit sequence number stamped on the committed rows.
     */
    public long getCommitCsn() {
        return commitCsn;
    }

    /**
     * Returns table name → inserted/updated row indexes.
     */
    public Map<String, int[]> getModifiedRows() {
        return modifiedRows;
    }

    /**
     * Returns table name → deleted row indexes.
     */
    public Map<String, int[]> getDeletedRows() {
        return deletedRows;
    }

    /**
     * Returns the epoch-millis commit time recorded for PITR (prompt 4 #43),
     * or {@code 0} for legacy payloads written before this field existed
     * ("timestamp unknown" — visible at every restore target).
     */
    public long getCommitTimestampMs() {
        return commitTimestampMs;
    }

    @Override
    public String toString() {
        return "CommitPayload{txid=" + txid + ", commitCsn=" + commitCsn
                + ", modifiedTables=" + modifiedRows.size()
                + ", deletedTables=" + deletedRows.size()
                + ", commitTimestampMs=" + commitTimestampMs + '}';
    }
}
