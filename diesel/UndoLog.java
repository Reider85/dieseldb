package diesel;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Undo log for MVCC transaction rollback support.
 * 
 * <p>Records operations that can be replayed in reverse order during ROLLBACK.
 * Supports spilling to disk when memory usage exceeds threshold.
 * 
 * <p><b>Record Types:</b>
 * <ul>
 *   <li>InsertUndo — rollback removes the inserted row</li>
 *   <li>UpdateUndo — rollback restores old values</li>
 *   <li>DeleteUndo — rollback restores the deleted row</li>
 * </ul>
 */
public class UndoLog {
    
    /**
     * Base interface for undo records.
     */
    public interface UndoRecord {
        /**
         * Applies this undo operation to the given table.
         */
        void apply(Table table);
        
        /**
         * The table this record belongs to, or null when the record was
         * created without table context (unit tests).
         */
        String getTableName();
        
        /**
         * Serializes this record to bytes.
         */
        byte[] serialize() throws IOException;
        
        /**
         * Deserializes a record from bytes written by {@link #serialize()}.
         * The stream layout mirrors serialize() field-for-field: type code
         * first, then the fields of that record type.
         */
        static UndoRecord deserialize(byte[] data) throws IOException, ClassNotFoundException {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                int type = ois.readInt();
                switch (type) {
                    case 1: {
                        String tableName = (String) ois.readObject();
                        int rowIndex = ois.readInt();
                        return new InsertUndo(tableName, rowIndex);
                    }
                    case 2: {
                        String tableName = (String) ois.readObject();
                        int rowIndex = ois.readInt();
                        @SuppressWarnings("unchecked")
                        Map<String, Object> oldValues = (Map<String, Object>) ois.readObject();
                        RowVersionMeta oldMeta = (RowVersionMeta) ois.readObject();
                        return new UpdateUndo(tableName, rowIndex, oldValues, oldMeta);
                    }
                    case 3: {
                        String tableName = (String) ois.readObject();
                        int rowIndex = ois.readInt();
                        RowVersionMeta oldMeta = (RowVersionMeta) ois.readObject();
                        return new DeleteUndo(tableName, rowIndex, oldMeta);
                    }
                    case 4: {
                        String name = (String) ois.readObject();
                        return new SavepointMarkerUndo(name);
                    }
                    default:
                        throw new IOException("Unknown undo record type code: " + type);
                }
            }
        }
    }
    
    /**
     * Undo record for INSERT operations.
     * Rollback: removes the inserted row or marks it as not inserted.
     */
    public static class InsertUndo implements UndoRecord {
        private final String tableName;
        private final int rowIndex;
        
        public InsertUndo(int rowIndex) {
            this(null, rowIndex);
        }
        
        public InsertUndo(String tableName, int rowIndex) {
            this.tableName = tableName;
            this.rowIndex = rowIndex;
        }
        
        @Override
        public void apply(Table table) {
            // Clear the pending flags: the TxStatusTracker entry (marked ABORTED
            // before rollback runs) hides the row from every reader; physical
            // reclamation happens when the table compacts its tombstones.
            table.markRowRolledBack(rowIndex);
        }
        
        @Override
        public String getTableName() {
            return tableName;
        }
        
        @Override
        public byte[] serialize() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeInt(1); // Type code for InsertUndo
                oos.writeObject(tableName);
                oos.writeInt(rowIndex);
            }
            return baos.toByteArray();
        }
        
        public int getRowIndex() {
            return rowIndex;
        }
    }
    
    /**
     * Undo record for UPDATE operations.
     * Rollback: restores the old values and metadata.
     */
    public static class UpdateUndo implements UndoRecord {
        private final String tableName;
        private final int rowIndex;
        private final Map<String, Object> oldValues;
        private final RowVersionMeta oldMeta;
        
        public UpdateUndo(int rowIndex, Map<String, Object> oldValues, RowVersionMeta oldMeta) {
            this(null, rowIndex, oldValues, oldMeta);
        }
        
        public UpdateUndo(String tableName, int rowIndex, Map<String, Object> oldValues, RowVersionMeta oldMeta) {
            this.tableName = tableName;
            this.rowIndex = rowIndex;
            this.oldValues = oldValues;
            this.oldMeta = oldMeta;
        }
        
        @Override
        public void apply(Table table) {
            // Restore physical values (with secondary-index diff) and the
            // pre-change version metadata captured before markUpdate mutated it.
            table.restoreRowAfterUpdateUndo(rowIndex, oldValues, oldMeta);
        }
        
        @Override
        public String getTableName() {
            return tableName;
        }
        
        @Override
        public byte[] serialize() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeInt(2); // Type code for UpdateUndo
                oos.writeObject(tableName);
                oos.writeInt(rowIndex);
                oos.writeObject(oldValues);
                oos.writeObject(oldMeta);
            }
            return baos.toByteArray();
        }
        
        public int getRowIndex() {
            return rowIndex;
        }
        
        @SuppressWarnings("unchecked")
        public Map<String, Object> getOldValues() {
            return oldValues;
        }
        
        public RowVersionMeta getOldMeta() {
            return oldMeta;
        }
    }
    
    /**
     * Undo record for DELETE operations.
     * Rollback: restores the deleted row or clears the delete mark.
     */
    public static class DeleteUndo implements UndoRecord {
        private final String tableName;
        private final int rowIndex;
        private final RowVersionMeta oldMeta;
        
        public DeleteUndo(int rowIndex, RowVersionMeta oldMeta) {
            this(null, rowIndex, oldMeta);
        }
        
        public DeleteUndo(String tableName, int rowIndex, RowVersionMeta oldMeta) {
            this.tableName = tableName;
            this.rowIndex = rowIndex;
            this.oldMeta = oldMeta;
        }
        
        @Override
        public void apply(Table table) {
            // Restore the pre-delete metadata (clears xmax / pending flags);
            // a null oldMeta means the row had no version metadata yet.
            table.restoreMetaAfterUndo(rowIndex, oldMeta);
        }
        
        @Override
        public String getTableName() {
            return tableName;
        }
        
        @Override
        public byte[] serialize() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeInt(3); // Type code for DeleteUndo
                oos.writeObject(tableName);
                oos.writeInt(rowIndex);
                oos.writeObject(oldMeta);
            }
            return baos.toByteArray();
        }
        
        public int getRowIndex() {
            return rowIndex;
        }
        
        public RowVersionMeta getOldMeta() {
            return oldMeta;
        }
    }
    
    /**
     * Undo record for SAVEPOINT markers (prompt4.md #26).
     * Rollback: no-op marker that identifies savepoint boundaries in the undo log.
     * Applied during ROLLBACK TO to stop partial undo at the savepoint.
     */
    public static class SavepointMarkerUndo implements UndoRecord {
        private final String name;
        
        public SavepointMarkerUndo(String name) {
            this.name = name;
        }
        
        @Override
        public void apply(Table table) {
            // No-op: savepoint markers are not applied during rollback.
            // They serve as boundary markers for partial undo.
        }
        
        @Override
        public String getTableName() {
            return null; // No table context for savepoint markers
        }
        
        @Override
        public byte[] serialize() throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeInt(4); // Type code for SavepointMarkerUndo
                oos.writeObject(name);
            }
            return baos.toByteArray();
        }
        
        public String getName() {
            return name;
        }
    }
    
    // ─── UndoLog implementation ───────────────────────────────────────────────
    
    private Deque<UndoRecord> inMemoryRecords = new ArrayDeque<>();
    private AtomicLong inMemoryBytes = new AtomicLong(0);
    private final long spillThresholdBytes;
    private Path spillFile;
    private DataOutputStream spillOut;
    
/**
     * Creates a new undo log with the given spill threshold in bytes.
     */
    public UndoLog(long spillThresholdBytes) {
        this.spillThresholdBytes = spillThresholdBytes;
        this.inMemoryRecords = new ArrayDeque<>();
        this.inMemoryBytes = new AtomicLong(0);
    }

    /**
     * Adds an undo record to the log.
     */
    public void addUndoRecord(UndoRecord record) {
        inMemoryRecords.addLast(record);
        inMemoryBytes.addAndGet(estimateRecordSize(record));
        
        // Check if we need to spill to disk
        if (inMemoryBytes.get() > spillThresholdBytes) {
            try {
                spillRecordsToDisk();
            } catch (IOException e) {
                System.err.println("Failed to spill undo log to disk: " + e.getMessage());
            }
        }
    }

    /**
     * Spills in-memory records to disk.
     */
    private void spillRecordsToDisk() throws IOException {
        if (spillFile == null) {
            spillFile = Files.createTempFile("diesel-undo-", ".log");
        }
        
        try (DataOutputStream dos = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(spillFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND)))) {
            
            while (!inMemoryRecords.isEmpty() && inMemoryBytes.get() > 0) {
                UndoRecord record = inMemoryRecords.removeFirst();
                byte[] data = record.serialize();
                // Trailer framing: data followed by its 4-byte length, so
                // applySpilledRecordsReverse can walk the file backwards.
                dos.write(data);
                dos.writeInt(data.length);
                inMemoryBytes.addAndGet(-estimateRecordSize(record));
            }
        }
    }
    
    /**
     * Adds an undo record to the log.
     */
    public void log(UndoRecord record) throws IOException {
        checkAndSpill();
        
        byte[] serialized = record.serialize();
        inMemoryRecords.addLast(record);
        inMemoryBytes.addAndGet(serialized.length);
    }
    
    /**
     * Appends a savepoint marker to the log.
     * The marker is a no-op record that identifies a savepoint boundary.
     *
     * @param name the savepoint name
     */
    public void appendSavepointMarker(String name) {
        SavepointMarkerUndo marker = new SavepointMarkerUndo(name);
        addUndoRecord(marker);
    }
    
    /**
     * Checks if spill is needed and performs it if necessary.
     */
    private void checkAndSpill() throws IOException {
        if (inMemoryBytes.get() > spillThresholdBytes) {
            spillAllRecords();
        }
    }
    
    /**
     * Spills all in-memory records to disk using trailer framing (data
     * followed by its 4-byte length), matching
     * {@link #applySpilledRecordsReverse(Database)}.
     */
    private void spillAllRecords() throws IOException {
        if (spillFile == null) {
            spillFile = Files.createTempFile("diesel-undo-", ".log");
        }
        
        if (spillOut == null) {
            spillOut = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(spillFile, StandardOpenOption.APPEND)));
        }
        
        // Write all in-memory records to disk
        for (UndoRecord record : inMemoryRecords) {
            byte[] data = record.serialize();
            spillOut.write(data);
            spillOut.writeInt(data.length);
        }
        spillOut.flush();
        
        // Clear in-memory records
        inMemoryRecords.clear();
        inMemoryBytes.set(0);
    }
    
    /**
     * Applies all undo records in reverse order (ROLLBACK).
     *
     * <p>Newest records are applied first: the in-memory tail (most recent
     * operations) is replayed before the spilled head, which itself is read
     * backwards from disk. Records without table context are skipped.
     *
     * @param database the database used to resolve each record's table
     */
    public void rollback(Database database) throws IOException {
        // Apply in-memory records in reverse order (newest first)
        while (!inMemoryRecords.isEmpty()) {
            UndoRecord record = inMemoryRecords.removeLast();
            inMemoryBytes.addAndGet(-estimateRecordSize(record));
            applyRecord(record, database);
        }
        
        // Then apply spilled records in reverse order (older ones, newest first)
        if (spillFile != null) {
            applySpilledRecordsReverse(database);
        }
    }
    
    /**
     * Rolls back to the specified savepoint marker: applies undo records
     * newer than the marker and stops at the marker (which remains in the log).
     *
     * <p>The marker is not consumed - it remains valid for subsequent
     * rollbacks to the same savepoint (standard SQL behavior).
     *
     * @param database the database used to resolve each record's table
     * @param name the savepoint name to rollback to
     * @return the number of records applied
     * @throws IOException if the spill file cannot be read/truncated
     * @throws IllegalArgumentException if no marker with the given name is found
     */
    public int rollbackToMarker(Database database, String name) throws IOException {
        Objects.requireNonNull(name, "name");
        int appliedCount = 0;
        
        // First check in-memory records (newest first)
        while (!inMemoryRecords.isEmpty()) {
            UndoRecord record = inMemoryRecords.getLast();
            if (record instanceof SavepointMarkerUndo marker && marker.getName().equals(name)) {
                // Found marker in memory - stop, leave marker in place
                break;
            }
            
            // Apply and remove the record
            record = inMemoryRecords.removeLast();
            inMemoryBytes.addAndGet(-estimateRecordSize(record));
            applyRecord(record, database);
            appliedCount++;
        }
        
        // If marker not found in memory, check spill file
        if (inMemoryRecords.isEmpty() || !(inMemoryRecords.getLast() instanceof SavepointMarkerUndo marker) || !marker.getName().equals(name)) {
            appliedCount += rollbackToMarkerInSpill(database, name);
        }
        
        if (appliedCount == 0) {
            throw new IllegalArgumentException("Savepoint not found in undo log: " + name);
        }
        
        return appliedCount;
    }
    
    /**
     * Helper: rolls back to marker in spill file by reading backwards.
     * Truncates the file at the marker position and rewrites the marker to preserve it.
     */
    private int rollbackToMarkerInSpill(Database database, String name) throws IOException {
        if (spillFile == null || !Files.exists(spillFile)) {
            return 0;
        }
        
        int appliedCount = 0;
        long markerPosition = -1;
        
        try (RandomAccessFile raf = new RandomAccessFile(spillFile.toFile(), "r")) {
            long fileLength = raf.length();
            long position = fileLength;
            
            // Read backwards to find the marker
            while (position > 0) {
                // Read the 4-byte length trailer
                position -= 4;
                if (position < 0) break;
                raf.seek(position);
                int length = raf.readInt();
                if (length < 0 || length > position) {
                    throw new IOException("Corrupt undo spill file: record length " + length + " at offset " + position);
                }
                
                // Read record data
                position -= length;
                raf.seek(position);
                byte[] data = new byte[length];
                raf.readFully(data);
                
                try {
                    UndoRecord record = UndoRecord.deserialize(data);
                    if (record instanceof SavepointMarkerUndo marker && marker.getName().equals(name)) {
                        markerPosition = position;
                        break;
                    }
                    
                    // Apply the record (it's newer than the target marker)
                    applyRecord(record, database);
                    appliedCount++;
                } catch (ClassNotFoundException e) {
                    System.err.println("Failed to deserialize undo record: " + e.getMessage());
                }
            }
        }
        
        if (markerPosition == -1) {
            return 0; // Marker not found in spill file
        }
        
        // Truncate file at marker position and rewrite the marker
        try (RandomAccessFile raf = new RandomAccessFile(spillFile.toFile(), "rw")) {
            // Truncate to marker start
            raf.setLength(markerPosition);
            
            // Rewrite the marker at the end
            SavepointMarkerUndo marker = new SavepointMarkerUndo(name);
            byte[] markerData = marker.serialize();
            raf.seek(markerPosition);
            raf.write(markerData);
            raf.writeInt(markerData.length); // Trailer framing
        }
        
        return appliedCount;
    }
    
    /** Resolves the record's table and applies the record, when table context is known. */
    private void applyRecord(UndoRecord record, Database database) {
        String tableName = record.getTableName();
        if (tableName == null || database == null) {
            return;
        }
        Table table = database.getTable(tableName);
        if (table != null) {
            record.apply(table);
        }
    }
    
    /**
     * Applies spilled records in reverse order by reading the file backwards.
     */
    private void applySpilledRecordsReverse(Database database) throws IOException {
        if (!Files.exists(spillFile)) {
            return;
        }
        
        try (RandomAccessFile raf = new RandomAccessFile(spillFile.toFile(), "r")) {
            long fileLength = raf.length();
            long position = fileLength;
            
            while (position > 0) {
                // Read the 4-byte length trailer of the last record
                position -= 4;
                if (position < 0) {
                    break;
                }
                raf.seek(position);
                int length = raf.readInt();
                if (length < 0 || length > position) {
                    throw new IOException("Corrupt undo spill file: record length "
                            + length + " at offset " + position);
                }
                
                // Read record data
                position -= length;
                raf.seek(position);
                byte[] data = new byte[length];
                raf.readFully(data);
                
                // Deserialize and apply
                try {
                    UndoRecord record = UndoRecord.deserialize(data);
                    applyRecord(record, database);
                } catch (ClassNotFoundException e) {
                    // Log error but continue processing other records
                    System.err.println("Failed to deserialize undo record: " + e.getMessage());
                }
            }
        }
    }
    
    /**
     * Clears all undo records and cleans up spill file.
     */
    public void clear() throws IOException {
        inMemoryRecords.clear();
        inMemoryBytes.set(0);
        
        if (spillOut != null) {
            spillOut.close();
            spillOut = null;
        }
        
        if (spillFile != null) {
            Files.deleteIfExists(spillFile);
            spillFile = null;
        }
    }
    
    /**
     * Returns the number of in-memory records.
     */
    public int getInMemoryRecordCount() {
        return inMemoryRecords.size();
    }
    
    /**
     * Returns the current memory usage in bytes.
     */
    public long getMemoryUsage() {
        return inMemoryBytes.get();
    }
    
    /**
     * Returns true if the log is empty.
     */
    public boolean isEmpty() {
        return inMemoryRecords.isEmpty() && (spillFile == null || !Files.exists(spillFile));
    }

    /**
     * Returns the spill file holding records that exceeded the memory
     * threshold, or null when nothing has spilled yet. The file lives in the
     * system temp directory and is deleted by {@link #clear()}.
     *
     * @return the spill file path, or null
     */
    public Path getSpillFile() {
        return spillFile;
    }
    
    /**
     * Estimates the size of a record for accounting purposes.
     */
    private long estimateRecordSize(UndoRecord record) {
        // Rough estimate - actual size varies by record type
        return 100; // Average size in bytes
    }
}