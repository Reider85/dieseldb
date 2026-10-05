package diesel;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
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
            // Mark the row as not inserted (restore xmin=0) or remove it
            RowVersionMeta meta = table.getRowVersionMeta(rowIndex);
            if (meta != null) {
                meta.markAborted();
            }
            // The TxStatusTracker entry for the creating transaction turns the
            // row invisible to every reader; physical reclamation happens when
            // the table compacts its tombstones.
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
            // Restore old values
            Map<String, Object> currentValues = table.getRows().get(rowIndex);
            if (oldValues != null) {
                currentValues.clear();
                currentValues.putAll(oldValues);
            }
            
            // Restore old metadata
            if (oldMeta != null) {
                table.setRowVersionMeta(rowIndex, oldMeta);
            }
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
            // Restore old metadata (clear xmax)
            RowVersionMeta meta = table.getRowVersionMeta(rowIndex);
            if (meta != null && oldMeta != null) {
                meta.setXmax(oldMeta.getXmax());
                meta.setUncommittedDelete(oldMeta.isUncommittedDelete());
            }
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