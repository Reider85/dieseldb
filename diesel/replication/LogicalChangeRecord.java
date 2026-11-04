package diesel.replication;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a logical change event from WAL decoding.
 * Contains table name, operation type, and before/after row data.
 */
public class LogicalChangeRecord {
    
    public enum Operation {
        INSERT, UPDATE, DELETE
    }
    
    private final String tableName;
    private final Operation operation;
    private final Map<String, Object> beforeValues;
    private final Map<String, Object> afterValues;
    private final long lsn;
    private final long transactionId;
    
    public LogicalChangeRecord(String tableName, Operation operation, 
                             Map<String, Object> beforeValues, Map<String, Object> afterValues,
                             long lsn, long transactionId) {
        this.tableName = tableName;
        this.operation = operation;
        this.beforeValues = beforeValues;
        this.afterValues = afterValues;
        this.lsn = lsn;
        this.transactionId = transactionId;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public Operation getOperation() {
        return operation;
    }
    
    public Map<String, Object> getBeforeValues() {
        return beforeValues;
    }
    
    public Map<String, Object> getAfterValues() {
        return afterValues;
    }
    
    public long getLsn() {
        return lsn;
    }
    
    public long getTransactionId() {
        return transactionId;
    }
    
    public boolean hasBeforeValues() {
        return beforeValues != null && !beforeValues.isEmpty();
    }
    
    public boolean hasAfterValues() {
        return afterValues != null && !afterValues.isEmpty();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogicalChangeRecord that = (LogicalChangeRecord) o;
        return lsn == that.lsn && 
               transactionId == that.transactionId &&
               Objects.equals(tableName, that.tableName) &&
               operation == that.operation &&
               Objects.equals(beforeValues, that.beforeValues) &&
               Objects.equals(afterValues, that.afterValues);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(tableName, operation, beforeValues, afterValues, lsn, transactionId);
    }
    
    @Override
    public String toString() {
        return String.format("LogicalChangeRecord{tableName='%s', operation=%s, lsn=%d, txid=%d, before=%s, after=%s}",
                           tableName, operation, lsn, transactionId, beforeValues, afterValues);
    }
}