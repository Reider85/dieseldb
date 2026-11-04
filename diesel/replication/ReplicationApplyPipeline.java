package diesel.replication;

import diesel.Database;
import diesel.wal.WALManager;
import diesel.wal.WALEntry;
import diesel.wal.WALSegment;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles the replication apply pipeline for logical replication.
 * Monitors WAL entries and applies them to the subscriber database.
 */
public class ReplicationApplyPipeline {
    
    private final Database subscriberDatabase;
    private final WALManager masterWalManager;
    private final Subscription subscription;
    private final Publication publication;
    private final LogicalDecoder logicalDecoder;
    
    private final AtomicLong currentLsn = new AtomicLong(0);
    private final Map<String, Long> tableLsnMap = new ConcurrentHashMap<>();
    private volatile boolean running = false;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    
    public ReplicationApplyPipeline(Database subscriberDatabase, WALManager masterWalManager, Subscription subscription) {
        this.subscriberDatabase = subscriberDatabase;
        this.masterWalManager = masterWalManager;
        this.subscription = subscription;
        this.publication = subscriberDatabase.getReplicationRegistry().getPublication(subscription.getPublicationName());
        this.logicalDecoder = new LogicalDecoder(masterWalManager, publication);
    }
    
    /**
     * Starts the replication apply pipeline.
     */
    public void start() {
        if (running) {
            return;
        }
        
        running = true;
        
        // Schedule the replication apply task to run every 100ms
        executor.scheduleAtFixedRate(() -> {
            try {
                applyChanges();
            } catch (Exception e) {
                // Log error but continue running
                System.err.println("Replication apply error: " + e.getMessage());
            }
        }, 0, 100, TimeUnit.MILLISECONDS);
        
        System.out.println("Replication apply pipeline started for subscription: " + subscription.getName());
    }
    
    /**
     * Stops the replication apply pipeline.
     */
    public void stop() {
        if (!running) {
            return;
        }
        
        running = false;
        executor.shutdown();
        
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        System.out.println("Replication apply pipeline stopped for subscription: " + subscription.getName());
    }
    
/**
      * Applies changes from the master database to the subscriber.
      */
    private void applyChanges() {
        if (!running || publication == null) {
            return;
        }
        
        try {
            // Get the current position from the subscription
            long startLsn = getCurrentLsn();
            System.out.println("DEBUG: Replication apply starting from LSN: " + startLsn);
            
            // Decode WAL entries from the master, start from next LSN after confirmed
            List<LogicalChangeRecord> changes = logicalDecoder.decode(startLsn + 1, 100);
            
            System.out.println("DEBUG: Found " + changes.size() + " changes to apply");
            
            if (changes.isEmpty()) {
                return;
            }
            
            boolean anyChangeApplied = false;
            
            // Apply each change to the subscriber database
            for (LogicalChangeRecord change : changes) {
                // Skip changes that have already been confirmed
                if (change.getLsn() <= subscription.getConfirmedLsn()) {
                    System.out.println("DEBUG: Skipping already applied change with LSN: " + change.getLsn() + ", confirmed LSN: " + subscription.getConfirmedLsn());
                    continue;
                }
                
                System.out.println("DEBUG: Applying change: " + change);
                applyChange(change);
                anyChangeApplied = true;
                
                // Update the subscription's confirmed LSN
                subscription.setConfirmedLsn(change.getLsn());
                
                // Update table LSN tracking
                tableLsnMap.put(change.getTableName(), change.getLsn());
                
                System.out.println("DEBUG: Applied change, updated confirmed LSN to: " + subscription.getConfirmedLsn());
            }
            
            // Save the current position
            if (anyChangeApplied) {
                saveCurrentPosition();
                System.out.println("DEBUG: Updated confirmed LSN to: " + subscription.getConfirmedLsn());
            } else {
                System.out.println("DEBUG: No new changes applied, confirmed LSN remains: " + subscription.getConfirmedLsn());
            }
            
        } catch (Exception e) {
            System.err.println("Error applying replication changes: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Applies a single logical change record to the subscriber database.
     */
    private void applyChange(LogicalChangeRecord change) {
        String tableName = change.getTableName();
        
        System.out.println("DEBUG: Applying change to table: " + tableName + ", operation: " + change.getOperation());
        
        // Skip tables not in the publication
        if (!publication.containsTable(tableName)) {
            System.out.println("DEBUG: Table " + tableName + " is not in publication, skipping");
            return;
        }
        
        try {
            switch (change.getOperation()) {
                case INSERT:
                    applyInsert(change);
                    break;
                case UPDATE:
                    applyUpdate(change);
                    break;
                case DELETE:
                    applyDelete(change);
                    break;
            }
            System.out.println("DEBUG: Successfully applied change: " + change);
        } catch (Exception e) {
            System.err.println("ERROR: Failed to apply change: " + change + ", error: " + e.getMessage());
            // Handle conflicts based on the subscription's conflict resolution strategy
            handleConflict(change, e);
        }
    }
    
    /**
     * Applies an INSERT operation to the subscriber database.
     */
    private void applyInsert(LogicalChangeRecord change) {
        Map<String, Object> values = change.getAfterValues();
        if (values == null || values.isEmpty()) {
            return;
        }
        
        // Build INSERT query with actual values
        StringBuilder columns = new StringBuilder();
        StringBuilder valuesStr = new StringBuilder();
        
        boolean first = true;
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (!first) {
                columns.append(", ");
                valuesStr.append(", ");
            }
            columns.append(entry.getKey());
            
            // Format the value properly
            Object value = entry.getValue();
            if (value instanceof String) {
                valuesStr.append("'").append(value).append("'");
            } else if (value instanceof Character) {
                valuesStr.append("'").append(value).append("'");
            } else {
                valuesStr.append(value);
            }
            first = false;
        }
        
        String query = String.format("INSERT INTO %s (%s) VALUES (%s)", 
            change.getTableName(), columns.toString(), valuesStr.toString());
        
        System.out.println("DEBUG: Applying INSERT query: " + query);
        
        // Execute the INSERT
        subscriberDatabase.executeQuery(query, null);
    }
    
    /**
     * Applies an UPDATE operation to the subscriber database.
     */
    private void applyUpdate(LogicalChangeRecord change) {
        Map<String, Object> beforeValues = change.getBeforeValues();
        Map<String, Object> afterValues = change.getAfterValues();
        
        if (afterValues == null || afterValues.isEmpty()) {
            return;
        }
        
        // Build UPDATE query with actual values
        StringBuilder setClause = new StringBuilder();
        StringBuilder whereClause = new StringBuilder();
        
        boolean firstSet = true;
        boolean firstWhere = true;
        
        // Build SET clause with actual values
        for (Map.Entry<String, Object> entry : afterValues.entrySet()) {
            String column = entry.getKey();
            Object value = entry.getValue();
            
            if (!firstSet) {
                setClause.append(", ");
            }
            setClause.append(column).append(" = ");
            
            // Format the value properly
            if (value instanceof String) {
                setClause.append("'").append(value).append("'");
            } else if (value instanceof Character) {
                setClause.append("'").append(value).append("'");
            } else {
                setClause.append(value);
            }
            firstSet = false;
        }
        
        // Build WHERE clause with actual values
        if (beforeValues != null) {
            for (Map.Entry<String, Object> entry : beforeValues.entrySet()) {
                String column = entry.getKey();
                Object value = entry.getValue();
                
                if (!firstWhere) {
                    whereClause.append(" AND ");
                }
                whereClause.append(column).append(" = ");
                
                // Format the value properly
                if (value instanceof String) {
                    whereClause.append("'").append(value).append("'");
                } else if (value instanceof Character) {
                    whereClause.append("'").append(value).append("'");
                } else {
                    whereClause.append(value);
                }
                firstWhere = false;
            }
        }
        
        if (whereClause.length() == 0) {
            // If no before values, use the after values as a fallback
            for (Map.Entry<String, Object> entry : afterValues.entrySet()) {
                String column = entry.getKey();
                Object value = entry.getValue();
                
                if (!firstWhere) {
                    whereClause.append(" AND ");
                }
                whereClause.append(column).append(" = ");
                
                // Format the value properly
                if (value instanceof String) {
                    whereClause.append("'").append(value).append("'");
                } else if (value instanceof Character) {
                    whereClause.append("'").append(value).append("'");
                } else {
                    whereClause.append(value);
                }
                firstWhere = false;
            }
        }
        
        String query = String.format("UPDATE %s SET %s WHERE %s", 
            change.getTableName(), setClause.toString(), whereClause.toString());
        
        System.out.println("DEBUG: Applying UPDATE query: " + query);
        
        // Execute the UPDATE
        subscriberDatabase.executeQuery(query, null);
    }
    
    /**
     * Applies a DELETE operation to the subscriber database.
     */
    private void applyDelete(LogicalChangeRecord change) {
        Map<String, Object> beforeValues = change.getBeforeValues();
        if (beforeValues == null || beforeValues.isEmpty()) {
            return;
        }
        
        // Build DELETE query with actual values
        StringBuilder whereClause = new StringBuilder();
        
        boolean first = true;
        for (Map.Entry<String, Object> entry : beforeValues.entrySet()) {
            if (!first) {
                whereClause.append(" AND ");
            }
            whereClause.append(entry.getKey()).append(" = ");
            
            // Format the value properly
            Object value = entry.getValue();
            if (value instanceof String) {
                whereClause.append("'").append(value).append("'");
            } else if (value instanceof Character) {
                whereClause.append("'").append(value).append("'");
            } else {
                whereClause.append(value);
            }
            first = false;
        }
        
        String query = String.format("DELETE FROM %s WHERE %s", 
            change.getTableName(), whereClause.toString());
        
        System.out.println("DEBUG: Applying DELETE query: " + query);
        
        // Execute the DELETE
        subscriberDatabase.executeQuery(query, null);
    }
    
    /**
     * Handles conflicts based on the subscription's conflict resolution strategy.
     */
    private void handleConflict(LogicalChangeRecord change, Exception e) {
        switch (subscription.getConflictResolution()) {
            case ERROR:
                // Re-throw the exception
                throw new RuntimeException("Conflict applying change: " + change, e);
                
            case SKIP:
                // Skip the conflicting change
                System.err.println("Skipping conflicting change: " + change);
                break;
                
            case REPLACE:
                // Apply the change anyway (overwrite)
                System.err.println("Applying conflicting change with REPLACE strategy: " + change);
                try {
                    applyChangeForce(change);
                } catch (Exception ex) {
                    System.err.println("Failed to apply conflicting change: " + ex.getMessage());
                }
                break;
        }
    }
    
    /**
     * Applies a change forcefully (ignoring conflicts).
     * For INSERT this means deleting the conflicting row by primary key first,
     * otherwise the same duplicate-key failure would just repeat.
     */
    private void applyChangeForce(LogicalChangeRecord change) {
        switch (change.getOperation()) {
            case INSERT:
                deleteConflictingRow(change);
                applyInsert(change);
                break;
            case UPDATE:
                applyUpdate(change);
                break;
            case DELETE:
                applyDelete(change);
                break;
        }
    }

    /**
     * Deletes the existing row with the same primary key (REPLACE strategy).
     */
    private void deleteConflictingRow(LogicalChangeRecord change) {
        Map<String, Object> values = change.getAfterValues();
        if (values == null || values.isEmpty()) {
            return;
        }
        try {
            String pk = subscriberDatabase.getPrimaryKeyColumn(change.getTableName());
            if (pk == null) {
                return;
            }
            Object pkValue = null;
            for (Map.Entry<String, Object> entry : values.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(pk)) {
                    pkValue = entry.getValue();
                    break;
                }
            }
            if (pkValue == null) {
                return;
            }
            String literal = (pkValue instanceof String || pkValue instanceof Character)
                    ? "'" + pkValue + "'" : String.valueOf(pkValue);
            subscriberDatabase.executeQuery(
                    "DELETE FROM " + change.getTableName() + " WHERE " + pk + " = " + literal, null);
        } catch (Exception e) {
            System.err.println("REPLACE: could not delete conflicting row: " + e.getMessage());
        }
    }
    
/**
     * Gets the current replication position.
     */
    public long getCurrentLsn() {
        return subscription.getConfirmedLsn();
    }
    
    /**
     * Sets the current replication position.
     */
    public void setCurrentLsn(long lsn) {
        currentLsn.set(lsn);
    }
    
    /**
     * Saves the current position to the subscription.
     */
    private void saveCurrentPosition() {
        // In a real implementation, this would save the position to persistent storage
        // For now, we'll just update the subscription's confirmed LSN
        // This would typically be stored in the replication slot
    }
    
    /**
     * Gets the replication status.
     */
    public boolean isRunning() {
        return running;
    }
    
    /**
     * Gets the name of the subscription.
     */
    public String getSubscriptionName() {
        return subscription.getName();
    }
    
    /**
     * Gets the publication name.
     */
    public String getPublicationName() {
        return subscription.getPublicationName();
    }
}