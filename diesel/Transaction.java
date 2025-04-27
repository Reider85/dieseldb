package diesel;

import java.io.*;
import java.util.*;

enum IsolationLevel {
    READ_UNCOMMITTED,
    // Future levels: READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
}

class Transaction {
    private final UUID transactionId;
    private final IsolationLevel isolationLevel;
    private final Map<String, Table> originalTables; // Snapshot of tables at transaction start
    private final Map<String, Table> modifiedTables; // Changes during transaction
    private boolean active;

    public Transaction(IsolationLevel isolationLevel) {
        this.transactionId = UUID.randomUUID();
        this.isolationLevel = isolationLevel != null ? isolationLevel : IsolationLevel.READ_UNCOMMITTED; // Default
        this.originalTables = new HashMap<>();
        this.modifiedTables = new HashMap<>();
        this.active = true;
    }

    public UUID getTransactionId() {
        return transactionId;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public boolean isActive() {
        return active;
    }

    public void setInactive() {
        this.active = false;
    }

    public void snapshotTable(String tableName, Table table) {
        try {
            // Deep copy the table to preserve original state
            Table clonedTable = cloneTable(table);
            originalTables.put(tableName, clonedTable);
        } catch (Exception e) {
            throw new RuntimeException("Failed to snapshot table: " + tableName, e);
        }
    }

    public void updateTable(String tableName, Table table) {
        try {
            // Deep copy to store modified state
            Table clonedTable = table != null ? cloneTable(table) : null;
            modifiedTables.put(tableName, clonedTable);
        } catch (Exception e) {
            throw new RuntimeException("Failed to update transaction table: " + tableName, e);
        }
    }

    public Map<String, Table> getOriginalTables() {
        return originalTables;
    }

    public Map<String, Table> getModifiedTables() {
        return modifiedTables;
    }

    private Table cloneTable(Table table) throws IOException, ClassNotFoundException {
        // Serialize and deserialize to create a deep copy
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(table);
        oos.close();

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        Table clonedTable = (Table) ois.readObject();
        ois.close();
        return clonedTable;
    }
}