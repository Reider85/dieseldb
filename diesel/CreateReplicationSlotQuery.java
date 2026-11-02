package diesel;

/**
 * Executes a CREATE REPLICATION SLOT statement: creates a replication slot
 * to track standby progress and prevent WAL deletion (prompt4 #59).
 */
public class CreateReplicationSlotQuery implements Query<String> {
    private final String slotName;

    public CreateReplicationSlotQuery(String slotName) {
        this.slotName = slotName;
    }

    @Override
    public String execute(Table table) {
        // Get the database from the table
        Database database = table.getDatabase();
        
        // Get the current WAL end LSN as the initial LSN for the slot
        // If WAL is disabled, use a reasonable default LSN
        long initialLsn;
        if (database.getWalManager() != null) {
            initialLsn = database.getWalManager().getLastLsn(); // Use current WAL end
        } else {
            // Use a default LSN if WAL is disabled
            initialLsn = 1000;
        }
        
        // Create the replication slot
        database.createReplicationSlot(slotName, initialLsn);
        
        return "Replication slot created: " + slotName;
    }



    public String getSlotName() {
        return slotName;
    }

    @Override
    public Table extractTableForQuery(Database database, long txid) {
        // For replication slot operations, we use the system table
        return database.getTable("diesel_replication_slots");
    }
}