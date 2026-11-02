package diesel;

/**
 * Executes a DROP REPLICATION SLOT statement: removes a replication slot
 * and allows WAL segments to be deleted (prompt4 #59).
 */
public class DropReplicationSlotQuery implements Query<String> {
    private final String slotName;

    public DropReplicationSlotQuery(String slotName) {
        this.slotName = slotName;
    }

    @Override
    public String execute(Table table) {
        // Get the database from the table
        Database database = table.getDatabase();
        
        // Drop the replication slot
        boolean dropped = database.dropReplicationSlot(slotName);
        
        if (dropped) {
            return "Replication slot dropped: " + slotName;
        } else {
            return "Replication slot not found: " + slotName;
        }
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