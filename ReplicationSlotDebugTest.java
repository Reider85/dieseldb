package diesel.replication;

import diesel.Database;
import diesel.CreateReplicationSlotQuery;

/**
 * Simple test to debug replication slot update issue.
 */
public class ReplicationSlotDebugTest {
    public static void main(String[] args) {
        String testDir = "debug_test";
        
        // Clean up any existing directory
        java.io.File dir = new java.io.File(testDir);
        if (dir.exists()) {
            deleteDirectory(dir);
        }
        
        // Create database
        Database database = new Database(testDir);
        
        // Create slot
        CreateReplicationSlotQuery createQuery = new CreateReplicationSlotQuery("debug_slot");
        createQuery.execute(database, -1);
        
        // Check initial value
        ReplicationSlot slot = database.getReplicationSlot("debug_slot");
        System.out.println("Initial confirmedFlushLsn: " + slot.getConfirmedFlushLsn());
        
        // Update slot
        boolean updated = database.updateReplicationSlotConfirmedFlushLsn("debug_slot", 7000);
        System.out.println("Update result: " + updated);
        
        // Check updated value
        slot = database.getReplicationSlot("debug_slot");
        System.out.println("Updated confirmedFlushLsn: " + slot.getConfirmedFlushLsn());
        
        // Check database file
        java.io.File csvFile = new java.io.File(testDir, "diesel_replication_slots.csv");
        if (csvFile.exists()) {
            try {
                java.util.List<String> lines = java.nio.file.Files.readAllLines(csvFile.toPath());
                for (String line : lines) {
                    System.out.println("CSV line: " + line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        // Close database
        database.close();
        
        // Reopen database
        database = new Database(testDir);
        
        // Check value after reopen
        slot = database.getReplicationSlot("debug_slot");
        System.out.println("After reopen confirmedFlushLsn: " + slot.getConfirmedFlushLsn());
        
        // Clean up
        deleteDirectory(dir);
    }
    
    private static void deleteDirectory(java.io.File directory) {
        if (directory.exists()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) {
                for (java.io.File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}