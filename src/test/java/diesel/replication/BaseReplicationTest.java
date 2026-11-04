package diesel.replication;

import diesel.Database;
import diesel.wal.FsyncPolicy;
import diesel.wal.WALConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Base class for replication tests that sets up database instances.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class BaseReplicationTest {
    
    protected Database master;
    protected Database subscriber;
    protected Publication publication;
    protected Subscription subscription;
    
    /**
     * Per-class data directories: subclasses run in parallel, so they must
     * not share (and clean) the same data/WAL directories.
     */
    protected String masterDir() {
        return "data/rt_" + getClass().getSimpleName().toLowerCase(java.util.Locale.ROOT) + "_master";
    }
    
    protected String subscriberDir() {
        return "data/rt_" + getClass().getSimpleName().toLowerCase(java.util.Locale.ROOT) + "_subscriber";
    }
    
    @BeforeEach
    void setUp() throws Exception {
        // Clean up any existing data directories
        cleanDirectory(masterDir());
        cleanDirectory(subscriberDir());
        
        // Create master database with its own WAL directory (a shared ./wal
        // for two Database instances corrupts the segment: both writers keep
        // their own writePosition and overwrite each other's entries)
        master = new Database(masterDir(), enabledWalConfig(Paths.get(masterDir() + "/wal")));
        dropIfPresent(master, "test_table");
        master.executeQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING, value INTEGER)", null);
        
        // Create subscriber database with its own WAL directory
        subscriber = new Database(subscriberDir(), enabledWalConfig(Paths.get(subscriberDir() + "/wal")));
        dropIfPresent(subscriber, "test_table");
        subscriber.executeQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name STRING, value INTEGER)", null);
        
        // Create publication in both master and subscriber so it can find it when creating subscription
        publication = new Publication("test_pub", Set.of("test_table"), Publication.ConflictResolutionStrategy.ERROR);
        System.out.println("DEBUG: Creating publication 'test_pub' with tables: " + Set.of("test_table"));
        master.getLogicalReplicationRegistry().createPublication(publication);
        System.out.println("DEBUG: Publication created in master");
        subscriber.getLogicalReplicationRegistry().createPublication(publication);
        System.out.println("DEBUG: Publication created in subscriber");
        
        // Create subscription
        subscription = new Subscription("test_sub", "test_pub", "localhost", 5432, 
            Subscription.ConflictResolutionStrategy.ERROR, true);
        System.out.println("DEBUG: Creating subscription 'test_sub' for publication 'test_pub'");
        subscriber.getLogicalReplicationRegistry().createSubscription(subscription);
        System.out.println("DEBUG: Subscription created successfully");
    }
    
    @AfterEach
    void tearDown() {
        if (master != null) {
            master.close();
        }
        if (subscriber != null) {
            subscriber.close();
        }
        
        // Clean up WAL to ensure clean state for next test
        cleanupWal();
    }
    
    /**
     * WAL config for a test database: enabled, own directory, group commit.
     */
    protected static WALConfig enabledWalConfig(Path walDir) {
        return WALConfig.of(walDir, WALConfig.MIN_SEGMENT_SIZE_BYTES, 100_000,
                WALConfig.DISABLED, walDir.resolve("archive"), 0, WALConfig.DISABLED,
                true, FsyncPolicy.GROUP, 5, 64);
    }

    /**
     * Drops a table if a stale copy survived directory cleanup (no
     * DROP TABLE IF EXISTS support in the parser).
     */
    protected static void dropIfPresent(Database db, String table) {
        try {
            db.executeQuery("DROP TABLE " + table, null);
        } catch (Exception ignored) {
            // table absent - the normal case
        }
        try {
            db.executeQuery("DELETE FROM diesel_replication_registry", null);
        } catch (Exception ignored) {
            // registry table not created yet
        }
    }

    private void cleanDirectory(String dirPath) throws Exception {
        // Delete with verify-retry: under parallel-suite load a just-closed
        // Database can still hold (or re-create via flusher) files for a few
        // tens of ms, and a silently failed delete lets the next setUp reload
        // stale tables (e.g. "Table PRODUCTS already exists").
        for (int attempt = 0; attempt < 20; attempt++) {
            Path path = Paths.get(dirPath);
            if (Files.exists(path)) {
                try (java.util.stream.Stream<Path> walk = Files.walk(path)) {
                    walk.filter(Files::isRegularFile).forEach(file -> {
                        try {
                            Files.delete(file);
                        } catch (Exception e) {
                            // locked; retried on the next attempt
                        }
                    });
                }
            }
            boolean anyLeft;
            try (java.util.stream.Stream<Path> walk = Files.exists(path) ? Files.walk(path) : java.util.stream.Stream.empty()) {
                anyLeft = walk.anyMatch(Files::isRegularFile);
            }
            if (!anyLeft) {
                return;
            }
            Thread.sleep(50);
        }
    }
    
    protected void insertTestData(int id, String name, int value) {
        // Start transaction
        String beginResult = (String) master.executeQuery("BEGIN", null);
        UUID txId = UUID.fromString(beginResult.split(": ")[1]);
        
        // Execute INSERT within the transaction
        master.executeQuery("INSERT INTO test_table (id, name, value) VALUES (" + id + ", '" + name + "', " + value + ")", txId);
        
        // Commit the transaction
        master.executeQuery("COMMIT", txId);
        
        // Wait for replication
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    protected int getSubscriberCount() {
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT COUNT(*) as count FROM test_table", null);
        return ((Number) results.get(0).get("count")).intValue();
    }
    
    protected Map<String, Object> getSubscriberRow(int id) {
        List<Map<String, Object>> results = (List<Map<String, Object>>) subscriber.executeQuery("SELECT * FROM test_table WHERE id = " + id, null);
        return results.isEmpty() ? null : results.get(0);
    }
    
    /**
     * Create a fresh database instance with clean data directory and WAL
     */
    protected Database createFreshDatabase(String name) {
        // Clean up any existing directory
        File dataDir = new File("data/" + name);
        if (dataDir.exists()) {
            deleteDirectory(dataDir);
        }
        
        // Create fresh database under data/ (gitignored) with its own WAL directory
        Database db = new Database("data/" + name, enabledWalConfig(Paths.get("data/" + name + "/wal")));
        
        // Force checkpoint to clean up any residual WAL segments
        try {
            if (db.getWalManager() != null) {
                db.getWalManager().writeCheckpoint(java.util.Collections.emptyList());
            }
        } catch (Exception e) {
            System.err.println("Warning: Could not clean up WAL: " + e.getMessage());
        }
        
        return db;
    }
    
    /**
     * Recursively delete a directory and all its contents
     */
    protected void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }
    
    /**
     * Clean up WAL segments to ensure a clean test state
       */
    protected void cleanupWal() {
        try {
            // Clean up WAL files after each test to prevent historical data pollution
            cleanDirectory(masterDir());
            cleanDirectory(subscriberDir());
            
            // Reset subscription confirmed LSN
            if (subscription != null) {
                subscription.setConfirmedLsn(0);
            }
            
            System.out.println("Cleaning up WAL for clean test state");
        } catch (Exception e) {
            System.err.println("Error during WAL cleanup: " + e.getMessage());
        }
    }
    
    /**
     * Force a complete WAL reset for a database
     */
    protected void resetWal(Database db) {
        try {
            if (db != null && db.getWalManager() != null) {
                // Force checkpoint to clean up segments
                db.getWalManager().writeCheckpoint(java.util.Collections.emptyList());
                System.out.println("WAL checkpoint completed for database");
                
                // Clean up any WAL files manually
                File walDir = new File("data/" + db.getDataDir() + "/wal");
                if (walDir.exists()) {
                    // Delete all WAL files
                    File[] walFiles = walDir.listFiles((dir, name) -> name.endsWith(".wal"));
                    if (walFiles != null) {
                        for (File walFile : walFiles) {
                            if (!walFile.delete()) {
                                System.err.println("Failed to delete WAL file: " + walFile.getAbsolutePath());
                            }
                        }
                    }
                    
                    // Delete any checkpoint files
                    File[] checkpointFiles = walDir.listFiles((dir, name) -> name.endsWith(".checkpoint"));
                    if (checkpointFiles != null) {
                        for (File checkpointFile : checkpointFiles) {
                            if (!checkpointFile.delete()) {
                                System.err.println("Failed to delete checkpoint file: " + checkpointFile.getAbsolutePath());
                            }
                        }
                    }
                    
                    // Delete the checkpoint pointer file specifically
                    File checkpointPtr = new File(walDir, "checkpoint.ptr");
                    if (checkpointPtr.exists()) {
                        if (!checkpointPtr.delete()) {
                            System.err.println("Failed to delete checkpoint.ptr: " + checkpointPtr.getAbsolutePath());
                        }
                    }
                }
                
                // Force garbage collection to clean up any file handles
                System.gc();
            }
        } catch (Exception e) {
            System.err.println("Error during WAL reset: " + e.getMessage());
        }
    }
    
    /**
     * Completely clear the WAL directory for a database
     */
    protected void clearWalDirectory(Database db) {
        try {
            if (db != null) {
                File walDir = new File("data/" + db.getDataDir() + "/wal");
                System.out.println("WAL directory exists: " + walDir.exists());
                if (walDir.exists()) {
                    // List files before deletion
                    File[] files = walDir.listFiles();
                    System.out.println("Files in WAL directory before deletion: " + (files != null ? files.length : 0));
                    if (files != null) {
                        for (File file : files) {
                            System.out.println("File: " + file.getName() + ", size: " + file.length());
                        }
                    }
                    
                    // Delete all files in the WAL directory
                    if (files != null) {
                        for (File file : files) {
                            if (!file.delete()) {
                                System.err.println("Failed to delete file: " + file.getAbsolutePath());
                            }
                        }
                    }
                    
                    // Verify deletion
                    files = walDir.listFiles();
                    System.out.println("Files in WAL directory after deletion: " + (files != null ? files.length : 0));
                    System.out.println("Cleared WAL directory for database: " + db.getDataDir());
                }
                
                // Also clear the parent data directory to remove any checkpoint files
                File dataDir = new File("data/" + db.getDataDir());
                if (dataDir.exists()) {
                    File[] dataFiles = dataDir.listFiles();
                    if (dataFiles != null) {
                        for (File dataFile : dataFiles) {
                            if (dataFile.getName().endsWith(".checkpoint") || dataFile.getName().endsWith(".wal")) {
                                if (!dataFile.delete()) {
                                    System.err.println("Failed to delete checkpoint/WAL file: " + dataFile.getAbsolutePath());
                                }
                            }
                        }
                    }
                }
                
                // Delete the entire WAL directory and recreate it to ensure it's completely clean
                if (walDir.exists()) {
                    if (!walDir.delete()) {
                        System.err.println("Failed to delete WAL directory: " + walDir.getAbsolutePath());
                    } else {
                        System.out.println("Deleted WAL directory: " + walDir.getAbsolutePath());
                    }
                }
                
                // Recreate the WAL directory
                if (walDir.mkdirs()) {
                    System.out.println("Recreated WAL directory: " + walDir.getAbsolutePath());
                } else {
                    System.err.println("Failed to recreate WAL directory: " + walDir.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            System.err.println("Error clearing WAL directory: " + e.getMessage());
        }
    }
}