package diesel.backup;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PhysicalBackup and PhysicalRestore functionality.
 */
@Tag("storage")
public class PhysicalBackupRestoreTest {
    
    @TempDir
    Path tempDir;
    
    private Path databaseDir;
    private Path backupDir;
    private Path restoreDir;
    
    @BeforeEach
    void setUp() {
        databaseDir = tempDir.resolve("database");
        backupDir = tempDir.resolve("backup");
        restoreDir = tempDir.resolve("restored");
        
        try {
            Files.createDirectories(databaseDir);
            Files.createDirectories(backupDir);
            Files.createDirectories(restoreDir);
            
            // Create some dummy database files
            createDummyDatabaseFiles();
            
            // Create a dummy checkpoint file for testing
            String checkpointData = "{\"version\": 1, \"lsn\": 1000, \"timestamp\": 1640995200000}";
            Files.writeString(databaseDir.resolve("checkpoint.json"), checkpointData);
        } catch (IOException e) {
            fail("Setup failed: " + e.getMessage());
        }
    }
    
    @AfterEach
    void tearDown() {
        // Clean up temporary files
    }
    
    @Test
    void testBackupCreation() throws IOException {
        // Test backup creation
        PhysicalBackup backup = new PhysicalBackup(databaseDir, backupDir);
        backup.backup();
        
        // Verify backup files exist
        assertTrue(Files.exists(backupDir.resolve("backup_manifest.json")));
        
        // Verify data files are copied
        assertTrue(Files.exists(backupDir.resolve("users.table")));
        assertTrue(Files.exists(backupDir.resolve("orders.table")));
        assertTrue(Files.exists(backupDir.resolve("transactions.log")));
        
        // Verify manifest content
        BackupManifest manifest = BackupManifest.load(backupDir.resolve("backup_manifest.json"));
        assertNotNull(manifest.getInfo());
        assertEquals(3, manifest.getFileEntries().size());
        assertTrue(manifest.getTotalSize() > 0);
    }
    
    @Test
    void testBackupIntegrityVerification() throws IOException {
        // Create backup
        PhysicalBackup backup = new PhysicalBackup(databaseDir, backupDir);
        backup.backup();
        
        // Verify integrity
        BackupManifest manifest = BackupManifest.load(backupDir.resolve("backup_manifest.json"));
        assertTrue(manifest.verifyIntegrity(backupDir));
    }
    
    @Test
    void testRestoreProcess() throws IOException {
        // First create a backup
        PhysicalBackup backup = new PhysicalBackup(databaseDir, backupDir);
        backup.backup();
        
        // Then restore it
        PhysicalRestore restore = new PhysicalRestore(backupDir, restoreDir);
        restore.restore();
        
        // Verify restore directory has the files
        assertTrue(Files.exists(restoreDir.resolve("backup_manifest.json")));
        assertTrue(Files.exists(restoreDir.resolve("users.table")));
        assertTrue(Files.exists(restoreDir.resolve("orders.table")));
        assertTrue(Files.exists(restoreDir.resolve("transactions.log")));
        
        // Verify backup integrity after restore
        BackupManifest manifest = BackupManifest.load(restoreDir.resolve("backup_manifest.json"));
        assertTrue(manifest.verifyIntegrity(restoreDir));
    }
    
    @Test
    void testBackupStatistics() throws IOException {
        // Create backup
        PhysicalBackup backup = new PhysicalBackup(databaseDir, backupDir);
        backup.backup();
        
        // Load manifest and check statistics
        BackupManifest manifest = BackupManifest.load(backupDir.resolve("backup_manifest.json"));
        var stats = manifest.getStatistics();
        
        assertEquals(3, stats.get("totalFiles"));
        assertTrue((Long) stats.get("totalSize") > 0);
        assertNotNull(stats.get("fileTypes"));
        
        // Check file type counts
        @SuppressWarnings("unchecked")
        var fileTypes = (java.util.Map<String, Long>) stats.get("fileTypes");
        assertEquals(2, fileTypes.get("table"));
        assertEquals(1, fileTypes.get("wal"));
    }
    
    @Test
    void testBackupValidation() throws IOException {
        // First create a simple backup structure to test validation
        Files.createFile(backupDir.resolve("backup_manifest.json"));
        Files.createFile(backupDir.resolve("users.table"));
        
        // Test validation of backup directory (should be true now)
        assertTrue(PhysicalRestore.validateBackupDirectory(backupDir));
        
        // Test validation of empty directory
        Path emptyDir = tempDir.resolve("empty");
        assertTrue(PhysicalRestore.validateBackupDirectory(emptyDir) == false);
    }
    
    @Test
    void testTargetDirectorySafety() throws IOException {
        // Create a minimal valid manifest file in backup directory first
        String minimalManifest = "{\"info\":{\"timestamp\":1640995200000,\"sourceDatabase\":\"test\",\"fileCount\":0,\"totalSize\":0,\"backupType\":\"physical\"},\"fileEntries\":[],\"metadata\":{}}";
        Files.writeString(backupDir.resolve("backup_manifest.json"), minimalManifest);
        
        // Test that empty directory is safe
        PhysicalRestore restore = new PhysicalRestore(backupDir, restoreDir);
        assertTrue(restore.isTargetSafe());
        
        // Create a file in the directory and test again
        Files.createFile(restoreDir.resolve("test.txt"));
        assertFalse(restore.isTargetSafe());
    }
    
    @Test
    void testManifestFileOperations() throws IOException {
        // Create a manifest
        BackupManifest manifest = new BackupManifest();
        
        // Add some file entries
        manifest.addFile("users.table", 1024, "checksum-1024");
        manifest.addFile("orders.table", 2048, "checksum-2048");
        manifest.addFile("transactions.log", 512, "checksum-512");
        
        // Set backup info
        BackupManifest.BackupInfo info = new BackupManifest.BackupInfo(
            System.currentTimeMillis(),
            databaseDir.toString(),
            3,
            3584,
            "physical"
        );
        manifest.setInfo(info);
        
        // Save manifest
        Path manifestPath = backupDir.resolve("test_manifest.json");
        manifest.save(manifestPath);
        
        // Load manifest back
        BackupManifest loadedManifest = BackupManifest.load(manifestPath);
        
        // Verify content
        assertEquals(3, loadedManifest.getFileEntries().size());
        assertEquals(3584, loadedManifest.getTotalSize());
        assertEquals("physical", loadedManifest.getInfo().getBackupType());
        
        // Create dummy files for integrity check
        Files.write(backupDir.resolve("users.table"), new byte[1024]);
        Files.write(backupDir.resolve("orders.table"), new byte[2048]);
        Files.write(backupDir.resolve("transactions.log"), new byte[512]);
        
        // Test integrity verification (should pass with our dummy files)
        assertTrue(loadedManifest.verifyIntegrity(backupDir));
    }
    
    @Test
    void testFileFiltering() throws IOException {
        // Create backup
        PhysicalBackup backup = new PhysicalBackup(databaseDir, backupDir);
        backup.backup();
        
        // Test file filtering by type
        BackupManifest manifest = BackupManifest.load(backupDir.resolve("backup_manifest.json"));
        
        var tableFiles = manifest.getFilesByType("table");
        assertEquals(2, tableFiles.size());
        
        var walFiles = manifest.getFilesByType("wal");
        assertEquals(1, walFiles.size());
        
        var unknownFiles = manifest.getFilesByType("unknown");
        assertEquals(0, unknownFiles.size());
    }
    
    /**
     * Creates dummy database files for testing.
     */
    private void createDummyDatabaseFiles() throws IOException {
        // Create table files
        String usersTable = "table_version=1\ncolumns=id,name,email\n" +
                           "1,John,john@example.com\n" +
                           "2,Jane,jane@example.com\n";
        Files.writeString(databaseDir.resolve("users.table"), usersTable);
        
String ordersTable = "table_version=1\ncolumns=id,user_id,amount\n" +
                            "1,1,100.50\n" +
                            "2,1,200.75\n" +
                            "3,2,150.25\n";
        Files.writeString(databaseDir.resolve("orders.table"), ordersTable);
        
        // Create WAL file
        String walFile = "WAL_v2\n" +
                         "LSN: 1000\n" +
                         "Transaction: 1\n" +
                         "Operation: INSERT\n" +
                         "Data: users.table,1,John,john@example.com\n";
        Files.writeString(databaseDir.resolve("transactions.log"), walFile);
        
        // Create some other files that should be ignored
        Files.writeString(databaseDir.resolve("config.properties"), "database=test");
        Files.createFile(databaseDir.resolve("temp.tmp"));
    }
}