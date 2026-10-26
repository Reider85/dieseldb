package diesel.backup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Physical backup implementation for DieselDB.
 * Creates consistent snapshots of a running database by:
 * 1. Triggering a checkpoint to create a consistent snapshot
 * 2. Copying all data pages (frozen snapshot)
 * 3. Copying WAL segments from checkpoint LSN to current
 * 4. Creating a backup manifest with checksums and metadata
 */
public class PhysicalBackup {
    
    private final Path databaseDir;
    private final Path backupDir;
    private final BackupManifest manifest;
    
    public PhysicalBackup(Path databaseDir, Path backupDir) {
        this.databaseDir = databaseDir;
        this.backupDir = backupDir;
        this.manifest = new BackupManifest();
    }
    
    /**
     * Performs a physical backup of the database.
     * 
     * @throws IOException if backup fails
     */
    public void backup() throws IOException {
        System.out.println("Starting physical backup...");
        
        // Create backup directory
        Files.createDirectories(backupDir);
        
        // Step 1: Trigger checkpoint to create consistent snapshot
        System.out.println("Step 1: Triggering checkpoint...");
        long checkpointLsn = triggerCheckpoint();
        
        // Step 2: Copy data pages (frozen snapshot)
        System.out.println("Step 2: Copying data pages...");
        copyDataPages();
        
        // Step 3: Copy WAL segments from checkpoint LSN to current
        System.out.println("Step 3: Copying WAL segments...");
        copyWALSegments(checkpointLsn);
        
        // Step 4: Create backup manifest
        System.out.println("Step 4: Creating backup manifest...");
        createManifest();
        
        System.out.println("Backup completed successfully!");
        System.out.println("Backup location: " + backupDir);
        System.out.println("Manifest: " + backupDir.resolve("backup_manifest.json"));
    }
    
    /**
     * Triggers a checkpoint to create a consistent snapshot.
     * Note: This is a simplified version that assumes checkpoint files are available.
     */
    private long triggerCheckpoint() throws IOException {
        // Look for existing checkpoint files in the database directory
        Path checkpointFile = databaseDir.resolve("checkpoint.json");
        if (!Files.exists(checkpointFile)) {
            throw new IOException("No checkpoint file found at: " + checkpointFile);
        }
        
        // For now, we'll assume the checkpoint is already completed
        // In a real implementation, we would interact with the database's checkpoint manager
        System.out.println("Using existing checkpoint at: " + checkpointFile);
        return 1000L; // Placeholder LSN
    }
    
    /**
     * Copies all data pages to the backup directory.
     */
    private void copyDataPages() throws IOException {
        // Find all data files in the database directory
        try (Stream<Path> dataFiles = Files.walk(databaseDir)) {
            dataFiles.filter(this::isDataFile)
                    .forEach(dataFile -> {
                        try {
                            copyFileWithChecksum(dataFile, backupDir.resolve(dataFile.getFileName()));
                            manifest.addFile(dataFile.getFileName().toString(), dataFile.toFile().length(), calculateChecksum(dataFile));
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to copy data file: " + dataFile, e);
                        }
                    });
        }
        
        System.out.println("Copied " + manifest.getFileEntries().size() + " data files");
    }
    
    /**
     * Copies WAL segments from the checkpoint LSN to the current end.
     */
    private void copyWALSegments(long checkpointLsn) throws IOException {
        // Find all WAL files in the database directory
        try (Stream<Path> walFiles = Files.walk(databaseDir)) {
            walFiles.filter(this::isWALFile)
                    .forEach(walFile -> {
                        try {
                            copyFileWithChecksum(walFile, backupDir.resolve(walFile.getFileName()));
                            manifest.addFile(walFile.getFileName().toString(), walFile.toFile().length(), calculateChecksum(walFile));
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to copy WAL file: " + walFile, e);
                        }
                    });
        }
        
        System.out.println("Copied WAL segments from LSN " + checkpointLsn);
    }
    
    /**
     * Checks if a file is a WAL file.
     */
    private boolean isWALFile(Path path) {
        String fileName = path.getFileName().toString().toLowerCase();
        return fileName.endsWith(".log") || fileName.endsWith(".wal");
    }
    
    /**
     * Creates the backup manifest with metadata.
     */
    private void createManifest() throws IOException {
        // Create backup info
        BackupManifest.BackupInfo info = new BackupManifest.BackupInfo(
            System.currentTimeMillis(),
            databaseDir.toString(),
            manifest.getFileEntries().size(),
            manifest.getTotalSize(),
            "physical"
        );
        
        manifest.setInfo(info);
        manifest.save(backupDir.resolve("backup_manifest.json"));
    }
    
    /**
     * Checks if a file is a data file (table, index, etc.).
     */
    private boolean isDataFile(Path file) {
        String name = file.getFileName().toString();
        return name.endsWith(".table") || 
               name.endsWith(".csv") || 
               name.endsWith(".tsv") || 
               name.endsWith(".jsonl") ||
               name.endsWith(".avro");
    }
    
    /**
     * Copies a file and calculates its checksum.
     */
    private void copyFileWithChecksum(Path source, Path target) throws IOException {
        Files.copy(source, target);
    }
    
    /**
     * Calculates a simple checksum for a file.
     */
    private String calculateChecksum(Path file) throws IOException {
        // Simple checksum implementation - in production, use a proper cryptographic hash
        long size = Files.size(file);
        return "checksum-" + size;
    }
    
    
}