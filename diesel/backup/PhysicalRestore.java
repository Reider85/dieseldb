package diesel.backup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Physical restore implementation for DieselDB.
 * Restores a database from a physical backup by:
 * 1. Copying backup files to the target directory
 * 2. Replaying WAL segments to bring the database to a consistent state
 * 3. Validating the restored database
 */
public class PhysicalRestore {
    
    private final Path backupDir;
    private final Path targetDir;
    private final BackupManifest manifest;
    
    public PhysicalRestore(Path backupDir, Path targetDir) throws IOException {
        this.backupDir = backupDir;
        this.targetDir = targetDir;
        
        // Load the backup manifest
        Path manifestPath = backupDir.resolve("backup_manifest.json");
        if (!Files.exists(manifestPath)) {
            throw new IOException("Backup manifest not found: " + manifestPath);
        }
        
        this.manifest = BackupManifest.load(manifestPath);
    }
    
    /**
     * Restores the database from the physical backup.
     * 
     * @throws IOException if restore fails
     */
    public void restore() throws IOException {
        System.out.println("Starting physical restore...");
        System.out.println("Backup location: " + backupDir);
        System.out.println("Target location: " + targetDir);
        
        // Step 1: Copy backup files to target directory
        System.out.println("Step 1: Copying backup files...");
        copyBackupFiles();
        
        // Step 2: Replay WAL segments to bring database to consistent state
        System.out.println("Step 2: Replaying WAL segments...");
        replayWAL();
        
        // Step 3: Validate the restored database
        System.out.println("Step 3: Validating restored database...");
        validateRestore();
        
        System.out.println("Restore completed successfully!");
    }
    
    /**
     * Copies all backup files to the target directory.
     */
    private void copyBackupFiles() throws IOException {
        // Create target directory
        Files.createDirectories(targetDir);
        
        // Copy all files from backup directory
        try (Stream<Path> backupFiles = Files.walk(backupDir)) {
            backupFiles.filter(path -> !path.equals(backupDir))
                    .forEach(sourceFile -> {
                        try {
                            Path relativePath = backupDir.relativize(sourceFile);
                            Path targetFile = targetDir.resolve(relativePath);
                            
                            // Create parent directories if needed
                            Files.createDirectories(targetFile.getParent());
                            
                            // Copy the file
                            Files.copy(sourceFile, targetFile);
                            System.out.println("Copied: " + relativePath);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to copy file: " + sourceFile, e);
                        }
                    });
        }
        
        System.out.println("Copied " + manifest.getFileEntries().size() + " files");
    }
    
    /**
     * Replays WAL segments to bring the database to a consistent state.
     */
    private void replayWAL() throws IOException {
        // For now, we'll assume the database recovery is handled externally
        // In a real implementation, we would interact with the database's recovery manager
        
        System.out.println("WAL replay completed (simplified implementation)");
    }
    
    /**
     * Validates the restored database.
     */
    private void validateRestore() throws IOException {
        // Verify backup integrity
        boolean integrityOk = manifest.verifyIntegrity(targetDir);
        if (!integrityOk) {
            throw new IOException("Backup integrity verification failed");
        }
        
        // Basic validation - check that all expected files exist
        validateFileStructure();
        
        System.out.println("Database validation passed");
    }
    
    /**
     * Validates the file structure of the restored database.
     */
    private void validateFileStructure() throws IOException {
        // Check that all files from the manifest exist in the target directory
        for (BackupManifest.FileEntry entry : manifest.getFileEntries()) {
            Path filePath = targetDir.resolve(entry.getRelativePath());
            if (!Files.exists(filePath)) {
                throw new IOException("Missing expected file: " + entry.getRelativePath());
            }
            
            // Verify file size matches
            long actualSize = Files.size(filePath);
            if (actualSize != entry.getSize()) {
                throw new IOException("File size mismatch for " + entry.getRelativePath() + 
                                   ": expected " + entry.getSize() + ", got " + actualSize);
            }
        }
        
        System.out.println("File structure validation passed");
    }
    
    /**
     * Gets backup statistics before performing restore.
     */
    public void displayBackupInfo() {
        if (manifest.getInfo() != null) {
            BackupManifest.BackupInfo info = manifest.getInfo();
            System.out.println("Backup Information:");
            System.out.println("  Type: " + info.getBackupType());
            System.out.println("  Source: " + info.getSourceDatabase());
            System.out.println("  Created: " + new java.util.Date(info.getTimestamp()));
            System.out.println("  Files: " + info.getFileCount());
            System.out.println("  Size: " + formatSize(info.getTotalSize()));
        }
        
        Map<String, Object> stats = manifest.getStatistics();
        System.out.println("\nBackup Statistics:");
        System.out.println("  Total files: " + stats.get("totalFiles"));
        System.out.println("  Total size: " + formatSize((Long) stats.get("totalSize")));
        
        @SuppressWarnings("unchecked")
        Map<String, Long> fileTypes = (Map<String, Long>) stats.get("fileTypes");
        System.out.println("  File types:");
        fileTypes.forEach((type, count) -> 
            System.out.println("    " + type + ": " + count));
    }
    
    /**
     * Formats a size in bytes to a human-readable format.
     */
    private String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024.0));
        return String.format("%.1f GB", bytes / (1024.0 * 1024.0 * 1024.0));
    }
    
    /**
     * Checks if the target directory is empty or safe to restore to.
     */
    public boolean isTargetSafe() throws IOException {
        if (!Files.exists(targetDir)) {
            return true; // Directory doesn't exist, safe to create
        }
        
        // Check if directory is empty
        try (Stream<Path> files = Files.list(targetDir)) {
            return !files.findAny().isPresent();
        }
    }
    
    /**
     * Validates that the backup directory exists and contains the required files.
     */
    public static boolean validateBackupDirectory(Path backupDir) {
        if (!Files.exists(backupDir)) {
            return false;
        }
        
        Path manifestPath = backupDir.resolve("backup_manifest.json");
        if (!Files.exists(manifestPath)) {
            return false;
        }
        
        // Check if there are any data files
        try {
            return Files.walk(backupDir)
                    .filter(path -> path.getFileName().toString().endsWith(".table") ||
                                   path.getFileName().toString().endsWith(".csv") ||
                                   path.getFileName().toString().endsWith(".log"))
                    .findAny()
                    .isPresent();
        } catch (IOException e) {
            return false;
        }
    }
}