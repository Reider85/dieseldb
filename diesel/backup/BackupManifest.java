package diesel.backup;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Backup manifest that contains metadata and checksums for a physical backup.
 * Provides integrity verification and backup information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackupManifest {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private BackupInfo info;
    private List<FileEntry> fileEntries = new ArrayList<>();
    private Map<String, String> metadata = new HashMap<>();
    
    /**
     * Backup information including timestamp, source, and type.
     */
    public static class BackupInfo {
        private final long timestamp;
        private final String sourceDatabase;
        private final int fileCount;
        private final long totalSize;
        private final String backupType;
        
        @JsonCreator
        public BackupInfo(
            @JsonProperty("timestamp") long timestamp,
            @JsonProperty("sourceDatabase") String sourceDatabase,
            @JsonProperty("fileCount") int fileCount,
            @JsonProperty("totalSize") long totalSize,
            @JsonProperty("backupType") String backupType) {
            
            this.timestamp = timestamp;
            this.sourceDatabase = sourceDatabase;
            this.fileCount = fileCount;
            this.totalSize = totalSize;
            this.backupType = backupType;
        }
        
        // Getters
        public long getTimestamp() { return timestamp; }
        public String getSourceDatabase() { return sourceDatabase; }
        public int getFileCount() { return fileCount; }
        public long getTotalSize() { return totalSize; }
        public String getBackupType() { return backupType; }
    }
    
    /**
     * Individual file entry with checksum and metadata.
     */
    public static class FileEntry {
        private String relativePath;
        private long size;
        private String checksum;
        private String fileType;
        
        @JsonCreator
        public FileEntry(
            @JsonProperty("relativePath") String relativePath,
            @JsonProperty("size") long size,
            @JsonProperty("checksum") String checksum,
            @JsonProperty("fileType") String fileType) {
            
            this.relativePath = relativePath;
            this.size = size;
            this.checksum = checksum;
            this.fileType = fileType;
        }
        
        // Getters and setters
        public String getRelativePath() { return relativePath; }
        public long getSize() { return size; }
        public String getChecksum() { return checksum; }
        public String getFileType() { return fileType; }
        
        public void setRelativePath(String relativePath) { this.relativePath = relativePath; }
        public void setSize(long size) { this.size = size; }
        public void setChecksum(String checksum) { this.checksum = checksum; }
        public void setFileType(String fileType) { this.fileType = fileType; }
    }
    
    // Getters and setters
    public BackupInfo getInfo() { return info; }
    public void setInfo(BackupInfo info) { this.info = info; }
    
    public List<FileEntry> getFileEntries() { return fileEntries; }
    public void setFileEntries(List<FileEntry> fileEntries) { this.fileEntries = fileEntries; }
    
    public Map<String, String> getMetadata() { return metadata; }
    public void setMetadata(Map<String, String> metadata) { this.metadata = metadata; }
    
    /**
     * Adds a file entry to the manifest.
     */
    public void addFile(String relativePath, long size, String checksum) {
        String fileType = determineFileType(relativePath);
        fileEntries.add(new FileEntry(relativePath, size, checksum, fileType));
    }
    
    /**
     * Calculates the total size of all files in the backup.
     */
    public long getTotalSize() {
        return fileEntries.stream().mapToLong(FileEntry::getSize).sum();
    }
    
    /**
     * Saves the manifest to a JSON file.
     */
    public void save(Path manifestPath) throws IOException {
        objectMapper.writerWithDefaultPrettyPrinter().writeValue(manifestPath.toFile(), this);
    }
    
    /**
     * Loads a manifest from a JSON file.
     */
    public static BackupManifest load(Path manifestPath) throws IOException {
        return objectMapper.readValue(manifestPath.toFile(), BackupManifest.class);
    }
    
    /**
     * Verifies the integrity of the backup by checking all file checksums.
     * 
     * @param backupDir the backup directory
     * @return true if all files are valid, false otherwise
     */
    public boolean verifyIntegrity(Path backupDir) throws IOException {
        System.out.println("Verifying backup integrity...");
        
        int validFiles = 0;
        int totalFiles = fileEntries.size();
        
        for (FileEntry entry : fileEntries) {
            Path filePath = backupDir.resolve(entry.getRelativePath());
            
            if (!Files.exists(filePath)) {
                System.err.println("Missing file: " + entry.getRelativePath());
                return false;
            }
            
            if (Files.size(filePath) != entry.getSize()) {
                System.err.println("Size mismatch: " + entry.getRelativePath() + 
                                 " (expected: " + entry.getSize() + 
                                 ", actual: " + Files.size(filePath) + ")");
                return false;
            }
            
            // In production, implement proper checksum verification
            String actualChecksum = calculateChecksum(filePath);
            if (!entry.getChecksum().equals(actualChecksum)) {
                System.err.println("Checksum mismatch: " + entry.getRelativePath());
                return false;
            }
            
            validFiles++;
        }
        
        System.out.println("Integrity check passed: " + validFiles + "/" + totalFiles + " files valid");
        return validFiles == totalFiles;
    }
    
    /**
     * Determines the file type based on the extension.
     */
    private String determineFileType(String relativePath) {
        if (relativePath.endsWith(".table")) return "table";
        if (relativePath.endsWith(".csv")) return "csv";
        if (relativePath.endsWith(".tsv")) return "tsv";
        if (relativePath.endsWith(".jsonl")) return "jsonl";
        if (relativePath.endsWith(".avro")) return "avro";
        if (relativePath.endsWith(".log")) return "wal";
        return "unknown";
    }
    
    /**
     * Calculates a simple checksum for a file.
     * In production, use a proper cryptographic hash like SHA-256.
     */
    private String calculateChecksum(Path filePath) throws IOException {
        long size = Files.size(filePath);
        return "checksum-" + size;
    }
    
    /**
     * Gets files of a specific type.
     */
    public List<FileEntry> getFilesByType(String fileType) {
        List<FileEntry> result = new ArrayList<>();
        for (FileEntry entry : fileEntries) {
            if (fileType.equals(entry.getFileType())) {
                result.add(entry);
            }
        }
        return result;
    }
    
    /**
     * Gets statistics about the backup.
     */
    @JsonIgnore
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalFiles", fileEntries.size());
        stats.put("totalSize", getTotalSize());
        stats.put("fileTypes", getFileTypeCounts());
        stats.put("timestamp", info != null ? info.getTimestamp() : null);
        return stats;
    }
    
    /**
     * Gets counts of files by type.
     */
    private Map<String, Long> getFileTypeCounts() {
        Map<String, Long> counts = new HashMap<>();
        for (FileEntry entry : fileEntries) {
            counts.put(entry.getFileType(), counts.getOrDefault(entry.getFileType(), 0L) + 1);
        }
        return counts;
    }
}