package diesel.storage.tablespace;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import diesel.storage.page.PageManager;

/**
 * Represents a tablespace as a directory containing page files.
 * Each tablespace manages multiple files (file-001.dat, file-002.dat, etc.)
 * and routes page requests to the appropriate file based on page number.
 */
public class Tablespace {
    private final Path directoryPath;
    private final long maxFileSizeBytes;
    private final List<TablespaceFile> files;
    private final AtomicInteger nextFileId;
    
    /**
     * Creates a new tablespace.
     * 
     * @param directoryPath the directory path for the tablespace
     * @param maxFileSizeBytes maximum file size in bytes
     */
    public Tablespace(Path directoryPath, long maxFileSizeBytes) {
        this.directoryPath = directoryPath;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.files = new ArrayList<>();
        this.nextFileId = new AtomicInteger(1);
        
        // Create the directory if it doesn't exist
        if (!Files.exists(directoryPath)) {
            try {
                Files.createDirectories(directoryPath);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create tablespace directory: " + directoryPath, e);
            }
        }
    }
    
    /**
     * Writes a page to the tablespace.
     * 
     * @param pageNumber the page number (0-based)
     * @param pageData the page data to write
     * @throws IOException if the write fails
     */
    public synchronized void writePage(int pageNumber, ByteBuffer pageData) throws IOException {
        int fileId = getFileIdForPage(pageNumber);
        TablespaceFile file = getFile(fileId);
        
        // Calculate page offset within the file
        int offsetInFile = pageNumber % (int) (maxFileSizeBytes / PageManager.PAGE_SIZE);
        file.writePage(offsetInFile, pageData);
    }
    
    /**
     * Reads a page from the tablespace.
     * 
     * @param pageNumber the page number (0-based)
     * @param pageData the buffer to read into
     * @throws IOException if the read fails
     */
    public synchronized void readPage(int pageNumber, ByteBuffer pageData) throws IOException {
        int fileId = getFileIdForPage(pageNumber);
        TablespaceFile file = getFile(fileId);
        
        // Calculate page offset within the file
        int offsetInFile = pageNumber % (int) (maxFileSizeBytes / PageManager.PAGE_SIZE);
        file.readPage(offsetInFile, pageData);
    }
    
    /**
     * Gets the file ID for a given page number.
     * 
     * @param pageNumber the page number
     * @return the file ID
     */
    private int getFileIdForPage(int pageNumber) {
        // Each file can store this many pages
        int pagesPerFile = (int) (maxFileSizeBytes / PageManager.PAGE_SIZE);
        return pageNumber / pagesPerFile;
    }
    
    /**
     * Gets or creates a file for the given file ID.
     * 
     * @param fileId the file ID
     * @return the TablespaceFile
     * @throws IOException if the file cannot be created
     */
    private synchronized TablespaceFile getFile(int fileId) throws IOException {
        // Ensure we have enough files
        while (files.size() <= fileId) {
            createNextFile();
        }
        
        return files.get(fileId);
    }
    
    /**
     * Creates the next file in the tablespace.
     * 
     * @throws IOException if the file cannot be created
     */
    private synchronized void createNextFile() throws IOException {
        int fileId = nextFileId.getAndIncrement();
        String fileName = String.format("file-%03d.dat", fileId);
        Path filePath = directoryPath.resolve(fileName);
        
        TablespaceFile file = new TablespaceFile(filePath, maxFileSizeBytes);
        files.add(file);
    }
    
    /**
     * Gets the directory path of this tablespace.
     * 
     * @return the directory path
     */
    public Path getDirectoryPath() {
        return directoryPath;
    }
    
    /**
     * Gets the number of files in this tablespace.
     * 
     * @return the number of files
     */
    public int getFileCount() {
        return files.size();
    }
    
    /**
     * Gets the total size of all files in this tablespace.
     * 
     * @return total size in bytes
     * @throws IOException if an I/O error occurs
     */
    public long getTotalSize() throws IOException {
        long total = 0;
        for (TablespaceFile file : files) {
            total += file.getSize();
        }
        return total;
    }
    
    /**
     * Flushes all files in the tablespace.
     * 
     * @throws IOException if an I/O error occurs
     */
    public synchronized void flush() throws IOException {
        for (TablespaceFile file : files) {
            file.flush();
        }
    }
    
    /**
     * Closes all files in the tablespace.
     * 
     * @throws IOException if an I/O error occurs
     */
    public synchronized void close() throws IOException {
        for (TablespaceFile file : files) {
            file.close();
        }
    }
}