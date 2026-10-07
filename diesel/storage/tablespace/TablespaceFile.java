package diesel.storage.tablespace;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import diesel.storage.page.PageManager;

/**
 * Represents a single file in a tablespace containing page data.
 * Each file has a maximum size and manages page allocation within it.
 */
public class TablespaceFile {
    private final Path filePath;
    private final long maxFileSizeBytes;
    private final FileChannel channel;
    private final RandomAccessFile randomAccessFile;
    
    /**
     * Creates a new tablespace file.
     * 
     * @param filePath the path to the file
     * @param maxFileSizeBytes maximum file size in bytes
     * @throws IOException if the file cannot be created or opened
     */
    public TablespaceFile(Path filePath, long maxFileSizeBytes) throws IOException {
        this.filePath = filePath;
        this.maxFileSizeBytes = maxFileSizeBytes;
        
        // Create parent directory if it doesn't exist
        Path parent = filePath.getParent();
        if (parent != null) {
            parent.toFile().mkdirs();
        }
        
        // Open the file with read/write access, create if it doesn't exist
        this.randomAccessFile = new RandomAccessFile(filePath.toFile(), "rw");
        this.channel = randomAccessFile.getChannel();
    }
    
    /**
     * Writes a page to this file.
     * 
     * @param pageNumber the page number (0-based)
     * @param pageData the page data to write
     * @throws IOException if the write fails or file is full
     */
    public synchronized void writePage(int pageNumber, ByteBuffer pageData) throws IOException {
        long position = (long) pageNumber * PageManager.PAGE_SIZE;
        
        if (position + PageManager.PAGE_SIZE > maxFileSizeBytes) {
            throw new IOException("File size limit exceeded: " + maxFileSizeBytes + " bytes");
        }
        
        // Position the channel and write the page
        channel.position(position);
        while (pageData.hasRemaining()) {
            channel.write(pageData);
        }
    }
    
    /**
     * Reads a page from this file.
     * 
     * @param pageNumber the page number (0-based)
     * @param pageData the buffer to read into
     * @throws IOException if the read fails
     */
    public synchronized void readPage(int pageNumber, ByteBuffer pageData) throws IOException {
        long position = (long) pageNumber * PageManager.PAGE_SIZE;
        
        channel.position(position);
        while (pageData.hasRemaining()) {
            channel.read(pageData);
        }
    }
    
    /**
     * Gets the current size of the file in bytes.
     * 
     * @return file size in bytes
     */
    public long getSize() throws IOException {
        return randomAccessFile.length();
    }
    
    /**
     * Gets the maximum size of this file in bytes.
     * 
     * @return maximum file size in bytes
     */
    public long getMaxSize() {
        return maxFileSizeBytes;
    }
    
    /**
     * Checks if this file is full.
     * 
     * @return true if the file is full, false otherwise
     */
    public boolean isFull() throws IOException {
        return getSize() >= maxFileSizeBytes;
    }
    
    /**
     * Gets the file path.
     * 
     * @return the file path
     */
    public Path getFilePath() {
        return filePath;
    }
    
    /**
     * Closes this file and releases resources.
     * 
     * @throws IOException if an I/O error occurs
     */
    public synchronized void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
        if (randomAccessFile != null) {
            randomAccessFile.close();
        }
    }
    
    /**
     * Flushes any pending writes to disk.
     * 
     * @throws IOException if an I/O error occurs
     */
    public synchronized void flush() throws IOException {
        if (channel != null) {
            channel.force(true);
        }
    }
}