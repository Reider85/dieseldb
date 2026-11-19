package diesel.storage.page;

import java.io.IOException;
import java.nio.ByteBuffer;

public class O_DIRECT_Natives {
    private static boolean nativeLibraryLoaded = false;
    
    static {
        try {
            System.loadLibrary("diesel_odirect");
            nativeLibraryLoaded = true;
        } catch (UnsatisfiedLinkError e) {
            // O_DIRECT is not available on this platform (e.g., Windows)
            // Fall back to regular file I/O
            System.err.println("WARN: O_DIRECT native library not found, falling back to regular file I/O: " + e.getMessage());
        }
    }
    
    /**
     * Check if O_DIRECT is available on this platform.
     */
    public static boolean isO_DIRECTAvailable() {
        return nativeLibraryLoaded;
    }
    
    /**
     * Opens a file with O_DIRECT mode.
     * 
     * @param path file path to open
     * @param page_size page size in bytes (must be aligned to 512 bytes)
     * @return file descriptor (positive value) or error code (negative value)
     * @throws IOException if the file cannot be opened
     */
    public static native long openO_DIRECT(String path, int page_size) throws IOException;
    
    /**
     * Closes an O_DIRECT file descriptor.
     * 
     * @param fd file descriptor to close
     * @throws IOException if the file cannot be closed
     */
    public static native void closeO_DIRECT(long fd) throws IOException;
    
    /**
     * Reads data from an O_DIRECT file at the specified offset.
     * 
     * @param fd file descriptor
     * @param offset file offset to read from
     * @param buffer direct buffer to read into
     * @param position position within the buffer to start writing
     * @param length number of bytes to read (must be aligned to 512 bytes)
     * @return number of bytes actually read
     * @throws IOException if read fails or alignment is incorrect
     */
    public static native int readO_DIRECT(long fd, long offset, ByteBuffer buffer, int position, int length) throws IOException;
    
    /**
     * Writes data to an O_DIRECT file at the specified offset.
     * 
     * @param fd file descriptor
     * @param offset file offset to write to
     * @param buffer direct buffer to read from
     * @param position position within the buffer to start reading
     * @param length number of bytes to write (must be aligned to 512 bytes)
     * @return number of bytes actually written
     * @throws IOException if write fails or alignment is incorrect
     */
    public static native int writeO_DIRECT(long fd, long offset, ByteBuffer buffer, int position, int length) throws IOException;
    
    /**
     * Synchronizes file data to disk.
     * 
     * @param fd file descriptor to sync
     * @throws IOException if sync fails
     */
    public static native void syncO_DIRECT(long fd) throws IOException;
    
    /**
     * Sets the file size (truncates or extends with zeros).
     * 
     * @param fd file descriptor
     * @param size new file size in bytes
     * @throws IOException if operation fails
     */
    public static native void setSizeO_DIRECT(long fd, long size) throws IOException;
}