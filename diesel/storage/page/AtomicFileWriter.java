package diesel.storage.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * Atomic file writer for page files: creates new files atomically using
 * temp + rename with retry logic for transient OS errors.
 * <p>
 * Pattern:
 * 1. Write to a temporary sibling file (e.g., "data.pages.tmp")
 * 2. Force write to disk
 * 3. Atomic move from temp to target (with Windows retry for transient AccessDenied)
 * <p>
 * Uses retry logic adapted from diesel.storage.AtomicFileWriter for consistency.
 */
public final class AtomicFileWriter {

    // Config keys (same as diesel.storage.AtomicFileWriter for consistency)
    public static final String MOVE_MAX_ATTEMPTS_KEY = "atomic.write.move.max.attempts";
    public static final String MOVE_RETRY_MAX_DELAY_KEY = "atomic.write.move.retry.max.delay.ms";
    public static final int MOVE_MAX_ATTEMPTS_DEFAULT = 10;
    public static final int MOVE_RETRY_MAX_DELAY_DEFAULT = 300;

    /**
     * Creates a new page file atomically by writing all pages to a temp file
     * and then moving it to the target path.
     *
     * @param target path to the final page file
     * @param pages collection of pages to write (written sequentially)
     * @throws IOException on write or move failure
     */
    public static void writeAllPages(Path target, Iterable<Page> pages) throws IOException {
        Path tmp = tmpPath(target);
        
        try {
            // Write all pages to temp file
            try (FileChannelIO io = new FileChannelIO(tmp, pages.iterator().next().getPageSize())) {
                long offset = 0;
                for (Page page : pages) {
                    ByteBuffer buffer = ByteBuffer.allocate(page.getPageSize());
                    page.writeTo(buffer);
                    buffer.flip();
                    io.writePage(new PageId(0, 0, offset / page.getPageSize()), buffer);
                    offset += page.getPageSize();
                }
                io.force(); // ensure durability before move
            }

            // Atomic move with retry
            replaceAtomically(tmp, target);
        } finally {
            // Clean up temp file if move failed
            if (Files.exists(tmp)) {
                Files.deleteIfExists(tmp);
            }
        }
    }

    /**
     * Creates a new file atomically by writing content and moving.
     *
     * @param target path to the final file
     * @param content file content to write
     * @throws IOException on write or move failure
     */
    public static void writeNewFileAtomically(Path target, byte[] content) throws IOException {
        Path tmp = tmpPath(target);
        
        try {
            // Write content to temp file
            Files.write(tmp, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            
            // Force durability before move
            try (FileChannelIO io = new FileChannelIO(tmp, 1)) {
                io.force();
            }

            // Atomic move with retry
            replaceAtomically(tmp, target);
        } finally {
            // Clean up temp file if move failed
            if (Files.exists(tmp)) {
                Files.deleteIfExists(tmp);
            }
        }
    }

    /**
     * Generates a temporary path for the target file.
     * Format: <target>.tmp
     *
     * @param target the final file path
     * @return temp file path
     */
    public static Path tmpPath(Path target) {
        return target.resolveSibling(target.getFileName() + ".tmp");
    }

    /**
     * Moves a temporary file to the target with retry logic for transient errors.
     * Uses ATOMIC_MOVE if available, falls back to REPLACE_EXISTING.
     * Retries transient AccessDeniedException on Windows.
     *
     * @param tmp temporary file path
     * @param target final file path
     * @throws IOException if move fails after retries
     */
    public static void replaceAtomically(Path tmp, Path target) throws IOException {
        int maxAttempts = getMaxAttempts();
        int maxDelay = getMaxDelayMs();
        int attempt = 0;
        IOException lastError = null;

        while (attempt <= maxAttempts) {
            attempt++;
            try {
                // Try atomic move first
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                return; // success
            } catch (IOException e) {
                lastError = e;

                // Only retry on transient AccessDenied (common on Windows)
                if (attempt <= maxAttempts && isTransientAccessDenied(e)) {
                    try {
                        long delay = (long) (25 * Math.pow(2, attempt - 1));
                        delay = Math.min(delay, maxDelay);
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted during move retry", ie);
                    }
                    continue;
                }

                // Non-retryable error or attempt limit reached
                if (attempt > maxAttempts) {
                    break;
                }
                throw e;
            }
        }

        // All attempts failed
        throw new IOException("failed to move " + tmp + " to " + target + " after " + attempt + " attempts", lastError);
    }

    /**
     * Warns if a target file is missing but its temp file exists,
     * indicating an interrupted write that left the temp file behind.
     *
     * @param target the intended final file path
     */
    public static void warnInterruptedWrite(Path target) {
        Path tmp = tmpPath(target);
        if (!Files.exists(target) && Files.exists(tmp)) {
            System.err.println("WARN: interrupted write detected - target missing but temp exists: " + tmp);
        }
    }

    // --- Private helpers ---

    private static int getMaxAttempts() {
        try {
            Properties props = PageConfig.loadRootProps();
            return Integer.parseInt(props.getProperty(MOVE_MAX_ATTEMPTS_KEY, String.valueOf(MOVE_MAX_ATTEMPTS_DEFAULT)));
        } catch (NumberFormatException e) {
            return MOVE_MAX_ATTEMPTS_DEFAULT;
        }
    }

    private static int getMaxDelayMs() {
        try {
            Properties props = PageConfig.loadRootProps();
            return Integer.parseInt(props.getProperty(MOVE_RETRY_MAX_DELAY_KEY, String.valueOf(MOVE_RETRY_MAX_DELAY_DEFAULT)));
        } catch (NumberFormatException e) {
            return MOVE_RETRY_MAX_DELAY_DEFAULT;
        }
    }

    private static boolean isTransientAccessDenied(IOException e) {
        // Windows transient AccessDenied during move (AV, file handles)
        return e instanceof java.nio.file.AccessDeniedException;
    }
}