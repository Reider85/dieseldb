package diesel.storage.page;

/**
 * Page identifier for page-based storage (R3-002).
 * A page address consists of three components: tablespace, file, and page number.
 * 
 * <p>Immutable, thread-safe, value semantics via record.
 * Suitable for use as BufferPool keys (hash/equals/Comparable).
 */
public record PageId(long tablespaceId, long fileId, long pageNum) {

    /**
     * Computes the file offset for this page, given the page size.
     * Useful for direct addressing in PageManager (prompt 8).
     * 
     * @param pageSize the size of each page in bytes (must be > 0)
     * @return offset = pageNum * pageSize
     * @throws IllegalArgumentException if pageSize <= 0
     */
    public long fileOffset(int pageSize) {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("Page size must be positive: " + pageSize);
        }
        return pageNum * (long) pageSize;
    }

    /**
     * Returns a compact string representation for logging/debugging.
     * Format: "ts:tablespaceId/f:fileId/p:pageNum"
     */
    @Override
    public String toString() {
        return "ts:" + tablespaceId + "/f:" + fileId + "/p:" + pageNum;
    }
}