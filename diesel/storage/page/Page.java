package diesel.storage.page;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Page in the page-based storage layer (R3-002).
 * 
 * <p>Represents a fixed-size page with header + slotted payload.
 * Provides tuple insertion, deletion, defragmentation, and I/O operations.
 * 
 * <p>Binary layout: see PageHeader documentation for header format,
 * followed by slot directory (8 bytes per slot) and tuple data.
 * 
 * <p>Thread safety: Not thread-safe (each Page instance represents a single page in memory).
 * Concurrent access should be coordinated by BufferPool (prompt 7).
 */
public final class Page {

    // Constants
    public static final String PAGE_SIZE_KEY = PageConfig.PAGE_SIZE_KEY;
    private static final int[] ALLOWED_SIZES = {
        PageConfig.PAGE_SIZE_8K,
        PageConfig.PAGE_SIZE_16K,
        PageConfig.PAGE_SIZE_64K
    };

    // Fields
    private final PageId pageId;
    private final int pageSize;
    private byte[] data;
    private PageHeader header;
    private boolean dirty;

    /**
     * Creates a new empty page with the given identifier and size.
     * 
     * @param pageId the page identifier (tablespaceId/fileId/pageNum)
     * @param pageSize the page size in bytes (must be one of ALLOWED_SIZES)
     * @throws IllegalArgumentException if pageSize is invalid
     */
    public Page(PageId pageId, int pageSize) {
        if (!isAllowedSize(pageSize)) {
            throw new IllegalArgumentException("Invalid page size: " + pageSize + 
                    " (allowed: " + Arrays.toString(ALLOWED_SIZES) + ")");
        }
        
        this.pageId = pageId;
        this.pageSize = pageSize;
        this.data = new byte[pageSize];
        this.header = new PageHeader(pageId, pageSize);
        this.dirty = false;
        
        // Initialize with zeros (header will be written by first operation)
    }

    /**
     * Creates a new empty page with the default page size.
     * 
     * @param pageId the page identifier
     */
    public Page(PageId pageId) {
        this(pageId, PageConfig.getPageSize());
    }

    /**
     * Creates a page by reading from a ByteBuffer.
     * The buffer must be positioned at the page start and have remaining() == pageSize.
     * 
     * @param src the source buffer (must have remaining() == pageSize)
     * @return a new Page instance
     * @throws PageFormatException if the page format is invalid or corrupt
     */
    public static Page readFrom(ByteBuffer src) {
        if (src.remaining() < PageHeader.HEADER_SIZE) {
            throw new PageFormatException("Buffer too short for page header: " + src.remaining() + " < " + PageHeader.HEADER_SIZE);
        }
        
        // Read header first to get pageSize
        byte[] headerData = new byte[PageHeader.HEADER_SIZE];
        src.get(headerData);
        PageHeader header = PageHeader.readFrom(headerData);

        // Verify buffer has remaining data for the rest of the page (after header)
        int remainingAfterHeader = header.getPageSize() - PageHeader.HEADER_SIZE;
        if (src.remaining() < remainingAfterHeader) {
            throw new PageFormatException("Buffer too short for full page: " + (src.remaining() + PageHeader.HEADER_SIZE) + " < " + header.getPageSize());
        }

        // Read full page data
        byte[] data = new byte[header.getPageSize()];
        System.arraycopy(headerData, 0, data, 0, PageHeader.HEADER_SIZE);
        src.get(data, PageHeader.HEADER_SIZE, header.getPageSize() - PageHeader.HEADER_SIZE);
        
        // Create Page instance
        Page page = new Page(new PageId(header.getTablespaceId(), header.getFileId(), header.getPageNum()), header.getPageSize());
        page.data = data;
        page.header = header;
        
        return page;
    }

    /**
     * Writes this page to the destination buffer.
     * 
     * @param dst the destination buffer (must have remaining() >= pageSize)
     */
    public void writeTo(ByteBuffer dst) {
        if (dst.remaining() < pageSize) {
            throw new IllegalArgumentException("Destination buffer too short: " + dst.remaining() + " < " + pageSize);
        }
        
        // Sync header to data array before writing
        header.writeTo(data);
        dst.put(data);
    }

    /**
     * Inserts a tuple into the page.
     * 
     * @param tuple the tuple bytes to insert
     * @return the slotId where the tuple was inserted
     * @throws PageFullException if there is not enough free space
     */
    public int insert(byte[] tuple) {
        SlottedPageLayout.insertTuple(data, header, tuple);
        setDirty(true);
        return header.getSlotCount() - 1; // return the newly created slotId
    }

    /**
     * Reads a tuple from the given slot.
     * 
     * @param slotId the slot index (0-based)
     * @return tuple bytes, or null if slot is deleted/empty
     */
    public byte[] get(int slotId) {
        return SlottedPageLayout.readTuple(data, header, slotId);
    }

    /**
     * Marks a slot as deleted.
     * 
     * @param slotId the slot index (0-based)
     */
    public void delete(int slotId) {
        SlottedPageLayout.deleteTuple(data, header, slotId);
        setDirty(true);
    }

    /**
     * Defragments the page by compacting all live tuples.
     * 
     * @return the number of bytes reclaimed (always >= 0)
     */
    public int defrag() {
        int reclaimed = SlottedPageLayout.defragment(data, header);
        if (reclaimed > 0) {
            setDirty(true);
        }
        return reclaimed;
    }

    // Getters
    public PageId getPageId() { return pageId; }
    public int getPageSize() { return pageSize; }
    public int getSlotCount() { return header.getSlotCount(); }
    public int getFreeSpace() { return header.getFreeSpace(); }
    public long getLsn() { return header.getLsn(); }
    public int getChecksum() { return header.getChecksum(); }

    // Setters
    public void setLsn(long lsn) { 
        header.setLsn(lsn); 
        setDirty(true); 
    }
    public void setChecksum(int checksum) { 
        header.setChecksum(checksum); 
        setDirty(true); 
    }

    /**
     * Returns true if this page has been modified since last write.
     */
    public boolean isDirty() { return dirty; }

    /**
     * Marks the page as modified or clean.
     */
    public void setDirty(boolean dirty) { this.dirty = dirty; }

    /**
     * Returns a defensive copy of the full page data.
     * Useful for testing and serialization.
     */
    public byte[] toBytes() {
        return data.clone();
    }

    /**
     * Validates the page layout consistency.
     * 
     * @throws PageFormatException if inconsistencies are found
     */
    public void validate() {
        SlottedPageLayout.validateLayout(data, header);
    }

    /**
     * Returns true if the given size is one of the allowed page sizes.
     */
    public static boolean isAllowedSize(int size) {
        for (int allowed : ALLOWED_SIZES) {
            if (allowed == size) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("Page{ts=%d,f=%d,p=%d,size=%d,slots=%d,free=%s,dirty=%s}",
                pageId.tablespaceId(), pageId.fileId(), pageId.pageNum(), 
                pageSize, getSlotCount(), getFreeSpace(), dirty);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Page other = (Page) obj;
        return pageId.equals(other.pageId) && pageSize == other.pageSize && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        int result = pageId.hashCode();
        result = 31 * result + pageSize;
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}