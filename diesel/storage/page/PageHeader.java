package diesel.storage.page;

import java.nio.ByteBuffer;

/**
 * Header for a page in the page-based storage layer (R3-002).
 * 
 * <p>Mutable, thread-safe when used within a single thread (typical Page context).
 * Contains metadata for page identification, versioning, and layout.
 * 
 * <p>Binary layout (64 bytes, big-endian):
 * <pre>
 * Offset  Size  Field
 * 0       4     magic = 0x44504745 ("DPGE")
 * 4       2     formatVersion = 1
 * 6       1     pageType: 0=NORMAL, 1=CATALOG (reserved for prompt 9)
 * 7       1     flags: reserved (bit0=dirty not persisted)
 * 8       8     tablespaceId
 * 16      8     fileId
 * 24      8     pageNum
 * 32      8     lsn (log sequence number, for prompt 11+)
 * 40      4     checksum (placeholder, for prompt 52)
 * 44      4     slotCount (number of slots in slot directory)
 * 48      4     freeSpaceStart ("free-space-pointer": end of slot directory)
 * 52      4     freeSpaceEnd (upper bound of tuple data)
 * 56      4     pageSize (self-describing for readFrom)
 * 60      4     reserved = 0
 * </pre>
 */
public final class PageHeader {

    // Layout constants
    public static final int HEADER_SIZE = 64;
    public static final int MAGIC = 0x44504745; // "DPGE" in little-endian bytes
    public static final short FORMAT_VERSION = 1;
    
    // Page types (expandable for prompt 9+)
    public static final byte PAGE_TYPE_NORMAL = 0;
    public static final byte PAGE_TYPE_CATALOG = 1; // reserved for prompt 9
    
    // Field offsets
    private static final int OFFSET_MAGIC = 0;
    private static final int OFFSET_FORMAT_VERSION = 4;
    private static final int OFFSET_PAGE_TYPE = 6;
    private static final int OFFSET_FLAGS = 7;
    private static final int OFFSET_TABLESPACE_ID = 8;
    private static final int OFFSET_FILE_ID = 16;
    private static final int OFFSET_PAGE_NUM = 24;
    private static final int OFFSET_LSN = 32;
    private static final int OFFSET_CHECKSUM = 40;
    private static final int OFFSET_SLOT_COUNT = 44;
    private static final int OFFSET_FREE_SPACE_START = 48;
    private static final int OFFSET_FREE_SPACE_END = 52;
    private static final int OFFSET_PAGE_SIZE = 56;
    private static final int OFFSET_RESERVED = 60;

    // Fields
    private final int magic;
    private final short formatVersion;
    private final byte pageType;
    private final byte flags;
    private final long tablespaceId;
    private final long fileId;
    private final long pageNum;
    private long lsn;
    private int checksum;
    private int slotCount;
    private int freeSpaceStart;
    private int freeSpaceEnd;
    private final int pageSize;

    /**
     * Creates a new page header with default values.
     * 
     * @param pageId the page identifier (tablespaceId/fileId/pageNum)
     * @param pageSize the size of the page in bytes (must be one of ALLOWED_SIZES)
     * @throws IllegalArgumentException if pageSize is invalid
     */
    public PageHeader(PageId pageId, int pageSize) {
        if (!Page.isAllowedSize(pageSize)) {
            throw new IllegalArgumentException("Invalid page size: " + pageSize);
        }
        
        this.magic = MAGIC;
        this.formatVersion = FORMAT_VERSION;
        this.pageType = PAGE_TYPE_NORMAL;
        this.flags = 0;
        this.tablespaceId = pageId.tablespaceId();
        this.fileId = pageId.fileId();
        this.pageNum = pageId.pageNum();
        this.lsn = 0;
        this.checksum = 0;
        this.slotCount = 0;
        this.freeSpaceStart = HEADER_SIZE;
        this.freeSpaceEnd = pageSize;
        this.pageSize = pageSize;
    }

    /**
     * Reads and validates a PageHeader from the beginning of the given byte array.
     * 
     * @param data byte array containing at least HEADER_SIZE bytes
     * @return a new PageHeader instance
     * @throws PageFormatException if the header is corrupt, invalid size, or bad magic/version
     */
    public static PageHeader readFrom(byte[] data) {
        if (data.length < HEADER_SIZE) {
            throw new PageFormatException("Page data too short for header: " + data.length + " < " + HEADER_SIZE);
        }
        
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.order(java.nio.ByteOrder.BIG_ENDIAN);
        
        int magic = bb.getInt();
        if (magic != MAGIC) {
            throw new PageFormatException("Invalid page magic: 0x" + Integer.toHexString(magic) + " (expected 0x" + Integer.toHexString(MAGIC) + ")");
        }
        
        short formatVersion = bb.getShort();
        if (formatVersion != FORMAT_VERSION) {
            throw new PageFormatException("Unsupported page format version: " + formatVersion + " (expected " + FORMAT_VERSION + ")");
        }
        
        byte pageType = bb.get();
        byte flags = bb.get();
        long tablespaceId = bb.getLong();
        long fileId = bb.getLong();
        long pageNum = bb.getLong();
        long lsn = bb.getLong();
        int checksum = bb.getInt();
        int slotCount = bb.getInt();
        int freeSpaceStart = bb.getInt();
        int freeSpaceEnd = bb.getInt();
        int pageSize = bb.getInt();
        int reserved = bb.getInt();
        
        if (reserved != 0) {
            throw new PageFormatException("Non-zero reserved field in header: " + reserved);
        }
        
        if (!Page.isAllowedSize(pageSize)) {
            throw new PageFormatException("Unsupported page size: " + pageSize);
        }
        
        // Validate header invariants
        if (slotCount < 0 || slotCount > (pageSize - HEADER_SIZE) / 8) {
            throw new PageFormatException("Invalid slot count: " + slotCount);
        }
        
        if (freeSpaceStart < HEADER_SIZE || freeSpaceStart > pageSize) {
            throw new PageFormatException("Invalid freeSpaceStart: " + freeSpaceStart);
        }
        
        if (freeSpaceEnd < freeSpaceStart || freeSpaceEnd > pageSize) {
            throw new PageFormatException("Invalid freeSpaceEnd: " + freeSpaceEnd);
        }
        
        PageId pageId = new PageId(tablespaceId, fileId, pageNum);
        PageHeader header = new PageHeader(pageId, pageSize);
        header.lsn = lsn;
        header.checksum = checksum;
        header.slotCount = slotCount;
        header.freeSpaceStart = freeSpaceStart;
        header.freeSpaceEnd = freeSpaceEnd;
        
        return header;
    }

    /**
     * Writes this header to the beginning of the given byte array.
     * 
     * @param data byte array of at least HEADER_SIZE bytes
     */
    public void writeTo(byte[] data) {
        if (data.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Data array too short for header: " + data.length);
        }
        
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.order(java.nio.ByteOrder.BIG_ENDIAN);
        
        bb.putInt(magic);
        bb.putShort(formatVersion);
        bb.put(pageType);
        bb.put(flags);
        bb.putLong(tablespaceId);
        bb.putLong(fileId);
        bb.putLong(pageNum);
        bb.putLong(lsn);
        bb.putInt(checksum);
        bb.putInt(slotCount);
        bb.putInt(freeSpaceStart);
        bb.putInt(freeSpaceEnd);
        bb.putInt(pageSize);
        bb.putInt(0); // reserved
    }

    // Getters
    public int getMagic() { return magic; }
    public short getFormatVersion() { return formatVersion; }
    public byte getPageType() { return pageType; }
    public byte getFlags() { return flags; }
    public long getTablespaceId() { return tablespaceId; }
    public long getFileId() { return fileId; }
    public long getPageNum() { return pageNum; }
    public long getLsn() { return lsn; }
    public int getChecksum() { return checksum; }
    public int getSlotCount() { return slotCount; }
    public int getFreeSpaceStart() { return freeSpaceStart; }
    public int getFreeSpaceEnd() { return freeSpaceEnd; }
    public int getPageSize() { return pageSize; }

    // Setters
    public void setLsn(long lsn) { this.lsn = lsn; }
    public void setChecksum(int checksum) { this.checksum = checksum; }
    public void setSlotCount(int slotCount) { this.slotCount = slotCount; }
    public void setFreeSpaceStart(int freeSpaceStart) { this.freeSpaceStart = freeSpaceStart; }
    public void setFreeSpaceEnd(int freeSpaceEnd) { this.freeSpaceEnd = freeSpaceEnd; }

    /**
     * Returns the current free space in bytes.
     */
    public int getFreeSpace() {
        return freeSpaceEnd - freeSpaceStart;
    }

    /**
     * Returns the total used space (header + slots + tuples).
     */
    public int getUsedSpace() {
        return pageSize - getFreeSpace();
    }

    /**
     * Returns the maximum number of slots that can fit in this page.
     */
    public int getMaxSlots() {
        return (pageSize - HEADER_SIZE) / 8;
    }

    /**
     * Returns true if this header represents a valid page.
     */
    public boolean isValid() {
        return magic == MAGIC && 
               formatVersion == FORMAT_VERSION &&
               slotCount >= 0 && slotCount <= getMaxSlots() &&
               freeSpaceStart >= HEADER_SIZE && freeSpaceStart <= pageSize &&
               freeSpaceEnd >= freeSpaceStart && freeSpaceEnd <= pageSize;
    }

    @Override
    public String toString() {
        return String.format("PageHeader{ts=%d,f=%d,p=%d,slots=%d,free=%d/%d,lsn=%d}",
                tablespaceId, fileId, pageNum, slotCount, getFreeSpace(), pageSize, lsn);
    }
}