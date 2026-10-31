package diesel.checksum;

import diesel.checksum.CRC32C;
import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import diesel.storage.page.PageCorruptedException;
import diesel.storage.page.PageHeader;
import diesel.observability.MetricsRegistry;

import java.nio.ByteBuffer;

/**
 * Checksummed page implementation with CRC32C verification (Prompt4 #52).
 * 
 * <p>Computes CRC32C over header + payload and stores it in the page header.
 * Provides verification on read and recovery capabilities.
 */
public final class ChecksummedPage {
    
    /**
     * Computes CRC32C checksum over the entire page (header + payload).
     * The checksum field in the header is zeroed before computation to avoid self-referential CRC.
     * 
     * @param page the page to compute checksum for
     * @return CRC32C checksum value
     */
    public static int computeChecksum(Page page) {
        byte[] data = page.toBytes();
        // Zero the checksum field (offset 40-43) so CRC is independent of the field
        data[40] = data[41] = data[42] = data[43] = 0;
        return CRC32C.compute(data, 0, data.length);
    }
    
    /**
     * Computes CRC32C checksum over a specific range of the page.
     * 
     * @param page the page to compute checksum for
     * @param offset starting offset in bytes
     * @param length number of bytes to include
     * @return CRC32C checksum value
     */
    public static int computeChecksum(Page page, int offset, int length) {
        byte[] data = page.toBytes();
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException("Invalid range: offset=" + offset + 
                    ", length=" + length + ", data.length=" + data.length);
        }
        return CRC32C.compute(data, offset, length);
    }
    
    /**
     * Verifies that the stored checksum matches the computed checksum.
     * 
     * @param page the page to verify
     * @return true if checksum is valid, false if corrupted
     */
    public static boolean verifyChecksum(Page page) {
        int stored = page.getChecksum();
        if (stored == 0) {
            // No checksum stored (page not yet written or zero-initialized)
            // Check if page has data - if it does, this is corruption
            byte[] data = page.toBytes();
            // Check if page has any non-zero data beyond header
            for (int i = PageHeader.HEADER_SIZE; i < data.length; i++) {
                if (data[i] != 0) {
                    return false; // Page has data but no checksum - corrupted
                }
            }
            return true; // Empty page is valid
        }
        
        int computed = computeChecksum(page);
        return stored == computed;
    }
    
    /**
     * Updates the checksum field in the page header.
     * First syncs header to data, then computes checksum with zeroed field, then stores checksum.
     * 
     * @param page the page to update
     */
    public static void updateChecksum(Page page) {
        // Sync header to data to ensure we're computing over current header state
        page.syncHeader();
        int checksum = computeChecksum(page);
        page.setChecksum(checksum);
    }
    
    /**
     * Creates a new page with pre-computed checksum.
     * 
     * @param pageId the page identifier
     * @param pageSize the page size
     * @param pageType the page type
     * @return new page with checksum computed and stored
     */
    public static Page createPageWithChecksum(PageId pageId, int pageSize, byte pageType) {
        Page page = new Page(pageId, pageSize, pageType);
        updateChecksum(page);
        return page;
    }
    
    /**
     * Validates page header and checksum in one operation.
     * 
     * @param page the page to validate
     * @throws PageCorruptedException if page is corrupted
     */
    public static void validatePage(Page page) throws PageCorruptedException {
        if (!verifyChecksum(page)) {
            // Increment checksum failure metric
            MetricsRegistry.getInstance().counter("diesel_page_checksum_failures_total")
                .increment();
            
            throw new PageCorruptedException(
                page.getPageId(), 
                page.getChecksum(), 
                computeChecksum(page),
                "Page " + page.getPageId() + " has corrupted checksum. " +
                "Stored: 0x" + Integer.toHexString(page.getChecksum()) + 
                ", Computed: 0x" + Integer.toHexString(computeChecksum(page))
            );
        }
    }
    
    /**
     * Recovers a corrupted page by attempting to restore from WAL redo.
     * 
     * @param page the corrupted page
     * @param walLsn the LSN to recover from (0 if no WAL available)
     * @return true if recovery succeeded, false if page is still corrupted
     * @throws PageCorruptedException if recovery fails
     */
    public static boolean recoverFromWal(Page page, long walLsn) throws PageCorruptedException {
        // This would integrate with WAL recovery logic (Prompt4 #53)
        // For now, we just mark that recovery would be attempted
        if (walLsn > 0) {
            // In a full implementation, this would:
            // 1. Find the WAL entry at the given LSN
            // 2. Apply the redo operation to restore the page
            // 3. Re-compute and verify the checksum
            // 4. Return true if checksum is now valid
            
            // Placeholder implementation - just return false for now
            // Real implementation depends on WAL integration (Prompt4 #53)
            return false;
        }
        return false;
    }
}