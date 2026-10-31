import diesel.checksum.ChecksummedPage;
import diesel.storage.page.Page;
import diesel.storage.page.PageId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;

@Tag("storage")
public class SimpleChecksumTest {
    
    @Test
    void testChecksumVerification() {
        // Create a page
        PageId pageId = new PageId(1, 1, 0);
        Page page = new Page(pageId, 8192);
        
        // Insert test data
        byte[] testData = "Test data for checksum verification".getBytes();
        page.insert(testData);
        
        // Update checksum
        ChecksummedPage.updateChecksum(page);
        
        // Verify checksum should pass
        assertTrue(ChecksummedPage.verifyChecksum(page), "Checksum verification should pass for clean page");
        
        // Corrupt the data
        byte[] data = page.toBytes();
        data[100] ^= 0x01; // Flip a bit
        
        // Create corrupted page
        Page corruptedPage = new Page(pageId, 8192);
        corruptedPage.setData(data);
        
        // Debug output
        System.out.println("Original checksum: " + page.getChecksum());
        System.out.println("Corrupted checksum: " + corruptedPage.getChecksum());
        System.out.println("Verify corrupted: " + ChecksummedPage.verifyChecksum(corruptedPage));
        
        // Verify checksum should fail
        assertFalse(ChecksummedPage.verifyChecksum(corruptedPage), "Checksum verification should fail for corrupted page");
    }
}