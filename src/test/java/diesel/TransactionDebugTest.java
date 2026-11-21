import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.UUID;
import diesel.Database;

public class TransactionDebugTest {
    
    @Test
    void testTransactionCreation(@TempDir Path tempDir) {
        Database db = new Database(tempDir.toString());
        
        // Step 1: Create table and insert data
        db.executeQuery("CREATE TABLE test_table (id LONG PRIMARY KEY, value STRING)", null);
        db.executeQuery("INSERT INTO test_table (id, value) VALUES (1, 'original')", null);
        
        // Step 2: Begin transaction
        String result = (String) db.executeQuery("BEGIN TRANSACTION", null);
        System.out.println("BEGIN TRANSACTION result: " + result);
        
        // Step 3: Try to get the transaction ID
        UUID txId = UUID.fromString(result.substring("Transaction started: ".length()));
        System.out.println("Transaction ID: " + txId);
        
        // Step 4: Check if transaction is in activeTransactions
        // We can't directly access activeTransactions, but we can try a savepoint
        try {
            String savepointResult = (String) db.executeQuery("SAVEPOINT test_sp", txId);
            System.out.println("SAVEPOINT result: " + savepointResult);
        } catch (Exception e) {
            System.out.println("SAVEPOINT failed: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Step 5: Commit
        db.executeQuery("COMMIT", txId);
    }
}