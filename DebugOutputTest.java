import diesel.storage.CsvRowStorage;
import diesel.Database;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class DebugOutputTest {
    
    @TempDir
    Path tempDir;
    
    private Database database;
    
    @BeforeEach
    void setUp() {
        database = new Database(tempDir.toString());
    }
    
    @AfterEach
    void tearDown() {
        if (database != null) {
            database.close();
        }
    }
    
    @Test
    void testCsvStorageSave() {
        // Создаем таблицу
        database.execute("CREATE TABLE test_table (ID LONG PRIMARY KEY, NAME STRING)");
        
        // Вставляем данные
        database.execute("INSERT INTO test_table (ID, NAME) VALUES (1, 'test')");
        
        // Читаем данные - это должно вызвать сохранение в CSV
        var result = database.execute("SELECT * FROM test_table");
        
        // Проверяем, что данные сохранились
        assertEquals(1, result.size());
        assertEquals(1L, result.get(0).get("ID"));
        assertEquals("test", result.get(0).get("NAME"));
    }
}