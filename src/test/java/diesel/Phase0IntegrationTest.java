package diesel;

import diesel.Database;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Phase0IntegrationTest {

    private Path tempDir;
    private Database database;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("dieseldb_phase0_");
        database = new Database();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            try (Stream<Path> walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }
    }

    @Test
    void environmentIsInitialized() throws IOException {
        assertTrue(Files.isDirectory(tempDir));
        assertTrue(Files.isWritable(tempDir));
        assertInstanceOf(Database.class, database);
    }

    @Test
    void engineExecutesQueriesAgainstFreshDatabase() {
        database.executeQuery("CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER)", null);
        database.executeQuery("INSERT INTO USERS (NAME, AGE) VALUES ('John', 30)", null);
        Object result = database.executeQuery("SELECT ID, NAME, AGE FROM USERS WHERE NAME = 'John'", null);
        assertInstanceOf(List.class, result);
        assertEquals(1, ((List<?>) result).size());
    }
}
