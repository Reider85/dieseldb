package diesel;

import diesel.storage.CsvRowStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 37: two parallel {@code saveToFile} calls must not corrupt the file.
 *
 * <p>Before the fix {@code Table.saveToFile} held only the table <em>read</em>
 * lock, so two threads could open and truncate the very same
 * {@code <target>.tmp} sibling simultaneously (the {@code AtomicFileWriter}
 * temp path is deterministic). The write lock serialises writers and the atomic
 * temp+rename pattern (Prompt 30) guarantees the target always ends up as one
 * complete, uncorrupted snapshot.
 */
class ConcurrentSaveTest {

    @TempDir
    Path tempDir;

    @Test
    void parallelSaveToFileDoesNotCorruptFile() throws Exception {
        String prev = System.getProperty("diesel.storage.type");
        try {
            System.setProperty("diesel.storage.type", "csv");
            Database db = new Database(tempDir.toString());
            db.executeQuery("CREATE TABLE CONCUR (ID LONG, NAME STRING, SCORE INTEGER)", null);
            for (int i = 0; i < 500; i++) {
                db.executeQuery("INSERT INTO CONCUR (ID, NAME, SCORE) VALUES ("
                        + i + ", 'Name" + i + "', " + (i * 10) + ")", null);
            }
            Table table = db.getTable("CONCUR");

            int workers = 8;
            CountDownLatch ready = new CountDownLatch(workers);
            CountDownLatch go = new CountDownLatch(1);
            List<Thread> threads = new ArrayList<>();
            List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
            for (int w = 0; w < workers; w++) {
                Thread t = new Thread(() -> {
                    ready.countDown();
                    try {
                        if (!go.await(10, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("starting gun never fired");
                        }
                        for (int round = 0; round < 30; round++) {
                            table.saveToFile("CONCUR");
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }, "save-worker-" + w);
                threads.add(t);
                t.start();
            }
            ready.await(10, TimeUnit.SECONDS);
            go.countDown();
            for (Thread t : threads) {
                t.join(60_000);
            }

            assertTrue(errors.isEmpty(), "no save worker may fail: " + errors);

            try (var files = Files.list(tempDir)) {
                long tmpFiles = files.filter(p -> p.getFileName().toString().endsWith(".tmp")).count();
                assertEquals(0, tmpFiles, "no orphaned temp files may remain after concurrent saves");
            }

            // Reload the CSV snapshot independently of the poisoned in-memory
            // state and verify the file is one complete, consistent version.
            CsvRowStorage loaded = new CsvRowStorage("CONCUR", List.of("ID", "NAME", "SCORE"), types());
            loaded.setDataDir(tempDir.toString());
            loaded.open();
            loaded.loadFromFile("CONCUR");
            List<Map<String, Object>> rows = loaded.scan();
            assertEquals(500, rows.size(), "every inserted row must survive");
            for (int i = 0; i < 500; i++) {
                assertEquals((long) i, rows.get(i).get("ID"));
                assertEquals("Name" + i, rows.get(i).get("NAME"));
                assertEquals(i * 10, rows.get(i).get("SCORE"));
            }
            loaded.close();
        } finally {
            if (prev == null) {
                System.clearProperty("diesel.storage.type");
            } else {
                System.setProperty("diesel.storage.type", prev);
            }
        }
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("SCORE", Integer.class);
        return t;
    }
}