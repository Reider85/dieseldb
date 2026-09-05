package diesel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetWriterTest {
    private static final Logger LOGGER = Logger.getLogger(ParquetWriterTest.class.getName());
    private static final String TABLE = "PARQUET_WRITER_TEST";
    /** Test data directory: Parquet files must be co-located with CSV files in {@code data/}. */
    private static final String DATA_DIR = "data";
    private static final byte[] PARQUET_MAGIC = "PAR1".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    @BeforeEach
    void setUp() {
        cleanup();
    }

    @AfterEach
    void tearDown() {
        cleanup();
    }

    private void cleanup() {
        new File(DATA_DIR, TABLE + ".parquet").delete();
        new File(DATA_DIR, TABLE + ".table").delete();
        new File(DATA_DIR, TABLE + ".csv").delete();
    }

    @Test
    void testWriteParquetFile() throws IOException {
        LOGGER.log(Level.INFO, "Starting test: testWriteParquetFile");
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING, AGE INTEGER, ACTIVE BOOLEAN, BIRTHDATE DATE, BALANCE BIGDECIMAL)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE, BIRTHDATE, BALANCE) VALUES ('Alice', 25, TRUE, '1998-05-20', 123.45)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME, AGE, ACTIVE, BIRTHDATE, BALANCE) VALUES ('Bob', 30, FALSE, '1993-08-15', 678.90)", null);

        Table table = db.getTable(TABLE);
        ParquetWriter.writeTableToParquet(table, TABLE);

        File file = new File(DATA_DIR, TABLE + ".parquet");
        assertTrue(file.exists(), "Parquet file created on disk");
        assertTrue(file.length() > 0, "Parquet file is not empty");

        // Verify the Parquet magic header bytes "PAR1" at the start and end of the file.
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] header = new byte[4];
            int read = fis.read(header);
            assertTrue(read == 4, "Read 4 header bytes");
            assertArrayEquals(PARQUET_MAGIC, header, "Parquet file starts with PAR1 magic header");
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(file.length() - 4);
            byte[] footer = new byte[4];
            raf.readFully(footer);
            assertArrayEquals(PARQUET_MAGIC, footer, "Parquet file ends with PAR1 magic footer");
        }

        assertTrue(table.isFileInitialized(), "Table marked as initialized after Parquet write");
        LOGGER.log(Level.INFO, "Test testWriteParquetFile: DONE");
    }

    @Test
    void testWriteSkipsTombstonedRows() {
        LOGGER.log(Level.INFO, "Starting test: testWriteSkipsTombstonedRows");
        Database db = new Database(DATA_DIR);
        db.executeQuery("CREATE TABLE " + TABLE + " (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), NAME STRING)", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Alice')", null);
        db.executeQuery("INSERT INTO " + TABLE + " (NAME) VALUES ('Bob')", null);

        Table table = db.getTable(TABLE);
        table.compact();
        ParquetWriter.writeTableToParquet(table, TABLE);

        assertTrue(new File(DATA_DIR, TABLE + ".parquet").exists(), "Parquet file created");
        LOGGER.log(Level.INFO, "Test testWriteSkipsTombstonedRows: DONE");
    }
}
