package diesel;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * TESTS-9: guards the test-only bulk-load path. {@code Database.bulkLoadCSV}
 * must produce exactly the same rows as the SQL INSERT path it replaced in
 * {@link AbstractDieselTest#setupCommonTables()}, and the table sequences
 * must continue past the loaded rows so a follow-up INSERT does not collide
 * with the explicit IDs from the CSV.
 */
@Tag("smoke")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BulkLoadEquivalenceTest {

    private static final int N = 100;
    private static final Path FIXTURES = Path.of("src/test/resources/fixtures");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    // Must match BASE_DATE in scripts/generate-fixtures.py: the INSERT path
    // below regenerates rows with this base date so it lines up with the
    // frozen CSVs.
    private static final LocalDate BASE_DATE = LocalDate.of(2026, 1, 1);
    private static final Map<String, String> PRIMARY_KEYS = Map.of(
            "USERS", "ID",
            "PROFILES", "PROFILE_ID",
            "TRANSACTIONS", "TRANS_ID",
            "USER_DETAILS", "DETAIL_ID");

    @TempDir
    static Path tempDir;

    private Database insertPath;
    private Database bulkPath;

    @BeforeAll
    void buildBothPaths() throws IOException {
        insertPath = new Database();
        insertPath.setDataDir(tempDir.resolve("insert_path").toString());
        bulkPath = new Database();
        bulkPath.setDataDir(tempDir.resolve("bulk_path").toString());
        createSchema(insertPath);
        createSchema(bulkPath);
        for (int i = 1; i <= N; i++) {
            insertFixtureRow(insertPath, i);
        }
        bulkPath.bulkLoadCSV("USERS", FIXTURES.resolve("users.csv"));
        bulkPath.bulkLoadCSV("PROFILES", FIXTURES.resolve("profiles.csv"));
        bulkPath.bulkLoadCSV("TRANSACTIONS", FIXTURES.resolve("transactions.csv"));
        bulkPath.bulkLoadCSV("USER_DETAILS", FIXTURES.resolve("user_details.csv"));
    }

    @AfterAll
    void closeBoth() {
        if (insertPath != null) {
            insertPath.close();
        }
        if (bulkPath != null) {
            bulkPath.close();
        }
    }

    @Test
    void bulkLoadMatchesInsert() {
        for (Map.Entry<String, String> t : PRIMARY_KEYS.entrySet()) {
            assertSameRows(t.getKey(), t.getValue(), N, "initial load");
        }

        // Sequence continuation: the next INSERT past the loaded IDs (1..100)
        // must get 101 on BOTH paths -- bulkLoadCSV seeds the sequences. This
        // exercises all four sequence columns, not just USERS.ID.
        insertFixtureRow(insertPath, N + 1);
        insertFixtureRow(bulkPath, N + 1);
        for (Map.Entry<String, String> t : PRIMARY_KEYS.entrySet()) {
            assertSameRows(t.getKey(), t.getValue(), N + 1, "after follow-up INSERT");
        }
        assertEquals(101L, sortedRows(bulkPath, "USERS", "ID").get(N).get("ID"),
                "bulk-loaded table must continue its sequence at 101");
    }

    private void assertSameRows(String table, String pk, int expectedRows, String phase) {
        List<Map<String, Object>> viaInsert = sortedRows(insertPath, table, pk);
        List<Map<String, Object>> viaBulk = sortedRows(bulkPath, table, pk);
        assertEquals(expectedRows, viaInsert.size(), table + " INSERT-path row count (" + phase + ")");
        assertEquals(expectedRows, viaBulk.size(), table + " bulk-load row count (" + phase + ")");
        for (int i = 0; i < expectedRows; i++) {
            assertEquals(viaInsert.get(i), viaBulk.get(i),
                    table + " row " + i + " differs between INSERT and bulkLoadCSV (" + phase + ")");
        }
    }

    private static List<Map<String, Object>> sortedRows(Database db, String table, String pk) {
        List<Map<String, Object>> rows = new ArrayList<>(db.getTable(table).getRows());
        rows.sort(Comparator.comparing(r -> ((Number) r.get(pk)).longValue()));
        return rows;
    }

    private static void createSchema(Database db) {
        db.executeQuery("CREATE TABLE USERS (ID LONG PRIMARY KEY SEQUENCE(id_seq 1 1), USER_CODE STRING, NAME STRING, AGE INTEGER, BALANCE BIGDECIMAL, DATE_FIELD DATE, ACTIVE BOOLEAN, PRECISION DOUBLE)", null);
        db.executeQuery("CREATE UNIQUE INDEX ON USERS (ID)", null);
        db.executeQuery("CREATE INDEX ON USERS (AGE)", null);
        db.executeQuery("CREATE HASH INDEX ON USERS (NAME)", null);
        db.executeQuery("CREATE UNIQUE INDEX ON USERS (USER_CODE)", null);
        db.executeQuery("CREATE TABLE PROFILES (PROFILE_ID LONG PRIMARY KEY SEQUENCE(profile_seq 1 1), USER_ID LONG, PROFILE_AGE INTEGER, PROFILE_NAME STRING, PROFILE_CODE STRING, NON_INDEXED STRING, PROFILE_DATE DATE)", null);
        db.executeQuery("CREATE TABLE TRANSACTIONS (TRANS_ID LONG PRIMARY KEY SEQUENCE(trans_seq 1 1), USER_ID LONG, TRANS_DATE DATE, AMOUNT BIGDECIMAL)", null);
        db.executeQuery("CREATE TABLE USER_DETAILS (DETAIL_ID LONG PRIMARY KEY SEQUENCE(detail_seq 1 1), USER_ID LONG, USER_CODE STRING, NAME STRING, AGE INTEGER, INFO STRING, BALANCE BIGDECIMAL)", null);
    }

    /** Replicates the historical setupCommonTables() INSERT formulas with the fixture BASE_DATE. */
    private static void insertFixtureRow(Database db, int i) {
        String date = BASE_DATE.minusDays(i).format(DATE_FORMATTER);
        BigDecimal balance = new BigDecimal(100 + (i % 9000)).setScale(2, RoundingMode.HALF_UP);
        db.executeQuery(String.format(Locale.US,
                "INSERT INTO USERS (USER_CODE, NAME, AGE, BALANCE, DATE_FIELD, ACTIVE, PRECISION) VALUES ('CODE%d', 'User%d', %d, %s, '%s', %s, %f)",
                i, i, 18 + (i % 82), balance, date, (i % 2 == 0) ? "TRUE" : "FALSE", (i % 100) / 10.0), null);
        db.executeQuery(String.format(Locale.US,
                "INSERT INTO PROFILES (USER_ID, PROFILE_AGE, PROFILE_NAME, PROFILE_CODE, NON_INDEXED, PROFILE_DATE) VALUES (%d, %d, 'Profile%d', 'PCODE%d', 'Non%d', '%s')",
                i, 18 + (i % 82), i, i, i, date), null);
        db.executeQuery(String.format(Locale.US,
                "INSERT INTO TRANSACTIONS (USER_ID, TRANS_DATE, AMOUNT) VALUES (%d, '%s', %s)",
                i, date, new BigDecimal(50 + (i % 500)).setScale(2, RoundingMode.HALF_UP)), null);
        db.executeQuery(String.format(Locale.US,
                "INSERT INTO USER_DETAILS (USER_ID, USER_CODE, NAME, AGE, INFO, BALANCE) VALUES (%d, 'CODE%d', 'User%d', %d, 'Info%d', %s)",
                i, i, i, 18 + (i % 82), i, balance), null);
    }
}
