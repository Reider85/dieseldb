package diesel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("query")
class InsertQueryTest {

    @TempDir
    Path tempDir;

    private Database database;

    @BeforeEach
    void setUp() {
        database = new Database(tempDir.toString());
    }

    @Test
    void rejectsMismatchedColumnAndValueCounts() {
        Table table = createTable("MISMATCH", Map.of("ID", Integer.class), null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new InsertQuery(List.of("ID"), List.of(1, 2)).execute(table));

        assertEquals("Column and value counts mismatch", exception.getMessage());
    }

    @Test
    void rejectsUnknownColumn() {
        Table table = createTable("UNKNOWN", Map.of("ID", Integer.class), null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new InsertQuery(List.of("MISSING"), List.of(1)).execute(table));

        assertTrue(exception.getMessage().contains("MISSING"));
    }

    @Test
    void convertsValuesToColumnTypes() {
        Map<String, Class<?>> types = new LinkedHashMap<>();
        types.put("ID", Integer.class);
        types.put("LONG_VALUE", Long.class);
        types.put("SHORT_VALUE", Short.class);
        types.put("BYTE_VALUE", Byte.class);
        types.put("DECIMAL_VALUE", BigDecimal.class);
        types.put("FLOAT_VALUE", Float.class);
        types.put("DOUBLE_VALUE", Double.class);
        types.put("CHAR_VALUE", Character.class);
        types.put("UUID_VALUE", UUID.class);
        types.put("BOOLEAN_VALUE", Boolean.class);
        types.put("DATE_VALUE", LocalDate.class);
        types.put("DATETIME_VALUE", LocalDateTime.class);
        types.put("STRING_VALUE", String.class);
        Table table = createTable("CONVERSIONS", types, "ID");
        UUID uuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");

        new InsertQuery(
                List.copyOf(types.keySet()),
                List.of("7", "8", "9", "10", "12.50", "1.25", "2.5", "Q", uuid.toString(),
                        Boolean.TRUE, LocalDate.of(2026, 9, 25),
                        LocalDateTime.of(2026, 9, 25, 12, 30, 45), 123))
                .execute(table);

        Map<String, Object> row = table.getRows().getFirst();
        assertEquals(7, row.get("ID"));
        assertEquals(8L, row.get("LONG_VALUE"));
        assertEquals((short) 9, row.get("SHORT_VALUE"));
        assertEquals((byte) 10, row.get("BYTE_VALUE"));
        assertEquals(new BigDecimal("12.50"), row.get("DECIMAL_VALUE"));
        assertEquals(1.25F, row.get("FLOAT_VALUE"));
        assertEquals(2.5D, row.get("DOUBLE_VALUE"));
        assertEquals('Q', row.get("CHAR_VALUE"));
        assertEquals(uuid, row.get("UUID_VALUE"));
        assertEquals(Boolean.TRUE, row.get("BOOLEAN_VALUE"));
        assertEquals(LocalDate.of(2026, 9, 25), row.get("DATE_VALUE"));
        assertEquals(LocalDateTime.of(2026, 9, 25, 12, 30, 45), row.get("DATETIME_VALUE"));
        assertEquals("123", row.get("STRING_VALUE"));
        assertEquals(1, table.rowCount());
    }

    @Test
    void preservesNullValues() {
        Table table = createTable("NULLS", Map.of("ID", Integer.class, "NAME", String.class), null);

        new InsertQuery(List.of("ID", "NAME"), java.util.Arrays.asList(null, null)).execute(table);

        Map<String, Object> row = table.getRows().getFirst();
        assertEquals(1, table.rowCount());
        org.junit.jupiter.api.Assertions.assertNull(row.get("ID"));
        org.junit.jupiter.api.Assertions.assertNull(row.get("NAME"));
    }

    @Test
    void reportsInvalidCharacterWithColumnName() {
        Table table = createTable("CHARACTERS", Map.of("CODE", Character.class), null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new InsertQuery(List.of("CODE"), List.of("two")).execute(table));

        assertTrue(exception.getMessage().contains("CODE"));
        assertTrue(exception.getMessage().contains("CHARACTER"));
    }

    @Test
    void reportsInvalidNumericValue() {
        Table table = createTable("NUMBERS", Map.of("VALUE", Integer.class), null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new InsertQuery(List.of("VALUE"), List.of("not-a-number")).execute(table));

        assertTrue(exception.getMessage().contains("VALUE"));
        assertTrue(exception.getMessage().contains("INTEGER"));
    }

    @Test
    void rejectsDuplicatePrimaryKey() {
        Table table = createTable("DUPLICATES", Map.of("ID", Integer.class, "NAME", String.class), "ID");
        new InsertQuery(List.of("ID", "NAME"), List.of(1, "first")).execute(table);

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new InsertQuery(List.of("ID", "NAME"), List.of(1, "second")).execute(table));

        assertTrue(exception.getMessage().contains("Insert failed"));
        assertEquals(1, table.rowCount());
    }

    @Test
    void exposesInsertedRowCount() {
        Table table = createTable("AFFECTED", Map.of("ID", Integer.class), "ID");
        InsertQuery query = new InsertQuery(List.of("ID"), List.of(1));

        query.execute(table);

        assertEquals(1L, query.getLastAffectedRows());
    }

    private Table createTable(String name, Map<String, Class<?>> types, String primaryKey) {
        database.createTable(name, List.copyOf(types.keySet()), types, primaryKey);
        return database.getTable(name);
    }
}
