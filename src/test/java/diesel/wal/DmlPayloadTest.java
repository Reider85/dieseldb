package diesel.wal;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Codec tests for the logical DML WAL payload (prompt 4 #19).
 */
@Tag("smoke")
@Tag("storage")
public class DmlPayloadTest {

    private static Map<String, Object> sampleRow() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", 42L);
        row.put("NAME", "Alice");
        row.put("SCORE", 9.75);
        row.put("ACTIVE", true);
        row.put("RATIO", new BigDecimal("1.25"));
        row.put("COUNT", 7);
        row.put("NOTE", null);
        return row;
    }

    @Test
    void roundTripPreservesTableIndexAndValues() throws Exception {
        Map<String, Object> row = sampleRow();
        byte[] encoded = DmlPayload.serialize("USERS", 17, row);
        DmlPayload decoded = DmlPayload.deserialize(encoded);

        assertEquals("USERS", decoded.getTableName());
        assertEquals(17, decoded.getRowIndex());
        assertEquals(row, decoded.getValues());
    }

    @Test
    void roundTripEmptyValues() throws Exception {
        byte[] encoded = DmlPayload.serialize("T", 0, Map.of());
        DmlPayload decoded = DmlPayload.deserialize(encoded);

        assertEquals("T", decoded.getTableName());
        assertEquals(0, decoded.getRowIndex());
        assertTrue(decoded.getValues().isEmpty());
    }

    @Test
    void nullValuesMapBecomesEmpty() throws Exception {
        byte[] encoded = DmlPayload.serialize("T", 3, null);
        DmlPayload decoded = DmlPayload.deserialize(encoded);
        assertTrue(decoded.getValues().isEmpty());
    }

    @Test
    void serializableFallbackTypeRoundTrips() throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("CUSTOM", new java.util.ArrayList<>(java.util.List.of("a", "b")));
        byte[] encoded = DmlPayload.serialize("T", 1, row);
        DmlPayload decoded = DmlPayload.deserialize(encoded);
        assertEquals(java.util.List.of("a", "b"), decoded.getValues().get("CUSTOM"));
    }

    @Test
    void truncatedPayloadThrowsIllegalArgument() {
        byte[] encoded;
        try {
            encoded = DmlPayload.serialize("USERS", 5, sampleRow());
        } catch (Exception e) {
            fail("serialize failed: " + e);
            return;
        }
        byte[] truncated = java.util.Arrays.copyOf(encoded, encoded.length / 2);
        assertThrows(IllegalArgumentException.class, () -> DmlPayload.deserialize(truncated));
    }

    @Test
    void emptyPayloadThrowsIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> DmlPayload.deserialize(new byte[0]));
    }

    @Test
    void malformedTypeTagThrowsIllegalArgument() throws Exception {
        // Hand-build a payload with an unknown type tag.
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
        dos.writeUTF("T");
        dos.writeInt(0);
        dos.writeInt(1);
        dos.writeUTF("COL");
        dos.writeByte(99); // unknown tag
        dos.flush();
        assertThrows(IllegalArgumentException.class,
                () -> DmlPayload.deserialize(baos.toByteArray()));
    }

    @Test
    void toStringContainsTableAndIndex() throws Exception {
        DmlPayload payload = DmlPayload.deserialize(DmlPayload.serialize("USERS", 9, sampleRow()));
        assertTrue(payload.toString().contains("USERS"));
        assertTrue(payload.toString().contains("9"));
    }
}
