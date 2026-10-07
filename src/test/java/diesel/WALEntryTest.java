package diesel;

import diesel.wal.InvalidCRCException;
import diesel.wal.WALEntry;
import diesel.wal.WALFormatException;
import diesel.wal.WALFormat;
import diesel.wal.WALOpcode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WALEntry/WALFormat/WALOpcode binary format
 * (prompt4.md step 11, R3-003 step 1/5).
 *
 * <p>All operations are in-memory; no file I/O in this layer.
 */
@Tag("storage")
@Tag("smoke")
class WALEntryTest {

    /** Helper: deterministic byte array for image payloads. */
    private byte[] uniqueBytes(int size, int seed) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) ((seed + i) % 256);
        }
        return bytes;
    }

    private WALEntry roundTrip(WALEntry entry) {
        ByteBuffer buffer = ByteBuffer.allocate(entry.encodedSize());
        entry.writeTo(buffer);
        assertEquals(0, buffer.remaining(), "writeTo should consume exactly encodedSize bytes");
        buffer.rewind();
        return WALEntry.readFrom(buffer);
    }

    // ---------------------------------------------------------------
    // Round-trip: opcodes and images
    // ---------------------------------------------------------------

    @Test
    void roundTripOfAllOpcodes() {
        for (WALOpcode op : WALOpcode.values()) {
            WALEntry entry = new WALEntry(42L, 7L, op);
            WALEntry decoded = roundTrip(entry);
            assertEquals(42L, decoded.getLsn(), op + ": LSN should survive");
            assertEquals(7L, decoded.getTxid(), op + ": txid should survive");
            assertEquals(op, decoded.getOp(), op + ": opcode should survive");
            assertEquals(0, decoded.getBeforeImage().length, op + ": no before-image");
            assertEquals(0, decoded.getAfterImage().length, op + ": no after-image");
        }
    }

    @Test
    void roundTripWithoutImages() {
        WALEntry decoded = roundTrip(new WALEntry(1L, 1L, WALOpcode.COMMIT));
        assertFalse(decoded.hasBeforeImage());
        assertFalse(decoded.hasAfterImage());
        assertEquals(WALFormat.MIN_ENTRY_SIZE, decoded.encodedSize());
    }

    @Test
    void roundTripBeforeImageOnly() {
        byte[] before = uniqueBytes(17, 3);
        WALEntry decoded = roundTrip(new WALEntry(10L, 20L, WALOpcode.DELETE, before, null));
        assertTrue(decoded.hasBeforeImage());
        assertFalse(decoded.hasAfterImage());
        assertArrayEquals(before, decoded.getBeforeImage());
        assertEquals(0, decoded.getAfterImage().length);
    }

    @Test
    void roundTripAfterImageOnly() {
        byte[] after = uniqueBytes(31, 5);
        WALEntry decoded = roundTrip(new WALEntry(11L, 21L, WALOpcode.INSERT, null, after));
        assertFalse(decoded.hasBeforeImage());
        assertTrue(decoded.hasAfterImage());
        assertArrayEquals(after, decoded.getAfterImage());
        assertEquals(0, decoded.getBeforeImage().length);
    }

    @Test
    void roundTripOfBothImages() {
        byte[] before = uniqueBytes(64, 1);
        byte[] after = uniqueBytes(64, 100);
        WALEntry decoded = roundTrip(new WALEntry(99L, 55L, WALOpcode.UPDATE, before, after));
        assertEquals(99L, decoded.getLsn());
        assertEquals(55L, decoded.getTxid());
        assertEquals(WALOpcode.UPDATE, decoded.getOp());
        assertArrayEquals(before, decoded.getBeforeImage());
        assertArrayEquals(after, decoded.getAfterImage());
    }

    @Test
    void roundTripOfLargeImages() {
        byte[] before = uniqueBytes(10_000, 7);
        byte[] after = uniqueBytes(10_000, 8);
        WALEntry decoded = roundTrip(new WALEntry(1L, 2L, WALOpcode.TRUNCATE, before, after));
        assertArrayEquals(before, decoded.getBeforeImage());
        assertArrayEquals(after, decoded.getAfterImage());
    }

    @Test
    void nullImagesAreNormalizedToEmpty() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.CHECKPOINT, null, null);
        assertNotNull(entry.getBeforeImage());
        assertNotNull(entry.getAfterImage());
        assertEquals(0, entry.getBeforeImage().length);
        assertEquals(0, entry.getAfterImage().length);
    }

    @Test
    void imageArraysAreDefensivelyCopied() {
        byte[] source = uniqueBytes(16, 42);
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.INSERT, source, source);
        source[0] = (byte) ~source[0];
        byte[] exposed = entry.getBeforeImage();
        exposed[1] = (byte) ~exposed[1];
        byte[] fresh = entry.getBeforeImage();
        assertEquals((byte) (42 % 256), fresh[0], "Constructor copy should isolate from source mutation");
        assertEquals(43, fresh[1] & 0xFF, "Getter copy should isolate from caller mutation");
    }

    // ---------------------------------------------------------------
    // Alignment / size
    // ---------------------------------------------------------------

    @Test
    void encodedSizeIsAlwaysEightByteAligned() {
        for (int before = 0; before <= 17; before++) {
            for (int after = 0; after <= 17; after++) {
                WALEntry entry = new WALEntry(1L, 1L, WALOpcode.UPDATE,
                        uniqueBytes(before, 0), uniqueBytes(after, 0));
                int size = entry.encodedSize();
                assertEquals(0, size % WALFormat.ALIGNMENT,
                        "before=" + before + " after=" + after + ": size " + size + " not aligned");
                assertTrue(size >= WALFormat.MIN_ENTRY_SIZE, "Size below minimum: " + size);
            }
        }
    }

    @Test
    void encodedSizeMatchesBytesActuallyWritten() {
        WALEntry entry = new WALEntry(5L, 5L, WALOpcode.UPDATE,
                uniqueBytes(13, 1), uniqueBytes(29, 2));
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        entry.writeTo(buffer);
        assertEquals(entry.encodedSize(), buffer.position(), "Position must advance by encodedSize");
        assertEquals(WALFormat.padding(WALFormat.rawEntrySize(13, 29)),
                buffer.position() - WALFormat.rawEntrySize(13, 29),
                "Trailing bytes must be exactly the padding");
    }

    @Test
    void paddingBytesAreZero() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.INSERT, uniqueBytes(5, 9), null);
        ByteBuffer buffer = ByteBuffer.allocate(entry.encodedSize());
        entry.writeTo(buffer);
        byte[] raw = buffer.array();
        for (int i = WALFormat.rawEntrySize(5, 0); i < raw.length; i++) {
            assertEquals(0, raw[i], "Padding byte at index " + i + " must be zero");
        }
    }

    // ---------------------------------------------------------------
    // CRC32C
    // ---------------------------------------------------------------

    @Test
    void storedCrcMatchesDocumentedScope() {
        byte[] before = uniqueBytes(40, 11);
        byte[] after = uniqueBytes(24, 12);
        WALEntry entry = new WALEntry(7L, 8L, WALOpcode.UPDATE, before, after);
        byte[] raw = entry.toBytes();

        // CRC scope: header (28 bytes) + before-image + after-image, per doc/wal/format.md
        CRC32C crc = new CRC32C();
        crc.update(raw, 0, WALFormat.ENTRY_FIXED_HEADER_SIZE + before.length + after.length);
        int expected = (int) crc.getValue();

        int crcOffset = WALFormat.ENTRY_FIXED_HEADER_SIZE + before.length + after.length;
        int stored = ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN).getInt(crcOffset);
        assertEquals(expected, stored, "Stored CRC32C must cover header + both images");
    }

    @Test
    void corruptByteInBeforeImageThrowsInvalidCrc() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.UPDATE,
                uniqueBytes(64, 1), uniqueBytes(64, 2));
        byte[] raw = entry.toBytes();
        raw[WALFormat.ENTRY_FIXED_HEADER_SIZE + 10] ^= 0x01;   // flip a payload bit

        InvalidCRCException ex = assertThrows(InvalidCRCException.class,
                () -> WALEntry.fromBytes(raw));
        assertTrue(ex.getMessage().contains("CRC32C"), "Message should mention CRC: " + ex.getMessage());
    }

    @Test
    void corruptByteInHeaderThrowsInvalidCrc() {
        WALEntry entry = new WALEntry(0x0102030405060708L, 1L, WALOpcode.INSERT);
        byte[] raw = entry.toBytes();
        raw[WALFormat.OFFSET_LSN + 3] ^= 0x40;                // flip a bit inside the LSN

        assertThrows(InvalidCRCException.class, () -> WALEntry.fromBytes(raw));
    }

    @Test
    void corruptCrcFieldItselfThrowsInvalidCrc() {
        WALEntry entry = new WALEntry(3L, 3L, WALOpcode.COMMIT);
        byte[] raw = entry.toBytes();
        raw[WALFormat.MIN_ENTRY_SIZE - 1] ^= (byte) 0xFF;      // last byte of the CRC field

        assertThrows(InvalidCRCException.class, () -> WALEntry.fromBytes(raw));
    }

    @Test
    void corruptByteInAfterImageThrowsInvalidCrc() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.INSERT, null, uniqueBytes(50, 6));
        byte[] raw = entry.toBytes();
        raw[WALFormat.ENTRY_FIXED_HEADER_SIZE + 5] ^= 0x08;

        assertThrows(InvalidCRCException.class, () -> WALEntry.fromBytes(raw));
    }

    @Test
    void invalidCrcExceptionReportsExpectedAndActual() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.ABORT);
        byte[] raw = entry.toBytes();
        raw[0] ^= 0x01;

        InvalidCRCException ex = assertThrows(InvalidCRCException.class,
                () -> WALEntry.fromBytes(raw));
        assertTrue(ex.getMessage().contains("expected=0x"), "Should report expected CRC: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("actual=0x"), "Should report actual CRC: " + ex.getMessage());
    }

    // ---------------------------------------------------------------
    // Structural corruption
    // ---------------------------------------------------------------

    @Test
    void truncatedHeaderThrowsFormatException() {
        ByteBuffer buffer = ByteBuffer.allocate(WALFormat.ENTRY_FIXED_HEADER_SIZE - 1);
        assertThrows(WALFormatException.class, () -> WALEntry.readFrom(buffer));
    }

    @Test
    void truncatedImagesThrowFormatException() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.UPDATE,
                uniqueBytes(100, 1), uniqueBytes(100, 2));
        byte[] raw = entry.toBytes();
        ByteBuffer buffer = ByteBuffer.wrap(raw, 0, WALFormat.ENTRY_FIXED_HEADER_SIZE + 50);

        assertThrows(WALFormatException.class, () -> WALEntry.readFrom(buffer));
    }

    @Test
    void missingCrcBytesThrowFormatException() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.COMMIT);
        ByteBuffer buffer = ByteBuffer.wrap(entry.toBytes(), 0, WALFormat.ENTRY_FIXED_HEADER_SIZE);

        assertThrows(WALFormatException.class, () -> WALEntry.readFrom(buffer));
    }

    @Test
    void missingPaddingThrowsFormatException() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.INSERT, uniqueBytes(5, 1), null);
        int rawSize = WALFormat.rawEntrySize(5, 0);            // 37 -> 40 aligned, pad = 3
        ByteBuffer buffer = ByteBuffer.wrap(entry.toBytes(), 0, rawSize);

        WALFormatException ex = assertThrows(WALFormatException.class,
                () -> WALEntry.readFrom(buffer));
        assertTrue(ex.getMessage().contains("padding"), "Should mention padding: " + ex.getMessage());
    }

    @Test
    void unknownOpcodeByteThrowsFormatException() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.COMMIT);
        byte[] raw = entry.toBytes();
        raw[WALFormat.OFFSET_OP] = (byte) 0x63;                // 99 = not a known opcode

        WALFormatException ex = assertThrows(WALFormatException.class,
                () -> WALEntry.fromBytes(raw));
        assertTrue(ex.getMessage().contains("opcode"), "Should mention opcode: " + ex.getMessage());
    }

    @Test
    void nonZeroFlagsThrowFormatException() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.COMMIT);
        byte[] raw = entry.toBytes();
        raw[WALFormat.OFFSET_FLAGS] = 0x01;

        WALFormatException ex = assertThrows(WALFormatException.class,
                () -> WALEntry.fromBytes(raw));
        assertTrue(ex.getMessage().contains("flags"), "Should mention flags: " + ex.getMessage());
    }

    @Test
    void nonZeroReservedFieldThrowsFormatException() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.COMMIT);
        byte[] raw = entry.toBytes();
        raw[WALFormat.OFFSET_RESERVED + 1] = 0x01;

        WALFormatException ex = assertThrows(WALFormatException.class,
                () -> WALEntry.fromBytes(raw));
        assertTrue(ex.getMessage().contains("reserved"), "Should mention reserved: " + ex.getMessage());
    }

    @Test
    void negativeImageLengthThrowsFormatException() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.UPDATE,
                uniqueBytes(8, 1), uniqueBytes(8, 2));
        byte[] raw = entry.toBytes();
        ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN).putInt(WALFormat.OFFSET_BEFORE_LEN, -1);

        WALFormatException ex = assertThrows(WALFormatException.class,
                () -> WALEntry.fromBytes(raw));
        assertTrue(ex.getMessage().contains("length"), "Should mention length: " + ex.getMessage());
    }

    @Test
    void imageLengthBeyondBufferThrowsFormatException() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.UPDATE,
                uniqueBytes(8, 1), uniqueBytes(8, 2));
        byte[] raw = entry.toBytes();
        ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN).putInt(WALFormat.OFFSET_AFTER_LEN, 1_000_000);

        assertThrows(WALFormatException.class, () -> WALEntry.fromBytes(raw));
    }

    // ---------------------------------------------------------------
    // Multi-entry streams, byte helpers, value semantics
    // ---------------------------------------------------------------

    @Test
    void multipleEntriesReadWriteSequentially() {
        WALEntry first = new WALEntry(1L, 10L, WALOpcode.INSERT, null, uniqueBytes(33, 1));
        WALEntry second = new WALEntry(2L, 10L, WALOpcode.COMMIT);
        WALEntry third = new WALEntry(3L, 11L, WALOpcode.UPDATE,
                uniqueBytes(9, 2), uniqueBytes(7, 3));

        ByteBuffer buffer = ByteBuffer.allocate(
                first.encodedSize() + second.encodedSize() + third.encodedSize());
        first.writeTo(buffer);
        second.writeTo(buffer);
        third.writeTo(buffer);
        assertEquals(0, buffer.remaining(), "Buffer should be exactly filled");
        buffer.rewind();

        assertEquals(first, WALEntry.readFrom(buffer), "Entry 1");
        assertEquals(second, WALEntry.readFrom(buffer), "Entry 2");
        assertEquals(third, WALEntry.readFrom(buffer), "Entry 3");
        assertEquals(0, buffer.remaining(), "All entries consumed");
    }

    @Test
    void toBytesAndFromBytesRoundTrip() {
        WALEntry entry = new WALEntry(123L, 456L, WALOpcode.TRUNCATE,
                uniqueBytes(20, 4), uniqueBytes(40, 5));
        byte[] raw = entry.toBytes();
        assertEquals(entry.encodedSize(), raw.length, "toBytes length must equal encodedSize");
        assertEquals(entry, WALEntry.fromBytes(raw));
    }

    @Test
    void writeToRejectsTooSmallBuffer() {
        WALEntry entry = new WALEntry(1L, 1L, WALOpcode.UPDATE,
                uniqueBytes(100, 1), null);
        ByteBuffer buffer = ByteBuffer.allocate(entry.encodedSize() - 1);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> entry.writeTo(buffer));
        assertTrue(ex.getMessage().contains("needed"), "Should report needed size: " + ex.getMessage());
    }

    @Test
    void equalsAndHashCodeAreConsistent() {
        WALEntry a = new WALEntry(5L, 6L, WALOpcode.UPDATE, uniqueBytes(10, 1), uniqueBytes(10, 2));
        WALEntry b = new WALEntry(5L, 6L, WALOpcode.UPDATE, uniqueBytes(10, 1), uniqueBytes(10, 2));
        WALEntry differentLsn = new WALEntry(6L, 6L, WALOpcode.UPDATE, uniqueBytes(10, 1), uniqueBytes(10, 2));
        WALEntry differentOp = new WALEntry(5L, 6L, WALOpcode.DELETE, uniqueBytes(10, 1), uniqueBytes(10, 2));

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, differentLsn);
        assertNotEquals(a, differentOp);
    }

    // ---------------------------------------------------------------
    // WALOpcode / WALFormat units
    // ---------------------------------------------------------------

    @Test
    void opcodeWireCodesAreStable() {
        assertEquals(0, WALOpcode.BEGIN.code());
        assertEquals(1, WALOpcode.INSERT.code());
        assertEquals(2, WALOpcode.UPDATE.code());
        assertEquals(3, WALOpcode.DELETE.code());
        assertEquals(4, WALOpcode.COMMIT.code());
        assertEquals(5, WALOpcode.ABORT.code());
        assertEquals(6, WALOpcode.TRUNCATE.code());
        assertEquals(7, WALOpcode.CHECKPOINT.code());
    }

    @Test
    void opcodeFromByteRoundTripsAllValues() {
        for (WALOpcode op : WALOpcode.values()) {
            assertEquals(op, WALOpcode.fromByte(op.code()));
        }
    }

    @Test
    void opcodeFromByteRejectsUnknownValues() {
        assertThrows(IllegalArgumentException.class, () -> WALOpcode.fromByte((byte) 8));
        assertThrows(IllegalArgumentException.class, () -> WALOpcode.fromByte((byte) -1));
        assertThrows(IllegalArgumentException.class, () -> WALOpcode.fromByte((byte) 99));
    }

    @Test
    void formatHelpersComputeDocumentedSizes() {
        assertEquals(28, WALFormat.ENTRY_FIXED_HEADER_SIZE);
        assertEquals(4, WALFormat.CRC_SIZE);
        assertEquals(32, WALFormat.MIN_ENTRY_SIZE);
        assertEquals(32, WALFormat.rawEntrySize(0, 0));
        assertEquals(32, WALFormat.entrySize(0, 0));
        assertEquals(33, WALFormat.rawEntrySize(1, 0));
        assertEquals(40, WALFormat.entrySize(1, 0), "33 bytes must pad to 40");
        assertEquals(7, WALFormat.padding(33));
        assertEquals(0, WALFormat.padding(40));
        assertEquals(40, WALFormat.alignedSize(34));
        assertThrows(IllegalArgumentException.class, () -> WALFormat.rawEntrySize(-1, 0));
        assertThrows(IllegalArgumentException.class, () -> WALFormat.alignedSize(-1));
    }

    @Test
    void toStringMentionsKeyFields() {
        WALEntry entry = new WALEntry(42L, 7L, WALOpcode.INSERT, new byte[3], new byte[5]);
        String text = entry.toString();
        assertTrue(text.contains("lsn=42"), text);
        assertTrue(text.contains("txid=7"), text);
        assertTrue(text.contains("INSERT"), text);
        assertTrue(Arrays.asList(WALOpcode.values()).stream()
                .anyMatch(op -> text.contains(op.name())), "Should contain an opcode name");
    }
}
