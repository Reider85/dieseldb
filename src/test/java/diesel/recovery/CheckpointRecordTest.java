package diesel.recovery;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

@Tag("storage")
@Tag("smoke")
class CheckpointRecordTest {
    @Test
    void testEmptyTxids() {
        CheckpointRecord record = new CheckpointRecord(100L, new long[0], 123456789L);
        assertEquals(100L, record.getLastLSN());
        assertEquals(0, record.getActiveTxidCount());
        assertArrayEquals(new long[0], record.getActiveTxids());
        assertEquals(123456789L, record.getTimestampEpochMs());
    }

    @Test
    void testSingleTxid() {
        long[] txids = {42L};
        CheckpointRecord record = new CheckpointRecord(200L, txids, 123456790L);
        assertEquals(200L, record.getLastLSN());
        assertEquals(1, record.getActiveTxidCount());
        assertArrayEquals(txids, record.getActiveTxids());
        assertEquals(123456790L, record.getTimestampEpochMs());
    }

    @Test
    void testMultipleTxids() {
        long[] txids = {10L, 20L, 30L};
        CheckpointRecord record = new CheckpointRecord(300L, txids, 123456791L);
        assertEquals(300L, record.getLastLSN());
        assertEquals(3, record.getActiveTxidCount());
        assertArrayEquals(txids, record.getActiveTxids());
        assertEquals(123456791L, record.getTimestampEpochMs());
    }

    @Test
    void testNullTxids() {
        assertThrows(IllegalArgumentException.class, () -> new CheckpointRecord(100L, null, 123456789L));
    }

    @Test
    void testTooManyTxids() {
        long[] txids = new long[CheckpointRecord.MAX_TXIDS + 1];
        assertThrows(IllegalArgumentException.class, () -> new CheckpointRecord(100L, txids, 123456789L));
    }

    @Test
    void testRoundTripEmpty() throws CheckpointFormatException {
        CheckpointRecord original = new CheckpointRecord(100L, new long[0], 123456789L);
        byte[] bytes = original.toBytes();
        CheckpointRecord decoded = CheckpointRecord.fromBytes(bytes);
        assertEquals(original, decoded);
    }

    @Test
    void testRoundTripSingle() throws CheckpointFormatException {
        CheckpointRecord original = new CheckpointRecord(200L, new long[]{42L}, 123456790L);
        byte[] bytes = original.toBytes();
        CheckpointRecord decoded = CheckpointRecord.fromBytes(bytes);
        assertEquals(original, decoded);
    }

    @Test
    void testRoundTripMultiple() throws CheckpointFormatException {
        CheckpointRecord original = new CheckpointRecord(300L, new long[]{10L, 20L, 30L}, 123456791L);
        byte[] bytes = original.toBytes();
        CheckpointRecord decoded = CheckpointRecord.fromBytes(bytes);
        assertEquals(original, decoded);
    }

    @Test
    void testWriteToReadFromEmpty() throws CheckpointFormatException {
        CheckpointRecord original = new CheckpointRecord(100L, new long[0], 123456789L);
        ByteBuffer buffer = ByteBuffer.allocate(original.encodedSize());
        original.writeTo(buffer);
        buffer.rewind();
        CheckpointRecord decoded = CheckpointRecord.readFrom(buffer);
        assertEquals(original, decoded);
    }

    @Test
    void testWriteToReadFromMultiple() throws CheckpointFormatException {
        CheckpointRecord original = new CheckpointRecord(300L, new long[]{10L, 20L, 30L}, 123456791L);
        ByteBuffer buffer = ByteBuffer.allocate(original.encodedSize());
        original.writeTo(buffer);
        buffer.rewind();
        CheckpointRecord decoded = CheckpointRecord.readFrom(buffer);
        assertEquals(original, decoded);
    }

    @Test
    void testBadMagic() {
        ByteBuffer buffer = ByteBuffer.allocate(36);
        buffer.putInt(0xFFFFFFFF); // bad magic
        buffer.putShort((short) 1);
        buffer.putShort((short) 0);
        buffer.putLong(100L);
        buffer.putLong(123456789L);
        buffer.putInt(0);
        buffer.putInt(0); // crc
        buffer.rewind();
        assertThrows(CheckpointFormatException.class, () -> CheckpointRecord.readFrom(buffer));
    }

    @Test
    void testBadVersion() {
        ByteBuffer buffer = ByteBuffer.allocate(36);
        buffer.putInt(CheckpointRecord.MAGIC);
        buffer.putShort((short) 999); // bad version
        buffer.putShort((short) 0);
        buffer.putLong(100L);
        buffer.putLong(123456789L);
        buffer.putInt(0);
        buffer.putInt(0); // crc
        buffer.rewind();
        assertThrows(CheckpointFormatException.class, () -> CheckpointRecord.readFrom(buffer));
    }

    @Test
    void testNegativeTxidCount() {
        ByteBuffer buffer = ByteBuffer.allocate(36);
        buffer.putInt(CheckpointRecord.MAGIC);
        buffer.putShort((short) 1);
        buffer.putShort((short) 0);
        buffer.putLong(100L);
        buffer.putLong(123456789L);
        buffer.putInt(-1); // bad count
        buffer.putInt(0); // crc
        buffer.rewind();
        assertThrows(CheckpointFormatException.class, () -> CheckpointRecord.readFrom(buffer));
    }

    @Test
    void testTooManyTxidsInBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(36);
        buffer.putInt(CheckpointRecord.MAGIC);
        buffer.putShort((short) 1);
        buffer.putShort((short) 0);
        buffer.putLong(100L);
        buffer.putLong(123456789L);
        buffer.putInt(CheckpointRecord.MAX_TXIDS + 1); // too many
        buffer.putInt(0); // crc
        buffer.rewind();
        assertThrows(CheckpointFormatException.class, () -> CheckpointRecord.readFrom(buffer));
    }

    @Test
    void testTruncatedBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(10); // too short
        buffer.putInt(CheckpointRecord.MAGIC);
        buffer.putShort((short) 1);
        buffer.rewind();
        assertThrows(BufferUnderflowException.class, () -> CheckpointRecord.readFrom(buffer));
    }

    @Test
    void testBadCRC() {
        ByteBuffer buffer = ByteBuffer.allocate(36);
        buffer.putInt(CheckpointRecord.MAGIC);
        buffer.putShort((short) 1);
        buffer.putShort((short) 0);
        buffer.putLong(100L);
        buffer.putLong(123456789L);
        buffer.putInt(0);
        buffer.putInt(0x12345678); // bad crc
        buffer.rewind();
        assertThrows(CheckpointFormatException.class, () -> CheckpointRecord.readFrom(buffer));
    }

    @Test
    void testEqualsAndHashCode() {
        CheckpointRecord r1 = new CheckpointRecord(100L, new long[]{10L, 20L}, 123456789L);
        CheckpointRecord r2 = new CheckpointRecord(100L, new long[]{10L, 20L}, 123456789L);
        CheckpointRecord r3 = new CheckpointRecord(200L, new long[]{10L, 20L}, 123456789L);
        CheckpointRecord r4 = new CheckpointRecord(100L, new long[]{30L, 40L}, 123456789L);
        CheckpointRecord r5 = new CheckpointRecord(100L, new long[]{10L, 20L}, 223456789L);

        assertEquals(r1, r2);
        assertEquals(r2, r1);
        assertNotEquals(r1, r3);
        assertNotEquals(r1, r4);
        assertNotEquals(r1, r5);
        assertNotEquals(r1, null);
        assertNotEquals(r1, "string");

        assertEquals(r1.hashCode(), r2.hashCode());
        assertNotEquals(r1.hashCode(), r3.hashCode());
        assertNotEquals(r1.hashCode(), r4.hashCode());
        assertNotEquals(r1.hashCode(), r5.hashCode());
    }

    @Test
    void testToString() {
        CheckpointRecord record = new CheckpointRecord(100L, new long[]{10L, 20L}, 123456789L);
        String s = record.toString();
        assertTrue(s.contains("lastLSN=100"));
        assertTrue(s.contains("activeTxids=[10, 20]"));
        assertTrue(s.contains("timestampEpochMs=123456789"));
    }
}