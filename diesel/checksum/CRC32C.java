package diesel.checksum;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;

/**
 * Prompt4 #51 — CRC32C implementation (hardware-accelerated) — R3-014 step 1/3.
 * 
 * High-performance Castagnoli CRC32C (CRC-32C, Castagnoli polynomial 0x82F63B78)
 * utility wrapping JDK's {@link java.util.zip.CRC32C} for hardware acceleration,
 * with a pure-Java table-based fallback for verification and testing.
 * 
 * <p>The hardware path delegates to {@code java.util.zip.CRC32C}, which HotSpot
 * intrinsifies to SSE4.2 CRC32 instructions on supported CPUs (Intel/AMD since 2008).
 * On unsupported CPUs, the JDK silently falls back to a pure-Java implementation.
 * 
 * <p>The pure-Java fallback ({@code computeSoftware}) is a self-contained table-based
 * implementation of the Castagnoli polynomial. It is byte-wise with 256-byte lookup table
 * for correctness verification and when explicit software mode is needed.
 * 
 * <p>Thread-safe: all methods are static and stateless (except {@code newChecksum} which
 * returns a thread-safe {@code Checksum} instance).
 */
public final class CRC32C {

    private CRC32C() {
        // Utility class, no instances
    }

    /**
     * Computes CRC32C over the entire byte array.
     * Uses JDK's hardware-accelerated {@code java.util.zip.CRC32C} when available.
     * 
     * @param data the input data
     * @return CRC32C checksum as int (32-bit, unsigned in practice)
     * @throws NullPointerException if data is null
     */
    public static int compute(byte[] data) {
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(data);
        return (int) crc.getValue();
    }

    /**
     * Computes CRC32C over a portion of the byte array.
     * Uses JDK's hardware-accelerated {@code java.util.zip.CRC32C} when available.
     * 
     * @param data the input data
     * @param offset the starting offset in the array
     * @param length the number of bytes to process
     * @return CRC32C checksum as int (32-bit, unsigned in practice)
     * @throws NullPointerException if data is null
     * @throws IndexOutOfBoundsException if offset or length are invalid
     */
    public static int compute(byte[] data, int offset, int length) {
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    /**
     * Creates a new hardware-accelerated CRC32C checksum instance for incremental updates.
     * 
     * @return a thread-safe {@code Checksum} instance that computes CRC32C
     */
    public static Checksum newChecksum() {
        return new java.util.zip.CRC32C();
    }

    /**
     * Computes CRC32C using a pure-Java table-based implementation.
     * Castagnoli polynomial (0x82F63B78), byte-wise algorithm with 256-byte lookup table.
     * Used for verification and when explicit software mode is required.
     * 
     * @param data the input data
     * @return CRC32C checksum as int (32-bit, unsigned in practice)
     * @throws NullPointerException if data is null
     */
    public static int computeSoftware(byte[] data) {
        return computeSoftware(data, 0, data.length);
    }

    /**
     * Computes CRC32C using a pure-Java table-based implementation over a portion of the array.
     * Castagnoli polynomial (0x82F63B78), byte-wise algorithm with 256-byte lookup table.
     * 
     * @param data the input data
     * @param offset the starting offset in the array
     * @param length the number of bytes to process
     * @return CRC32C checksum as int (32-bit, unsigned in practice)
     * @throws NullPointerException if data is null
     * @throws IndexOutOfBoundsException if offset or length are invalid
     */
    public static int computeSoftware(byte[] data, int offset, int length) {
        if (data == null) {
            throw new NullPointerException("data cannot be null");
        }
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IndexOutOfBoundsException("Invalid offset/length");
        }
        if (length == 0) {
            return 0x00000000; // RFC 3720: CRC32C("") = 0x00000000
        }

        // Castagnoli polynomial (reversed): 0x82F63B78
        final int POLY = 0x82F63B78;
        final int[] TABLE = makeTable(POLY);

        int crc = 0xFFFFFFFF; // Initial value (XOR with final 0xFFFFFFFF)
        
        for (int i = offset; i < offset + length; i++) {
            crc = (crc >>> 8) ^ TABLE[(crc ^ data[i]) & 0xFF];
        }
        
        return crc ^ 0xFFFFFFFF; // Final XOR
    }

    /**
     * Creates a 256-byte lookup table for the given polynomial.
     * Each entry TABLE[i] = CRC for byte i with the polynomial.
     */
    private static int[] makeTable(int poly) {
        int[] table = new int[256];
        for (int i = 0; i < 256; i++) {
            int crc = i;
            for (int j = 0; j < 8; j++) {
                crc = (crc >>> 1) ^ ((crc & 1) != 0 ? poly : 0);
            }
            table[i] = crc;
        }
        return table;
    }

    /**
     * Checks if software mode is forced via system property.
     * Returns true if {@code diesel.checksum.software=true} is set.
     * 
     * @return true if software mode is forced, false otherwise
     */
    public static boolean isSoftwareForced() {
        return Boolean.getBoolean("diesel.checksum.software");
    }
}