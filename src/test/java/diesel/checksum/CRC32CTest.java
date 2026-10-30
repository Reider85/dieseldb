package diesel.checksum;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.zip.Checksum;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt4 #51 — CRC32C implementation (hardware-accelerated) — R3-014 step 1/3.
 * 
 * Acceptance criterion: CRC32CTest: known vectors (RFC 3720) — correct values.
 * Tests both hardware path (java.util.zip.CRC32C) and pure-Java software path
 * for correctness and equivalence.
 */
@Tag("smoke")
class CRC32CTest {

    // RFC 3720 appendix B.4 known vectors for CRC32C (Castagnoli)
    private static final int CRC32C_EMPTY = 0x00000000;
    private static final int CRC32C_A = 0xC1D04330;
    private static final int CRC32C_32_ZEROES = 0x8A9136AA;
    private static final int CRC32C_32_FF = 0x62A8AB43;
    private static final int CRC32C_00_TO_1F = 0x46DD794E;
    private static final int CRC32C_123456789 = 0xE3069283;

    @Test
    void knownVectorsHardwarePath() {
        // RFC 3720 appendix B.4 standard vectors
        assertEquals(CRC32C_EMPTY, CRC32C.compute(new byte[0]), "CRC32C(empty)");
        assertEquals(CRC32C_A, CRC32C.compute(new byte[]{0x61}), "CRC32C('a')");
        
        byte[] zeros = new byte[32];
        Arrays.fill(zeros, (byte) 0x00);
        assertEquals(CRC32C_32_ZEROES, CRC32C.compute(zeros), "CRC32C(32 × 0x00)");
        
        byte[] ff = new byte[32];
        Arrays.fill(ff, (byte) 0xFF);
        assertEquals(CRC32C_32_FF, CRC32C.compute(ff), "CRC32C(32 × 0xFF)");
        
        byte[] sequence = new byte[32];
        for (int i = 0; i < 32; i++) {
            sequence[i] = (byte) i;
        }
        assertEquals(CRC32C_00_TO_1F, CRC32C.compute(sequence), "CRC32C(bytes 0x00..0x1F)");
        
        byte[] string = "123456789".getBytes(StandardCharsets.US_ASCII);
        assertEquals(CRC32C_123456789, CRC32C.compute(string), "CRC32C('123456789')");
    }

    @Test
    void knownVectorsSoftwarePath() {
        // Same vectors, pure-Java implementation
        assertEquals(CRC32C_EMPTY, CRC32C.computeSoftware(new byte[0]), "CRC32C Software(empty)");
        assertEquals(CRC32C_A, CRC32C.computeSoftware(new byte[]{0x61}), "CRC32C Software('a')");
        
        byte[] zeros = new byte[32];
        Arrays.fill(zeros, (byte) 0x00);
        assertEquals(CRC32C_32_ZEROES, CRC32C.computeSoftware(zeros), "CRC32C Software(32 × 0x00)");
        
        byte[] ff = new byte[32];
        Arrays.fill(ff, (byte) 0xFF);
        assertEquals(CRC32C_32_FF, CRC32C.computeSoftware(ff), "CRC32C Software(32 × 0xFF)");
        
        byte[] sequence = new byte[32];
        for (int i = 0; i < 32; i++) {
            sequence[i] = (byte) i;
        }
        assertEquals(CRC32C_00_TO_1F, CRC32C.computeSoftware(sequence), "CRC32C Software(bytes 0x00..0x1F)");
        
        byte[] string = "123456789".getBytes(StandardCharsets.US_ASCII);
        assertEquals(CRC32C_123456789, CRC32C.computeSoftware(string), "CRC32C Software('123456789')");
    }

    @Test
    void hardwareAndSoftwareEquivalence() {
        // Test various buffer sizes for equivalence
        byte[][] testBuffers = {
            new byte[0],                              // empty
            new byte[]{0x00},                         // 1 byte
            new byte[]{0x61, 0x62, 0x63},            // 3 bytes
            new byte[31],                             // 31 bytes (odd size)
            new byte[32],                             // 32 bytes (RFC vector size)
            new byte[33],                             // 33 bytes
            new byte[1024],                           // 1KB
            generateRandomBuffer(1 << 20)             // 1MB pseudo-random
        };

        for (byte[] buffer : testBuffers) {
            int hardware = CRC32C.compute(buffer);
            int software = CRC32C.computeSoftware(buffer);
            assertEquals(hardware, software, 
                String.format("Hardware/Software mismatch on buffer size %d", buffer.length));
        }
    }

    @Test
    void offsetAndLengthHandling() {
        byte[] data = "Hello, World!".getBytes(StandardCharsets.US_ASCII);
        
        // Test various offset/length combinations
        assertEquals(CRC32C.compute(data), CRC32C.compute(data, 0, data.length));
        assertEquals(CRC32C.compute("Hello".getBytes()), CRC32C.compute(data, 0, 5));
        assertEquals(CRC32C.compute("World!".getBytes()), CRC32C.compute(data, 7, 6));
        assertEquals(CRC32C.compute("!".getBytes()), CRC32C.compute(data, 12, 1));
        
        // Test pure-Java path too
        assertEquals(CRC32C.computeSoftware(data), CRC32C.computeSoftware(data, 0, data.length));
        assertEquals(CRC32C.computeSoftware("Hello".getBytes()), CRC32C.computeSoftware(data, 0, 5));
        assertEquals(CRC32C.computeSoftware("World!".getBytes()), CRC32C.computeSoftware(data, 7, 6));
        assertEquals(CRC32C.computeSoftware("!".getBytes()), CRC32C.computeSoftware(data, 12, 1));
    }

    @Test
    void incrementalChecksum() {
        byte[] data = "Incremental checksum test data".getBytes(StandardCharsets.US_ASCII);
        
        // Compute with one-shot method
        int oneShot = CRC32C.compute(data);
        
        // Compute incrementally
        Checksum checksum = CRC32C.newChecksum();
        checksum.update(data, 0, 5);      // "Incre"
        checksum.update(data, 5, 11);     // "mental "
        checksum.update(data, 16, 8);     // "checksum"
        checksum.update(data, 24, 6);     // " test"
        checksum.update(data, 30, 0);     // "" (end, no data left)
        
        int incremental = (int) checksum.getValue();
        assertEquals(oneShot, incremental, "Incremental checksum should match one-shot");
        
// Test pure-Java path (computeSoftware doesn't support incremental, so just verify it matches one-shot)
        int softwareOneShot = CRC32C.computeSoftware(data);
        assertEquals(oneShot, softwareOneShot, "Software should match hardware");
    }

    @Test
    void nullInputThrows() {
        assertThrows(NullPointerException.class, () -> CRC32C.compute(null));
        assertThrows(NullPointerException.class, () -> CRC32C.computeSoftware(null));
    }

    @Test
    void invalidOffsetLengthThrows() {
        byte[] data = new byte[10];
        
        assertThrows(IndexOutOfBoundsException.class, () -> CRC32C.compute(data, -1, 5));
        assertThrows(IndexOutOfBoundsException.class, () -> CRC32C.compute(data, 0, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> CRC32C.compute(data, 5, 10));
        assertThrows(IndexOutOfBoundsException.class, () -> CRC32C.compute(data, 10, 1));
        
        // Same for software path
        assertThrows(IndexOutOfBoundsException.class, () -> CRC32C.computeSoftware(data, -1, 5));
        assertThrows(IndexOutOfBoundsException.class, () -> CRC32C.computeSoftware(data, 0, -1));
        assertThrows(IndexOutOfBoundsException.class, () -> CRC32C.computeSoftware(data, 5, 10));
        assertThrows(IndexOutOfBoundsException.class, () -> CRC32C.computeSoftware(data, 10, 1));
    }

    /**
     * Helper to generate deterministic pseudo-random buffer for testing.
     * Uses a fixed seed for reproducibility.
     */
    private byte[] generateRandomBuffer(int size) {
        byte[] buffer = new byte[size];
        Random random = new Random(12345); // Fixed seed
        random.nextBytes(buffer);
        return buffer;
    }
}