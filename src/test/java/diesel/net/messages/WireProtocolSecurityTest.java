package diesel.net.messages;

import diesel.net.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for the new wire protocol message types (prompt4.md #28).
 * Verifies that Java serialization payloads are rejected and message types work correctly.
 */
public class WireProtocolSecurityTest {

    @org.junit.jupiter.api.Test
    public void testJavaSerializationPayloadRejected() throws Exception {
        // Test that Java serialized objects are rejected by the wire protocol
        byte[] maliciousPayload = createJavaSerializedPayload();
        
        try {
            WireProtocol.Frame frame = WireProtocol.decodeFrame(
                new DataInputStream(new ByteArrayInputStream(maliciousPayload)));
            fail("Should have thrown InvalidMessageException for Java serialization payload");
        } catch (InvalidMessageException e) {
            // Expected - Java serialization should be rejected
            assertTrue(e.getMessage().contains("Bad magic byte"));
        }
    }

    @org.junit.jupiter.api.Test
    public void testQueryMessageRoundTrip() throws Exception {
        // Test QueryMessage serialization and deserialization
        String testQuery = "SELECT * FROM users WHERE id = ?";
        UUID testTxId = UUID.randomUUID();
        QueryMessage original = new QueryMessage(testQuery, testTxId);
        
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        original.writeTo(out);
        
        // Deserialize
        QueryMessage decoded = QueryMessage.readFrom(
            new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        
        assertEquals(testQuery, decoded.getQuery());
        assertEquals(testTxId, decoded.getTransactionId());
    }

    @org.junit.jupiter.api.Test
    public void testQueryResultMessageRoundTrip() throws Exception {
        // Test QueryResultMessage serialization and deserialization
        String testStatus = "OK";
        long testDuration = 123456789L;
        List<String> testColumns = Arrays.asList("id", "name", "email");
        List<List<Object>> testRows = Arrays.asList(
            Arrays.asList(1, "Alice", "alice@example.com"),
            Arrays.asList(2, "Bob", "bob@example.com")
        );
        UUID testTxId = UUID.randomUUID();
        
        QueryResultMessage original = new QueryResultMessage(testStatus, testDuration, testColumns, testRows, testTxId);
        
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        original.writeTo(out);
        
        // Deserialize
        QueryResultMessage decoded = QueryResultMessage.readFrom(
            new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        
        assertEquals(testStatus, decoded.getStatus());
        assertEquals(testDuration, decoded.getDuration());
        assertEquals(testColumns, decoded.getColumnNames());
        assertEquals(2, decoded.getRows().size());
        assertEquals(1, decoded.getRows().get(0).get(0));
        assertEquals("Alice", decoded.getRows().get(0).get(1));
        assertEquals(testTxId, decoded.getTransactionId());
    }

    @org.junit.jupiter.api.Test
    public void testPrepareMessageRoundTrip() throws Exception {
        // Test PrepareMessage serialization and deserialization
        String testSql = "SELECT * FROM users WHERE id = ?";
        UUID testTxId = UUID.randomUUID();
        PrepareMessage original = new PrepareMessage(testSql, testTxId);
        
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        original.writeTo(out);
        
        // Deserialize
        PrepareMessage decoded = PrepareMessage.readFrom(
            new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        
        assertEquals(testSql, decoded.getSqlTemplate());
        assertEquals(testTxId, decoded.getTransactionId());
    }

    @org.junit.jupiter.api.Test
    public void testBatchQueryMessageRoundTrip() throws Exception {
        // Test BatchQueryMessage serialization and deserialization
        List<String> testQueries = Arrays.asList("SELECT * FROM users", "SELECT * FROM orders");
        UUID testTxId = UUID.randomUUID();
        BatchQueryMessage original = new BatchQueryMessage(testQueries, testTxId);
        
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        original.writeTo(out);
        
        // Deserialize
        BatchQueryMessage decoded = BatchQueryMessage.readFrom(
            new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        
        assertEquals(2, decoded.getQueries().size());
        assertEquals("SELECT * FROM users", decoded.getQueries().get(0));
        assertEquals("SELECT * FROM orders", decoded.getQueries().get(1));
        assertEquals(testTxId, decoded.getTransactionId());
    }

    @org.junit.jupiter.api.Test
    public void testHealthCheckMessageRoundTrip() throws Exception {
        // Test HealthCheckMessage serialization and deserialization
        long testTimestamp = System.currentTimeMillis();
        UUID testTxId = UUID.randomUUID();
        HealthCheckMessage original = new HealthCheckMessage(testTimestamp, testTxId);
        
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        original.writeTo(out);
        
        // Deserialize
        HealthCheckMessage decoded = HealthCheckMessage.readFrom(
            new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        
        assertEquals(testTimestamp, decoded.getTimestamp());
        assertEquals(testTxId, decoded.getTransactionId());
    }

    @org.junit.jupiter.api.Test
    public void testCompressionHandshakeMessageRoundTrip() throws Exception {
        // Test CompressionHandshakeMessage serialization and deserialization
        boolean supportsCompression = true;
        List<String> supportedAlgorithms = Arrays.asList("GZIP", "ZSTD");
        int preferredLevel = 6;
        int thresholdBytes = 1024;
        UUID testTxId = UUID.randomUUID();
        
        CompressionHandshakeMessage original = new CompressionHandshakeMessage(
            supportsCompression, supportedAlgorithms, preferredLevel, thresholdBytes, testTxId);
        
        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        original.writeTo(out);
        
        // Deserialize
        CompressionHandshakeMessage decoded = CompressionHandshakeMessage.readFrom(
            new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        
        assertTrue(decoded.isClientSupportsCompression());
        assertEquals(2, decoded.getSupportedAlgorithms().size());
        assertEquals("GZIP", decoded.getSupportedAlgorithms().get(0));
        assertEquals(6, decoded.getPreferredCompressionLevel());
        assertEquals(1024, decoded.getCompressionThresholdBytes());
        assertEquals(testTxId, decoded.getTransactionId());
    }

    private byte[] createJavaSerializedPayload() {
        // Create a malicious Java serialized payload that should be rejected
        // This simulates an RCE attempt via Java deserialization
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            // Write a fake header that doesn't match DSEL magic
            baos.write(new byte[]{'J', 'A', 'V', 'A'}); // Wrong magic bytes
            baos.write(new byte[]{0x00, 0x02}); // Fake version
            baos.write(new byte[]{0x00, 0x01}); // Fake type
            baos.write(new byte[]{0x00, 0x00, 0x00, 0x10}); // Fake length
            baos.write(new byte[16]); // Fake payload
            baos.write(new byte[]{0x00, 0x00, 0x00, 0x00}); // Fake CRC
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test payload", e);
        }
        return baos.toByteArray();
    }
}