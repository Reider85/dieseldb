package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class AvroDataFileWriter {

    private final DataFileWriter<GenericRecord> dataFileWriter;
    private final DatumWriter<GenericRecord> datumWriter;
    private final Schema schema;
    private final File outputFile;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long syncInterval;
    private final int effectiveBlockSize;
    private long bytesSinceLastSync;
    private AvroBlockManager blockManager;

    public AvroDataFileWriter(List<String> columns, Map<String, Class<?>> columnTypes, File outputFile) throws IOException {
        this(columns, columnTypes, outputFile, nullCodec());
    }

    public AvroDataFileWriter(List<String> columns, Map<String, Class<?>> columnTypes, File outputFile,
                                 CodecFactory codec) throws IOException {
        this(columns, columnTypes, outputFile, codec, 0);
    }

    public AvroDataFileWriter(List<String> columns, Map<String, Class<?>> columnTypes, File outputFile,
                                 CodecFactory codec, int blockSize) throws IOException {
        this.outputFile = outputFile;
        this.schema = AvroSchemaManager.buildTableSchema("temp", columns, columnTypes);
        this.datumWriter = new GenericDatumWriter<>(schema);
        this.dataFileWriter = new DataFileWriter<>(datumWriter);
        AvroBlockConfig cfg = AvroBlockConfig.resolve();
        this.effectiveBlockSize = (int) cfg.effectiveBlockSize();
        this.syncInterval = cfg.syncInterval();
        if (codec != null) {
            dataFileWriter.setCodec(codec);
        }
        try {
            dataFileWriter.create(schema, new FileOutputStream(outputFile));
        } catch (IOException e) {
            try (var out = dataFileWriter) {
                // already closed via create failure; release the stream
            } catch (IOException ignored) {
            }
            throw e;
        }
        this.bytesSinceLastSync = 0;
        this.blockManager = null;
    }

    public synchronized void writeRow(Map<String, Object> rowMap) throws IOException {
        if (closed.get()) {
            throw new IOException("DataFileWriter is already closed");
        }
        Object[] rowArray = new Object[schema.getFields().size()];
        for (int i = 0; i < schema.getFields().size(); i++) {
            String fieldName = schema.getFields().get(i).name();
            rowArray[i] = rowMap.get(fieldName);
        }
        GenericRecord record = toRecord(rowArray, schema);
        dataFileWriter.append(record);
        bytesSinceLastSync += estimatedEncodedSize(rowArray);
        if (AvroBlockManager.shouldInsertSyncMarker(bytesSinceLastSync, syncInterval)) {
            dataFileWriter.sync();
            bytesSinceLastSync = 0;
        }
    }

    private static long estimatedEncodedSize(Object[] row) {
        long total = 0;
        for (Object value : row) {
            total += encodedScalarSize(value);
        }
        return total;
    }

    /**
     * Returns the exact Avro binary-encoding size for the scalar types used by
     * this engine (long/int → zigzag varint, String → varint length + UTF-8 bytes,
     * boolean → 1, double/float → 8/4). Used to drive the byte-based sync interval
     * (Prompt 68), since {@link DataFileWriter} buffers records internally and only
     * exposes byte counts to the underlying stream once a block is emitted.
     */
    private static long encodedScalarSize(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof byte[] b) {
            return b.length;
        }
        if (value instanceof String s) {
            int len = s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            return varintLen(len) + len;
        }
        if (value instanceof Boolean) {
            return 1;
        }
        if (value instanceof Double) {
            return 8;
        }
        if (value instanceof Float) {
            return 4;
        }
        if (value instanceof Long l) {
            return varintLen(zigzag(l));
        }
        if (value instanceof Integer i) {
            return varintLen(zigzag(i.longValue()));
        }
        return 8;
    }

    private static long zigzag(long v) {
        return (v << 1) ^ (v >> 63);
    }

    private static long varintLen(long zigzag) {
        long n = 1;
        long rest = zigzag >>> 7;
        while (rest != 0) {
            n++;
            rest >>>= 7;
        }
        return n;
    }

    private static GenericRecord toRecord(Object[] row, Schema schema) {
        GenericRecord record = new org.apache.avro.generic.GenericData.Record(schema);
        for (int i = 0; i < schema.getFields().size() && i < row.length; i++) {
            Schema.Field field = schema.getFields().get(i);
            record.put(field.name(), row[i]);
        }
        return record;
    }

    public synchronized void flush() throws IOException {
        if (closed.get()) {
            return;
        }
        dataFileWriter.flush();
    }

    public synchronized void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                dataFileWriter.close();
            } finally {
                scanWrittenBlocks();
            }
        }
    }

    private void scanWrittenBlocks() throws IOException {
        try (AvroDataFileReader probe = new AvroDataFileReader(outputFile)) {
            byte[] sync = probe.getSyncMarker();
            String codecName = probe.getCodecName();
            long headerEnd = probe.getPosition();
            try (RandomAccessFile raf = new RandomAccessFile(outputFile, "r");
                 FileChannel ch = raf.getChannel()) {
                long pos = headerEnd;
                int index = 0;
                long fileLen = raf.length();
                AvroBlockManager mgr = new AvroBlockManager(sync, codecName, headerEnd, effectiveBlockSize, true);
                while (pos < fileLen) {
                    if (fileLen - pos == AvroBlockManager.SYNC_SIZE) {
                        byte[] tail = new byte[AvroBlockManager.SYNC_SIZE];
                        readFully(ch, pos, tail);
                        if (!Arrays.equals(tail, sync)) {
                            throw new IOException("Trailing sync marker mismatch in " + outputFile);
                        }
                        break;
                    }
                    long[] cv = readZigzagVlq(ch, pos);
                    long count = cv[0];
                    pos = cv[1];
                    long[] sv = readZigzagVlq(ch, pos);
                    long size = sv[0];
                    pos = sv[1];
                    if (size > Integer.MAX_VALUE) {
                        throw new IOException("Avro block too large in " + outputFile);
                    }
                    long payloadStart = pos;
                    pos += size;
                    long syncPos = pos;
                    byte[] actualSync = new byte[AvroBlockManager.SYNC_SIZE];
                    readFully(ch, syncPos, actualSync);
                    if (!Arrays.equals(actualSync, sync)) {
                        throw new IOException("Block sync marker mismatch at " + syncPos + " in " + outputFile);
                    }
                    byte[] payload = new byte[(int) size];
                    readFully(ch, payloadStart, payload);
                    byte[] data = decompress(codecName, payload);
                    long crc = AvroBlockManager.computeCRC32(data);
                    mgr.addBlock(index, count, size, data.length, data, payloadStart, syncPos);
                    index++;
                    pos = syncPos + AvroBlockManager.SYNC_SIZE;
                }
                this.blockManager = mgr;
            }
        }
    }

    private static byte[] decompress(String codecName, byte[] payload) throws IOException {
        if ("null".equals(codecName)) {
            return payload;
        }
        Codec codec = createCodec(codecName);
        ByteBuffer in = ByteBuffer.wrap(payload);
        ByteBuffer out = codec.decompress(in);
        byte[] arr = new byte[out.remaining()];
        out.get(arr);
        return arr;
    }

    private static Codec createCodec(String codecName) throws IOException {
        try {
            Method createInstance = CodecFactory.class.getDeclaredMethod("createInstance");
            createInstance.setAccessible(true);
            return (Codec) createInstance.invoke(CodecFactory.fromString(codecName));
        } catch (ReflectiveOperationException | SecurityException e) {
            throw new IOException("Unsupported Avro codec '" + codecName + "': " + e.getMessage(), e);
        }
    }

    private static long[] readZigzagVlq(FileChannel ch, long pos) throws IOException {
        long value = 0;
        int shift = 0;
        long p = pos;
        while (true) {
            ByteBuffer bb = ByteBuffer.allocate(1);
            int got = ch.read(bb, p);
            if (got < 0) {
                throw new IOException("Truncated varint at " + p);
            }
            byte b = bb.array()[0];
            p++;
            value |= (long) (b & 0x7F) << shift;
            shift += 7;
            if ((b & 0x80) == 0) {
                break;
            }
            if (shift > 63) {
                throw new IOException("Malformed varint");
            }
        }
        return new long[]{(value >>> 1) ^ -(value & 1L), p};
    }

    private static void readFully(FileChannel ch, long pos, byte[] out) throws IOException {
        int off = 0;
        while (off < out.length) {
            int got = ch.read(ByteBuffer.wrap(out, off, out.length - off), pos + off);
            if (got < 0) {
                throw new IOException("Truncated read at " + pos + " wanted " + out.length + " got " + off);
            }
            off += got;
        }
    }

    public synchronized void rollback() throws IOException {
        if (!closed.get()) {
            try {
                dataFileWriter.close();
            } catch (IOException ignored) {
            }
            closed.set(true);
        }
        if (outputFile.exists()) {
            if (!outputFile.delete()) {
                throw new IOException("Failed to rollback file: " + outputFile.getAbsolutePath());
            }
        }
    }

    public File getOutputFile() {
        return outputFile;
    }

    public Schema getSchema() {
        return schema;
    }

    public AvroBlockManager getBlockManager() {
        return blockManager;
    }

    public String getFileSummary() {
        return blockManager != null ? blockManager.getFileSummary() : "uninitialized";
    }

    private static CodecFactory nullCodec() {
        return CodecFactory.nullCodec();
    }
}