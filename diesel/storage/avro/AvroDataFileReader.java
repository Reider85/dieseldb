package diesel.storage.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Low-level streaming reader for Avro object-container (data) files.
 *
 * <p>The reader parses the Avro data file header itself (magic, metadata map,
 * 16-byte sync marker), then walks the data blocks that follow:
 * {@code [count, size, payload, sync] x N}. Each block's payload is
 * decompressed with the codec declared in the file header and decoded into
 * {@link GenericRecord}s through a {@link GenericDatumReader}.
 *
 * <p>All the features requested by Prompt 61 are exposed:
 * <ul>
 *   <li><b>Seek by sync markers</b> – {@link #seekToSyncMarker(long)} scans the
 *       raw file bytes for the header's 16-byte marker and lands the reader at
 *       the beginning of the block that follows it (or EOF), enabling
 *       block-granular random access (used later by the parallel reader).</li>
 *   <li><b>Streaming read of large files</b> – {@link #hasNext()}/{@link #next()}
 *       consume one block at a time and never materialise the whole file in
 *       memory; the record cursor, block bytes and current offset are exposed
 *       for diagnostics and future split points.</li>
 *   <li><b>Projection pushdown</b> – a reader can be constructed with a set of
 *       column names; the {@link GenericDatumReader} is then configured with a
 *       reader schema containing only those fields, so non-requested fields are
 *       {@code skip()}-ped straight off the binary stream and never converted.</li>
 * </ul>
 *
 * <p>Block integrity is checked at every boundary: a block whose trailing
 * sync marker does not equal the header marker is reported as a corrupt file.
 *
 * @since Prompt 61
 */
public final class AvroDataFileReader implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroDataFileReader.class);

    /** Header magic: {@code 'O' 'b' 'j' 0x01}. */
    private static final byte[] MAGIC = {'O', 'b', 'j', 0x01};

    /** Length of the Avro sync marker in bytes. */
    public static final int SYNC_SIZE = 16;

    private static final String META_SCHEMA = "avro.schema";
    private static final String META_CODEC = "avro.codec";

    private final File file;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final long fileLength;
    private final long headerEndPos;

    private final Schema writerSchema;
    private final String codecName;
    private final byte[] syncMarker;
    private final Schema readerSchema;
    private final boolean readerSchemaNameCompatible;
    private final Codec codec;
    private final AvroFileHeader fileHeader;

    private final GenericDatumReader<GenericRecord> datumReader;

    // Block walking state -------------------------------------------------
    private long nextBlockHeaderPos;          // file offset of the next block header
    private long currentBlockStart = -1;      // file offset of the current block's payload
    private long currentBlockCount;           // records declared by the current block
    private long currentBlockRemaining;       // records not yet consumed in the current block
    private int numBlocksRead;
    private Decoder currentBlockDecoder;
    private boolean eof;

    /**
     * Opens the given Avro data file for a full read (every field of every record).
     *
     * @param avroFile the Avro object-container file
     * @throws IOException if the file is missing, is not an Avro data file, or its
     *                     header cannot be parsed
     */
    public AvroDataFileReader(File avroFile) throws IOException {
        this(avroFile, null, null);
    }

    /**
     * Opens the given Avro data file and pushes the requested columns down to the
     * binary decoder. Non-requested writer fields are skipped at the token level
     * and do not appear in the returned records. If the writer schema is not a
     * RECORD or none of the requested fields exist, a full read is performed.
     *
     * @param avroFile         the Avro object-container file
     * @param projectedColumns requested field names; {@code null} or empty means a full read
     * @throws IOException if the file cannot be read or its header is invalid
     */
    public AvroDataFileReader(File avroFile, Collection<String> projectedColumns) throws IOException {
        this(avroFile, projectedColumns, null);
    }

    /**
     * Opens the given Avro data file with an explicit reader schema. The header
     * schema stays the writer schema; the two are resolved by
     * {@link GenericDatumReader} when their record fullnames match (Avro schema
     * resolution). Otherwise the reader schema is discarded (with a warning) and
     * the full header schema is used.
     *
     * @param avroFile     the Avro object-container file
     * @param readerSchema explicit reader schema (may be {@code null} for a full read)
     * @throws IOException if the file cannot be read or its header is invalid
     */
    public AvroDataFileReader(File avroFile, Schema readerSchema) throws IOException {
        this(avroFile, null, readerSchema);
    }

    /**
     * Shared constructor. Parses the header, resolves the reader schema and
     * initialises the datum reader. {@code projectedColumns} and
     * {@code explicitReader} are mutually exclusive: when projection is requested
     * a narrowed schema is derived from the writer schema; otherwise the explicit
     * schema is adopted only when it shares the writer record's fullname.
     */
    private AvroDataFileReader(File avroFile, Collection<String> projectedColumns, Schema explicitReader) throws IOException {
        this.file = avroFile;
        if (avroFile == null || !avroFile.isFile()) {
            throw new IOException("Avro data file does not exist: " + avroFile);
        }

        Header header = parseHeader(avroFile);
        this.writerSchema = header.writerSchema;
        this.codecName = header.codecName;
        this.syncMarker = header.syncMarker;
        this.fileHeader = header.fileHeader;
        this.codec = "null".equals(codecName) ? null : createCodec(codecName);

        Schema resolved = null;
        if (projectedColumns != null && !projectedColumns.isEmpty()) {
            resolved = buildProjectionSchema(writerSchema, projectedColumns);
        } else if (explicitReader != null) {
            resolved = explicitReader;
        }
        this.readerSchemaNameCompatible = resolved != null
                && writerSchema.getType() == Schema.Type.RECORD
                && resolved.getType() == Schema.Type.RECORD
                && resolved.getFullName().equals(writerSchema.getFullName());
        if (resolved != null && !readerSchemaNameCompatible) {
            LOGGER.warn("Discarding reader schema '{}' for {} (record fullname does not match writer schema '{}'; doing a full read)",
                    resolved.getFullName(), avroFile, writerSchema.getFullName());
        }
        this.readerSchema = readerSchemaNameCompatible ? resolved : null;
        Schema datumReaderReader = readerSchema != null ? readerSchema : writerSchema;
        this.datumReader = new GenericDatumReader<>(writerSchema, datumReaderReader);

        this.raf = new RandomAccessFile(avroFile, "r");
        try {
            this.channel = raf.getChannel();
            this.fileLength = raf.length();
        } catch (IOException e) {
            raf.close();
            throw e;
        }
        this.headerEndPos = header.headerEndPos;
        this.nextBlockHeaderPos = headerEndPos;

        LOGGER.debug("Opened Avro data file {} (schema '{}', codec '{}', first block at {} bytes)",
                avroFile, writerSchema.getFullName(), codecName, headerEndPos);
    }

    // ─── Header parsing ─────────────────────────────────────────────

    /**
     * Parsed head of an Avro object-container file.
     */
    private record Header(Schema writerSchema, String codecName, byte[] syncMarker,
                          long headerEndPos, AvroFileHeader fileHeader) { }

    private static Header parseHeader(File avroFile) throws IOException {
        try (CountingInputStream cin = new CountingInputStream(new BufferedInputStream(
                Files.newInputStream(avroFile.toPath()), 8192))) {
            Decoder decoder = DecoderFactory.get().directBinaryDecoder(cin, null);

            byte[] magic = new byte[MAGIC.length];
            try {
                decoder.readFixed(magic, 0, magic.length);
            } catch (IOException e) {
                throw new IOException("Not an Avro data file (bad magic): " + avroFile, e);
            }
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IOException("Not an Avro data file (bad magic " + hex(magic) + "): " + avroFile);
            }

            Map<String, byte[]> meta = readMetaMap(decoder);

            byte[] sync = new byte[SYNC_SIZE];
            try {
                decoder.readFixed(sync, 0, SYNC_SIZE);
            } catch (IOException e) {
                throw new IOException("Truncated Avro header (missing sync marker): " + avroFile, e);
            }

            byte[] schemaBytes = meta.get(META_SCHEMA);
            if (schemaBytes == null) {
                throw new IOException("Avro header is missing 'avro.schema' metadata: " + avroFile);
            }
            String schemaJson = new String(schemaBytes, StandardCharsets.UTF_8);
            Schema writerSchema = new Schema.Parser().parse(schemaJson);

            byte[] codecBytes = meta.get(META_CODEC);
            String codecName = codecBytes == null
                    ? "null"
                    : new String(codecBytes, StandardCharsets.UTF_8);
            if (codecName == null || codecName.isBlank()) {
                codecName = "null";
            }
            AvroFileHeader fileHeader;
            try {
                fileHeader = AvroFileHeader.fromMetaMap(meta);
            } catch (IllegalArgumentException e) {
                throw new IOException("Corrupt DieselDB metadata in Avro header of " + avroFile, e);
            }
            return new Header(writerSchema, codecName, sync, cin.count, fileHeader);
        }
    }

    /**
     * An {@link InputStream} that counts the exact number of bytes consumed by the
     * decoder. Used to determine where the file header ends and the first data
     * block begins.
     */
    private static final class CountingInputStream extends InputStream {
        private final InputStream in;
        private long count;

        CountingInputStream(InputStream in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b >= 0) {
                count++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n > 0) {
                count += n;
            }
            return n;
        }

        @Override
        public long skip(long n) throws IOException {
            long skipped = in.skip(n);
            count += skipped;
            return skipped;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /**
     * Reads the metadata map that follows the magic: a sequence of
     * {@code (count, key, value)} blocks with the final count being zero.
     */
    private static Map<String, byte[]> readMetaMap(Decoder decoder) throws IOException {
        Map<String, byte[]> map = new HashMap<>();
        long count;
        do {
            count = decoder.readLong();
            if (count < 0) {
                long blockSize = -count;
                for (long i = 0; i < blockSize; i++) {
                    putEntry(decoder, map);
                }
                long terminator = decoder.readLong(); // negative-count block framing
                if (terminator != 0) {
                    throw new IOException("Malformed Avro header map block terminator");
                }
            } else {
                for (long i = 0; i < count; i++) {
                    putEntry(decoder, map);
                }
            }
        } while (count != 0);
        return map;
    }

    private static void putEntry(Decoder decoder, Map<String, byte[]> map) throws IOException {
        String key = decoder.readString();
        ByteBuffer value = decoder.readBytes(null);
        byte[] bytes = new byte[value.remaining()];
        value.get(bytes);
        map.put(key, bytes);
    }

    // ─── Header / record traversal ──────────────────────────────────

    /**
     * Returns {@code true} if at least one more record can be read. Advances the
     * reader block-by-block; a block whose trailing sync marker is corrupt is
     * reported as an {@link IOException} wrapped in {@link UncheckedIOException}.
     */
    public boolean hasNext() {
        while (currentBlockRemaining == 0 && !eof) {
            try {
                if (!loadNextBlock()) {
                    eof = true;
                    return false;
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read block from Avro file: " + file, e);
            }
        }
        return currentBlockRemaining > 0;
    }

    /**
     * Returns the next {@link GenericRecord}. See {@link #hasNext()}.
     *
     * @throws NoSuchElementException if no more records are available
     */
    public GenericRecord next() {
        try {
            return nextRecord();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read record from Avro file: " + file, e);
        }
    }

    /**
     * Returns the next {@link GenericRecord}, throwing {@link IOException}
     * directly (suitable for streaming consumers that can propagate checked
     * exceptions).
     *
     * @throws NoSuchElementException if no more records are available
     * @throws IOException            on malformed block or sync-marker failure
     */
    public GenericRecord nextRecord() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException("No more Avro records in " + file);
        }
        GenericRecord record = datumReader.read(null, currentBlockDecoder);
        currentBlockRemaining--;
        return record;
    }

    /**
     * Reads the {@link #SYNC_SIZE}-byte sync syncing terminated block headers until
     * EOF is reached, counting blocks without decoding records. Resets the reader
     * state so a subsequent read starts from the beginning again.
     *
     * @return total number of data blocks in the file
     * @throws IOException if a block header or sync marker is corrupt
     */
    public long countBlocks() throws IOException {
        reset();
        long blocks = 0;
        while (loadNextBlock()) {
            blocks++;
        }
        eof = true;
        reset();
        return blocks;
    }

    private boolean loadNextBlock() throws IOException {
        if (eof) {
            return false;
        }
        if (nextBlockHeaderPos >= fileLength) {
            eof = true;
            return false;
        }
        if (fileLength - nextBlockHeaderPos == SYNC_SIZE) {
            byte[] tail = new byte[SYNC_SIZE];
            readFully(nextBlockHeaderPos, tail);
            if (Arrays.equals(tail, syncMarker)) {
                eof = true;
                return false;
            }
        }
        channel.position(nextBlockHeaderPos);

        long count = readZigzagVlq();
        if (count < 0) {
            throw new IOException("Negative record count (" + count + ") at block header offset "
                    + nextBlockHeaderPos + " in " + file);
        }
        long size = readZigzagVlq();
        if (size < 0) {
            throw new IOException("Negative block size (" + size + ") in " + file);
        }
        if (size > Integer.MAX_VALUE) {
            throw new IOException("Avro block too large to buffer (" + size + " bytes) in " + file);
        }

        long payloadStart = channel.position();
        byte[] payload = new byte[(int) size];
        readFully(payloadStart, payload);
        long syncPos = payloadStart + size;
        if (syncPos + SYNC_SIZE > fileLength) {
            throw new IOException("Truncated Avro block: expected sync marker at offset " + syncPos
                    + " but file ends at " + fileLength + " (" + file + ")");
        }

        byte[] sync = new byte[SYNC_SIZE];
        readFully(syncPos, sync);
        if (!Arrays.equals(sync, syncMarker)) {
            throw new IOException("Avro block sync marker mismatch at offset " + syncPos + " in " + file
                    + " (corrupt file or interrupted write)");
        }

        currentBlockStart = payloadStart;
        currentBlockCount = count;
        currentBlockRemaining = count;
        numBlocksRead++;
        nextBlockHeaderPos = syncPos + SYNC_SIZE;

        byte[] data = payload;
        if (codec != null) {
            ByteBuffer decompressed = codec.decompress(ByteBuffer.wrap(payload));
            data = new byte[decompressed.remaining()];
            decompressed.get(data);
        }
        currentBlockDecoder = DecoderFactory.get().binaryDecoder(data, null);
        return true;
    }

    // ─── Sync-marker seeking ────────────────────────────────────────

    /**
     * Seeks to the sync marker at or after {@code position} and aligns the reader
     * so the next {@link #hasNext()}/{@link #next()} read starts at the block that
     * follows that marker (or EOF if the marker is the last one in the file).
     *
     * <p>This gives block-granular random access: after a {@code seekToSyncMarker}
     * the reader's block statistics ({@link #getCurrentBlockStart()} etc.) reflect
     * the block that begins there.
     *
     * @param position byte offset from which to start scanning
     * @return {@code true} if a sync marker was found (the reader is now aligned
     *         to the following block or EOF), {@code false} if none was found
     *         (the reader is positioned at EOF)
     * @throws IOException on I/O errors
     */
    public boolean seekToSyncMarker(long position) throws IOException {
        currentBlockDecoder = null;
        currentBlockRemaining = 0;
        eof = false;
        long markerPos = findSyncMarker(position);
        if (markerPos < 0) {
            eof = true;
            return false;
        }
        nextBlockHeaderPos = markerPos + SYNC_SIZE;
        LOGGER.debug("Seeked to sync marker at {} in {} (next block header at {})",
                markerPos, file, nextBlockHeaderPos);
        return true;
    }

    /**
     * Returns the raw byte position where the next block header would be read
     * (i.e. the reader's logical position). Equal to the stream offset used by
     * {@link #seekToSyncMarker(long)}.
     */
    public long getPosition() {
        return nextBlockHeaderPos;
    }

    /**
     * Scans the raw file bytes for the header's sync marker, starting at
     * {@code startPos}. Returns the offset of the marker's first byte, or -1.
     */
    private long findSyncMarker(long startPos) throws IOException {
        byte[] buf = new byte[16384];
        int match = 0;
        long pos = Math.max(0, startPos);
        while (pos < fileLength) {
            int toRead = (int) Math.min(buf.length, fileLength - pos);
            readFully(pos, buf, toRead);
            for (int i = 0; i < toRead; i++) {
                byte c = buf[i];
                if (c == syncMarker[match]) {
                    match++;
                    if (match == SYNC_SIZE) {
                        return pos + i - (SYNC_SIZE - 1);
                    }
                } else {
                    match = (c == syncMarker[0]) ? 1 : 0;
                }
            }
            pos += toRead;
        }
        return -1;
    }

    // ─── Raw I/O helpers ────────────────────────────────────────────

    /**
     * Reads a zig-zag encoded variable-length long from the channel at its current
     * position (Avro's integer/long binary encoding).
     */
    private long readZigzagVlq() throws IOException {
        long value = 0;
        int shift = 0;
        int b;
        do {
            ByteBuffer one = ByteBuffer.allocate(1);
            int n = channel.read(one);
            if (n < 0) {
                throw new EOFException("Unexpected end of Avro file while reading block header: " + file);
            }
            b = one.array()[0] & 0xFF;
            value |= (long) (b & 0x7F) << shift;
            shift += 7;
            if (shift > 63) {
                throw new IOException("Malformed varint in Avro block header of " + file);
            }
        } while ((b & 0x80) != 0);
        return (value >>> 1) ^ -(value & 1L); // zig-zag decode
    }

    private void readFully(long position, byte[] out) throws IOException {
        readFully(position, out, out.length);
    }

    private void readFully(long position, byte[] out, int length) throws IOException {
        channel.position(position);
        int off = 0;
        while (off < length) {
            int got = channel.read(ByteBuffer.wrap(out, off, length - off));
            if (got < 0) {
                throw new EOFException("Unexpected end of Avro file " + file + " (expected " + length
                        + " bytes at offset " + position + ")");
            }
            off += got;
        }
    }

    // ─── Accessors ──────────────────────────────────────────────────

    /** Returns the writer schema declared in the file header. */
    public Schema getSchema() {
        return writerSchema;
    }

    /** Returns the writer schema declared in the file header. */
    public Schema getWriterSchema() {
        return writerSchema;
    }

    /** Returns the reader schema used to resolve records, or {@code null} for a full read. */
    public Schema getReaderSchema() {
        return readerSchema;
    }

    /** Returns the 16-byte sync marker parsed from the file header. */
    public byte[] getSyncMarker() {
        return syncMarker.clone();
    }

    /** Returns the codec name declared in the file header (e.g. {@code "null"}). */
    public String getCodecName() {
        return codecName;
    }

    /**
     * Returns the DieselDB header metadata parsed from the file's metadata map.
     * For files written before Prompt 76 (which only carry {@code avro.schema} /
     * {@code avro.codec}) all DieselDB keys fall back to their defaults.
     *
     * @return the parsed header metadata (never {@code null})
     */
    public AvroFileHeader getFileHeader() {
        return fileHeader;
    }

    /** Returns the absolute path of the backing file. */
    public File getFile() {
        return file;
    }

    /**
     * Returns {@code true} when an explicitly supplied reader schema shares the
     * writer schema's record fullname (i.e. it was usable for Avro schema
     * resolution rather than silently discarded).
     */
    public boolean isReaderSchemaCompatible() {
        return readerSchemaNameCompatible;
    }

    /** Returns the file offset of the current block's payload, or -1 before the first block. */
    public long getCurrentBlockStart() {
        return currentBlockStart;
    }

    /** Returns the record count declared by the current block. */
    public long getCurrentBlockRecordCount() {
        return currentBlockCount;
    }

    /** Returns the number of records not yet consumed in the current block. */
    public long getCurrentBlockRemaining() {
        return currentBlockRemaining;
    }

    /** Returns how many blocks have been loaded so far. */
    public int getNumBlocksRead() {
        return numBlocksRead;
    }

    /** Returns the file length in bytes. */
    public long getFileLength() {
        return fileLength;
    }

    // ─── Close / reset ──────────────────────────────────────────────

    /**
     * Rewinds the reader so the next read starts at the first data block.
     * Keeps the header and datum reader (cheap).
     */
    public void reset() {
        nextBlockHeaderPos = headerEndPos;
        currentBlockStart = -1;
        currentBlockCount = 0;
        currentBlockRemaining = 0;
        currentBlockDecoder = null;
        numBlocksRead = 0;
        eof = false;
    }

    @Override
    public void close() throws IOException {
        if (raf != null) {
            raf.close();
        }
    }

    // ─── Projection schema building ─────────────────────────────────

    /**
     * Builds a projected reader schema (record containing only the requested
     * writer fields), or {@code null} when nothing can be projected (no requested
     * name matches a writer field, or the writer schema is not a RECORD).
     */
    static Schema buildProjectionSchema(Schema writer, Collection<String> requested) {
        if (requested == null || requested.isEmpty()) {
            return null;
        }
        if (writer.getType() != Schema.Type.RECORD) {
            return null;
        }
        Set<String> want = new HashSet<>();
        for (String r : requested) {
            want.add(r);
        }
        List<Schema.Field> projected = new ArrayList<>();
        for (Schema.Field f : writer.getFields()) {
            if (want.contains(f.name())) {
                projected.add(new Schema.Field(f.name(), f.schema()));
            }
        }
        if (projected.isEmpty()) {
            return null;
        }
        Schema record = Schema.createRecord(writer.getName(), writer.getDoc(), writer.getNamespace(), writer.isError());
        record.setFields(projected);
        return record;
    }

    /**
     * Creates a {@link Codec} for the given codec name. {@code
     * CodecFactory#createInstance()} is protected in this Avro version, so the
     * instance is obtained reflectively.
     */
    private static Codec createCodec(String codecName) throws IOException {
        try {
            Method createInstance = CodecFactory.class.getDeclaredMethod("createInstance");
            createInstance.setAccessible(true);
            return (Codec) createInstance.invoke(CodecFactory.fromString(codecName));
        } catch (ReflectiveOperationException | SecurityException e) {
            throw new IOException("Unsupported Avro codec '" + codecName + "': " + e.getMessage(), e);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }
}