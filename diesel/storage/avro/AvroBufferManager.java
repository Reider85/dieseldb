package diesel.storage.avro;

import org.apache.avro.file.CodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Centralised AVRO read/write buffering (Prompt 78).
 *
 * <p>The class owns the four buffer concerns on top of the existing
 * {@link AvroDataFileWriter}/{@link AvroDataFileReader} codecs:
 * <ul>
 *   <li><b>Write buffer sizing</b> — {@link #createWriteBuffer(String, File, List, Map)}
 *       returns a {@link ManagedWriteBuffer} that buffers {@code Map} rows in memory and
 *       de-duplicates the underlying write I/O. The byte budget comes from
 *       {@link AvroBufferConfig#writeBufferSize()} ({@code avro.buffer.write.size}),
 *       so flushes happen on a <em>byte</em> budget rather than a raw row count.</li>
 *   <li><b>Read buffer sizing</b> — {@link #newReadBuffer(InputStream)} /
 *       {@link #newReadBuffer(Path)} wrap an input stream with a
 *       {@link BufferedInputStream} sized by {@link AvroBufferConfig#readBufferSize()}
 *       ({@code avro.buffer.read.size}).</li>
 *   <li><b>Flush strategies</b> — the resolved {@link AvroBufferConfig.FlushStrategy}
 *       drives automatic draining: {@code SIZE} flushes a managed buffer as soon as its
 *       buffered payload reaches the write budget, {@code TIME} installs a background
 *       scheduler that flushes all managed buffers every
 *       {@code avro.buffer.flush.interval.ms}, and {@code FORCED} leaves draining to
 *       explicit {@link #flush(String)} / {@link #flushAll()} calls.</li>
 *   <li><b>Zero-copy reads</b> — {@link #zeroCopyReadBuffer(Path)} memory-maps a read
 *       only region via {@link FileChannel#map} (guarded by {@code avro.buffer.zero.copy})
 *       and {@link #wrapZeroCopy(byte[])} re-uses a caller buffer without copying the
 *       underlying array.</li>
 * </ul>
 *
 * <p>The manager is thread-safe: the write-buffer registry is a
 * {@link ConcurrentHashMap}, every {@link ManagedWriteBuffer} guards its own state with
 * a {@link ReentrantLock}, and all counters are {@link AtomicLong}. Closing the manager
 * shuts the time scheduler down and closes every managed write buffer.
 *
 * @since Prompt 78
 */
public final class AvroBufferManager implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroBufferManager.class);

    private final AvroBufferConfig config;
    private final Map<String, ManagedWriteBuffer> writeBuffers = new ConcurrentHashMap<>();
    private final AtomicLong totalFlushes = new AtomicLong();
    private final AtomicLong totalRowsFlushed = new AtomicLong();
    private final AtomicLong totalBytesFlushed = new AtomicLong();
    private final ScheduledExecutorService scheduler;
    private volatile boolean closed;

    /**
     * Creates a manager from the resolved {@link AvroBufferConfig#resolve()}.
     */
    public AvroBufferManager() {
        this(AvroBufferConfig.resolve());
    }

    /**
     * Creates a manager from an explicit configuration. When the strategy is
     * {@link AvroBufferConfig.FlushStrategy#TIME}, a daemon scheduler is started
     * immediately and flushes every managed write buffer every
     * {@code flushIntervalMs}.
     *
     * @param config the resolved buffer configuration (never {@code null})
     */
    public AvroBufferManager(AvroBufferConfig config) {
        this.config = config;
        if (config.flushStrategy() == AvroBufferConfig.FlushStrategy.TIME) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "avro-buffer-flusher");
                t.setDaemon(true);
                return t;
            });
            this.scheduler.scheduleAtFixedRate(
                    this::scheduledFlushAll,
                    config.flushIntervalMs(), config.flushIntervalMs(), TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
        }
    }

    // ─── Write side ───────────────────────────────────────────────

    /**
     * Creates and registers a byte-budgeted write buffer for an Avro output file.
     *
     * @param name         unique buffer name (used by {@link #getWriteBuffer(String)})
     * @param outputFile   target Avro object-container file
     * @param columns      column names in schema order
     * @param columnTypes  column name to Java type map
     * @return the registered managed buffer (never {@code null})
     * @throws IOException              if the underlying writer cannot be created
     * @throws IllegalArgumentException if a buffer with the same name is already open
     */
    public ManagedWriteBuffer createWriteBuffer(String name, File outputFile,
                                                List<String> columns,
                                                Map<String, Class<?>> columnTypes) throws IOException {
        return createWriteBuffer(name, outputFile, columns, columnTypes, null);
    }

    /**
     * Creates and registers a byte-budgeted write buffer with an explicit Avro codec.
     *
     * @param name         unique buffer name (used by {@link #getWriteBuffer(String)})
     * @param outputFile   target Avro object-container file
     * @param columns      column names in schema order
     * @param columnTypes  column name to Java type map
     * @param codec        the Avro {@link CodecFactory} (or {@code null} for the null codec)
     * @return the registered managed buffer (never {@code null})
     * @throws IOException              if the underlying writer cannot be created
     * @throws IllegalArgumentException if a buffer with the same name is already open
     */
    public ManagedWriteBuffer createWriteBuffer(String name, File outputFile,
                                                List<String> columns,
                                                Map<String, Class<?>> columnTypes,
                                                CodecFactory codec) throws IOException {
        ensureOpen();
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("buffer name must not be blank");
        }
        ManagedWriteBuffer existing = writeBuffers.get(name);
        if (existing != null && existing.isOpen()) {
            throw new IllegalArgumentException("a write buffer named '" + name + "' is already open");
        }
        ManagedWriteBuffer buffer = new ManagedWriteBuffer(name, outputFile, columns, columnTypes, codec);
        writeBuffers.put(name, buffer);
        return buffer;
    }

    /**
     * Returns the registered managed write buffer for {@code name}, or {@code null}.
     */
    public ManagedWriteBuffer getWriteBuffer(String name) {
        return writeBuffers.get(name);
    }

    /** Number of currently registered managed write buffers. */
    public int getWriteBufferCount() {
        return writeBuffers.size();
    }

    /**
     * Explicitly (forced) flushes the managed buffer named {@code name}.
     *
     * @throws IOException if the buffer is not found or the flush fails
     */
    public void flush(String name) throws IOException {
        ManagedWriteBuffer buffer = writeBuffers.get(name);
        if (buffer == null) {
            throw new IOException("no write buffer named '" + name + "'");
        }
        buffer.flush();
    }

    /**
     * Explicitly flushes every registered managed write buffer.
     */
    public void flushAll() throws IOException {
        for (ManagedWriteBuffer buffer : writeBuffers.values()) {
            buffer.flush();
        }
    }

    /**
     * Closes and unregisters the managed buffer named {@code name}.
     */
    public void closeWriteBuffer(String name) throws IOException {
        ManagedWriteBuffer buffer = writeBuffers.remove(name);
        if (buffer != null) {
            buffer.close();
        }
    }

    /**
     * Flushes and closes every registered managed write buffer, clearing the registry.
     */
    public void closeAllWriteBuffers() throws IOException {
        IOException first = null;
        for (Map.Entry<String, ManagedWriteBuffer> e : writeBuffers.entrySet()) {
            try {
                e.getValue().close();
            } catch (IOException ex) {
                if (first == null) {
                    first = ex;
                }
            }
        }
        writeBuffers.clear();
        if (first != null) {
            throw first;
        }
    }

    // ─── Read side ────────────────────────────────────────────────

    /**
     * Wraps an input stream with the configured read buffer size.
     *
     * @param in the raw stream to buffer
     * @return a {@link BufferedInputStream} of {@link AvroBufferConfig#readBufferSize()} bytes
     */
    public BufferedInputStream newReadBuffer(InputStream in) {
        if (in instanceof BufferedInputStream) {
            return (BufferedInputStream) in;
        }
        return new BufferedInputStream(in, config.readBufferSize());
    }

    /**
     * Opens a file and wraps it with the configured read buffer size.
     *
     * @param path the file to read
     * @return a buffered input stream over the file
     * @throws IOException if the file cannot be opened
     */
    public BufferedInputStream newReadBuffer(Path path) throws IOException {
        return newReadBuffer(Files.newInputStream(path));
    }

    /**
     * Zero-copy read: memory-maps a file region read-only via {@link FileChannel#map}.
     * The OS pages the file directly into memory, so decoded bytes are never copied
     * through an additional user-space buffer. Returns {@link Optional#empty()} when
     * zero-copy is disabled by {@code avro.buffer.zero.copy}.
     *
     * @param path     the file to map
     * @param position the mapping start offset
     * @param size     the number of bytes to map
     * @return the mapped buffer, or empty when zero-copy is disabled
     * @throws IOException if the file cannot be opened or mapped
     */
    public Optional<MappedByteBuffer> zeroCopyReadBuffer(Path path, long position, long size) throws IOException {
        if (!config.zeroCopyEnabled()) {
            return Optional.empty();
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long mapSize = Math.max(0L, Math.min(size, channel.size() - Math.max(0L, position)));
            if (mapSize == 0L) {
                return Optional.of(channel.map(FileChannel.MapMode.READ_ONLY, Math.max(0L, position), 0L));
            }
            return Optional.of(channel.map(FileChannel.MapMode.READ_ONLY, Math.max(0L, position), mapSize));
        }
    }

    /**
     * Zero-copy read of the whole file. See {@link #zeroCopyReadBuffer(Path, long, long)}.
     */
    public Optional<MappedByteBuffer> zeroCopyReadBuffer(Path path) throws IOException {
        if (!config.zeroCopyEnabled()) {
            return Optional.empty();
        }
        return zeroCopyReadBuffer(path, 0L, Long.MAX_VALUE);
    }

    /**
     * Copy-free wrap: returns a {@link ByteBuffer} backed by {@code data} without
     * re-allocating or copying the array. Zero-copy data-pump helper for reusable
     * row buffers.
     *
     * @param data the byte array to back the buffer
     * @return a heap buffer sharing {@code data}
     */
    public ByteBuffer wrapZeroCopy(byte[] data) {
        return ByteBuffer.wrap(data);
    }

    // ─── Statistics ───────────────────────────────────────────────

    /** Total flush events performed on managed buffers since manager creation. */
    public long getTotalFlushes() {
        return totalFlushes.get();
    }

    /** Total rows flushed to disk since manager creation. */
    public long getTotalRowsFlushed() {
        return totalRowsFlushed.get();
    }

    /** Total estimated payload bytes flushed since manager creation. */
    public long getTotalBytesFlushed() {
        return totalBytesFlushed.get();
    }

    /** The resolved configuration of this manager. */
    public AvroBufferConfig getConfig() {
        return config;
    }

    /** Whether the manager has been {@link #close() closed}. */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Shuts the time scheduler down and closes every managed write buffer.
     * Idempotent: subsequent calls are no-ops and {@link #createWriteBuffer} throws.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        closeAllWriteBuffers();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("AvroBufferManager is closed");
        }
    }

    private void scheduledFlushAll() {
        try {
            flushAll();
        } catch (IOException e) {
            LOGGER.warn("Scheduled AVRO buffer flush failed: {}", e.getMessage());
        }
    }

    /**
     * Estimates the encoded payload bytes of a single {@code Map} row. String values
     * use their UTF-8 length, scalar primitives use their fixed wire width, arrays and
     * maps recurse; unknown types get a conservative fixed estimate.
     */
    static int estimateRowBytes(Map<String, Object> row) {
        int total = 0;
        for (Object value : row.values()) {
            total += estimateScalarBytes(value);
        }
        return total;
    }

    private static int estimateScalarBytes(Object value) {
        if (value == null) {
            return 1;
        }
        if (value instanceof String s) {
            return s.getBytes(StandardCharsets.UTF_8).length + 1;
        }
        if (value instanceof byte[] bytes) {
            return bytes.length + 8;
        }
        if (value instanceof ByteBuffer bb) {
            return bb.remaining() + 8;
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Float) {
            return 4;
        }
        if (value instanceof Long || value instanceof Double) {
            return 8;
        }
        if (value instanceof Boolean) {
            return 1;
        }
        if (value instanceof Map<?, ?> map) {
            int sum = 0;
            for (Map.Entry<?, ?> e : map.entrySet()) {
                sum += estimateScalarBytes(e.getKey()) + estimateScalarBytes(e.getValue());
            }
            return sum;
        }
        if (value instanceof Collection<?> col) {
            int sum = 0;
            for (Object o : col) {
                sum += estimateScalarBytes(o);
            }
            return sum;
        }
        if (value instanceof Object[] arr) {
            int sum = 0;
            for (Object o : arr) {
                sum += estimateScalarBytes(o);
            }
            return sum;
        }
        return 16;
    }

    /**
     * A registered write buffer that batches {@code Map} rows against an underlying
     * {@link AvroDataFileWriter}. Rows are held in memory until the byte budget
     * ({@code AvroBufferConfig.writeBufferSize()}) or an explicit flush drains them.
     * <p>Note: this class is an inner class so it can update the owning manager's
     * flush counters; it is not independently constructible.
     */
    public final class ManagedWriteBuffer implements AutoCloseable {

        private final String name;
        private final AvroDataFileWriter writer;
        private final int threshold;
        private final List<Map<String, Object>> buffer;
        private final ReentrantLock lock = new ReentrantLock();
        private int bufferedBytes;
        private boolean open = true;

        private ManagedWriteBuffer(String name, File outputFile,
                                   List<String> columns, Map<String, Class<?>> columnTypes,
                                   CodecFactory codec) throws IOException {
            this.name = name;
            this.threshold = AvroBufferManager.this.config.writeBufferSize();
            this.buffer = new ArrayList<>(Math.min(threshold, 4096));
            this.writer = codec != null
                    ? new AvroDataFileWriter(columns, columnTypes, outputFile, codec)
                    : new AvroDataFileWriter(columns, columnTypes, outputFile);
        }

        /** The registered buffer name. */
        public String getName() {
            return name;
        }

        /** The configured byte budget that triggers an automatic drain under the SIZE strategy. */
        public int getThreshold() {
            return threshold;
        }

        /** Number of rows currently sitting in memory. */
        public int getBufferedRows() {
            lock.lock();
            try {
                return buffer.size();
            } finally {
                lock.unlock();
            }
        }

        /** Estimated payload bytes currently sitting in memory. */
        public int getBufferedBytes() {
            lock.lock();
            try {
                return bufferedBytes;
            } finally {
                lock.unlock();
            }
        }

        /** Flush flag combining the SIZE threshold with the manager's implicit semantics. */
        private boolean autoFlushDue() {
            return bufferedBytes >= threshold
                    && AvroBufferManager.this.config.flushStrategy() == AvroBufferConfig.FlushStrategy.SIZE;
        }

        /**
         * Buffers a row, then applies the automatic flush policy: under the
         * {@code size} strategy a write is drained as soon as the buffered bytes
         * reach the configured budget.
         *
         * @param row the row to buffer
         * @throws IOException if the managed buffer or manager is closed
         */
        public void writeRow(Map<String, Object> row) throws IOException {
            ensureOpen();
            lock.lock();
            try {
                buffer.add(row);
                bufferedBytes += estimateRowBytes(row);
                if (autoFlushDue()) {
                    flush();
                }
            } finally {
                lock.unlock();
            }
        }

        /**
         * Explicitly flushes the buffered rows to the underlying writer (forced flush).
         *
         * @throws IOException if the flush fails
         */
        public void flush() throws IOException {
            lock.lock();
            try {
                if (buffer.isEmpty() || !open) {
                    return;
                }
                int rows = buffer.size();
                for (Map<String, Object> row : buffer) {
                    writer.writeRow(row);
                }
                int bytes = bufferedBytes;
                buffer.clear();
                bufferedBytes = 0;
                writer.flush();
                totalFlushes.incrementAndGet();
                totalRowsFlushed.addAndGet(rows);
                totalBytesFlushed.addAndGet(bytes);
            } finally {
                lock.unlock();
            }
        }

        /**
         * Flushes and closes the underlying writer. Idempotent.
         */
        @Override
        public void close() throws IOException {
            lock.lock();
            try {
                if (!open) {
                    return;
                }
                flush();
                writer.close();
                open = false;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Discards the pending rows and rolls the underlying writer back, deleting
         * the output file. After rollback the buffer is closed.
         */
        public void rollback() throws IOException {
            lock.lock();
            try {
                if (!open) {
                    return;
                }
                buffer.clear();
                bufferedBytes = 0;
                writer.rollback();
                open = false;
            } finally {
                lock.unlock();
            }
        }

        /** Whether this buffer can still accept rows. */
        public boolean isOpen() {
            lock.lock();
            try {
                return open;
            } finally {
                lock.unlock();
            }
        }

        /** The underlying Avro writer (exposed for low-level callers). */
        public AvroDataFileWriter getWriter() {
            return writer;
        }

        private void ensureOpen() throws IOException {
            if (!open) {
                throw new IOException("ManagedWriteBuffer '" + name + "' is closed");
            }
            AvroBufferManager.this.ensureOpen();
        }
    }
}