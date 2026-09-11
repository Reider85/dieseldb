package diesel.storage;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Crash-safe file writer using the temp + fsync + atomic-rename pattern
 * (Prompt 30).
 *
 * <p>Data is written to a {@code <target>.tmp} sibling file, then
 * {@link FileChannel#force(boolean) force(true)} (fsync) is applied, and
 * finally the temporary file is moved over the target with
 * {@link Files#move(Path, Path, java.nio.file.CopyOption...)
 * ATOMIC_MOVE + REPLACE_EXISTING}. A crash at any point leaves either the
 * previous valid target or a leftover {@code .tmp} file - never a truncated
 * target.
 *
 * <p>Usage (text):
 * <pre>{@code
 * try (AtomicFileWriter afw = AtomicFileWriter.openText(target)) {
 *     BufferedWriter writer = afw.bufferedWriter();
 *     // write content...
 *     afw.commit(); // fsync + atomic rename
 * }
 * }</pre>
 *
 * <p>Usage (binary, e.g. Java-serialised {@code .table} files):
 * <pre>{@code
 * try (AtomicFileWriter afw = AtomicFileWriter.openBinary(target)) {
 *     ObjectOutputStream oos = new ObjectOutputStream(afw.outputStream());
 *     oos.writeObject(payload);
 *     oos.flush();
 *     afw.commit();
 * }
 * }</pre>
 * Note: the serialisation stream itself must NOT be closed after {@link #commit()},
 * because {@link #commit()} already flushed and closed the underlying channel; the
 * {@link AtomicFileWriter} owns the channel lifecycle.
 *
 * <p>If {@link #commit()} is never invoked, {@link #close()} discards the
 * temporary file and leaves the previous target version untouched.
 */
public final class AtomicFileWriter implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(AtomicFileWriter.class.getName());

    private final Path target;
    private final Path tmp;
    private final FileChannel channel;
    private BufferedWriter bufferedWriter;
    private OutputStream outputStream;
    private boolean committed;

    private AtomicFileWriter(Path target, boolean text) throws IOException {
        this.target = target;
        this.tmp = tmpPath(target);
        this.channel = FileChannel.open(tmp,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        if (text) {
            this.bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(Channels.newOutputStream(channel), StorageConfig.getCharset()));
        } else {
            this.outputStream = Channels.newOutputStream(channel);
        }
    }

    /** Opens a text writer over the given target using the configured charset. */
    public static AtomicFileWriter openText(Path target) throws IOException {
        return new AtomicFileWriter(target, true);
    }

    /** Opens a text writer over the given target using the configured charset. */
    public static AtomicFileWriter openText(File target) throws IOException {
        return openText(target.toPath());
    }

    /** Opens a binary writer over the given target. */
    public static AtomicFileWriter openBinary(Path target) throws IOException {
        return new AtomicFileWriter(target, false);
    }

    /** Opens a binary writer over the given target. */
    public static AtomicFileWriter openBinary(File target) throws IOException {
        return openBinary(target.toPath());
    }

    /** Returns the buffered text writer this writer writes into. */
    public BufferedWriter bufferedWriter() {
        return bufferedWriter;
    }

    /** Returns the binary output stream this writer writes into. */
    public OutputStream outputStream() {
        return outputStream;
    }

    /**
     * Returns the temporary file path used for the given target, i.e.
     * {@code <target>.tmp}.
     */
    public static Path tmpPath(Path target) {
        return target.resolveSibling(target.getFileName() + ".tmp");
    }

    /**
     * Logs a WARNING if the target file is missing but its {@code .tmp} sibling
     * exists, signalling an interrupted (uncommitted) write.
     */
    public static void warnInterruptedWrite(Path target) {
        Path tmp = tmpPath(target);
        if (!Files.exists(target) && Files.exists(tmp)) {
            LOGGER.log(Level.WARNING,
                    "Interrupted write detected: target {0} is missing but temp file {1} exists; "
                            + "the previous version may have been lost",
                    new Object[]{target, tmp});
        }
    }

    /**
     * Flushes buffered data, forces the file to disk (fsync), closes the channel
     * and atomically renames the temporary file over the target. The target is
     * only replaced after the new content is durable, so a crash cannot truncate
     * it. A no-op when already committed.
     */
    public void commit() throws IOException {
        if (committed) {
            return;
        }
        if (bufferedWriter != null) {
            bufferedWriter.flush();
        } else {
            outputStream.flush();
        }
        channel.force(true);
        closeChannel();
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
        committed = true;
    }

    /**
     * {@inheritDoc}
     *
     * <p>When {@link #commit()} has been called this is a no-op; otherwise the
     * temporary file is discarded and the previous target version is left intact.
     */
    @Override
    public void close() throws IOException {
        if (committed) {
            return;
        }
        closeChannel();
        Files.deleteIfExists(tmp);
        LOGGER.log(Level.FINE, "Discarded uncommitted temp file {0}", tmp);
    }

    /**
     * Closes the channel and clears the wrappers so later closes become no-ops.
     */
    private void closeChannel() throws IOException {
        if (bufferedWriter != null) {
            bufferedWriter.close();
            bufferedWriter = null;
        }
        if (outputStream != null) {
            outputStream.close();
            outputStream = null;
        } else {
            channel.close();
        }
    }
}