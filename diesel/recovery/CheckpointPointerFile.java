package diesel.recovery;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import diesel.storage.page.AtomicFileWriter;

/**
 * Manages checkpoint.ptr file with atomic write semantics.
 * Format: 8-byte big-endian LSN (backward compatible with legacy files).
 */
public final class CheckpointPointerFile {
    private static final int SIZE = 8; // bytes
    private final Path ptrFile;

    public CheckpointPointerFile(Path walDir) {
        this.ptrFile = walDir.resolve("checkpoint.ptr");
    }

    /**
     * Atomically writes the LSN of the last checkpoint record.
     * Uses AtomicFileWriter for crash-safe temp+rename.
     */
    public void write(long checkpointLsn) throws IOException {
        byte[] bytes = new byte[SIZE];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(checkpointLsn);
        AtomicFileWriter.writeNewFileAtomically(ptrFile, bytes);
    }

    /**
     * Reads the checkpoint LSN.
     * @return LSN if valid, 0 if missing/invalid (legacy fallback).
     */
    public long read() throws IOException {
        if (!Files.exists(ptrFile)) {
            return 0;
        }

        byte[] bytes = Files.readAllBytes(ptrFile);
        if (bytes.length != SIZE) {
            // Legacy files might have different size, treat as invalid
            return 0;
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getLong();
    }

    /**
     * Returns the path to the checkpoint.ptr file.
     */
    public Path getPath() {
        return ptrFile;
    }
}