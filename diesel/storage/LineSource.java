package diesel.storage;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Line provider abstraction for delimited row readers (prompt 43). A reader
 * sees a uniform {@link #nextLine()} stream regardless of whether lines come
 * from a live {@link BufferedReader} (streaming path) or from an in-memory
 * list of already-split physical lines (byte-array fast path). Closing the
 * source closes the underlying stream, or is a no-op for in-memory lines.
 */
public final class LineSource implements AutoCloseable {

    private final BufferedReader reader;
    private final Iterator<String> lines;

    private LineSource(BufferedReader reader, Iterator<String> lines) {
        this.reader = reader;
        this.lines = lines;
    }

    /** Wraps a buffered character stream. Closing this source closes the stream. */
    public static LineSource over(BufferedReader reader) {
        return new LineSource(reader, null);
    }

    /** Wraps an in-memory list of physical lines (no resources to close). */
    public static LineSource over(List<String> lines) {
        return new LineSource(null, lines.iterator());
    }

    /**
     * Returns the next line without its line terminator, or {@code null} when
     * the stream is exhausted. Behaves like {@link BufferedReader#readLine()}.
     */
    public String nextLine() throws IOException {
        if (reader != null) {
            return reader.readLine();
        }
        return lines.hasNext() ? lines.next() : null;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}