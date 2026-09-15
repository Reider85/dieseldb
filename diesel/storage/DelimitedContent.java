package diesel.storage;

import diesel.DieselIOException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Coarse-grained fast path for delimited files (prompt 43): reads the whole
 * file bytes in one shot, decodes them once with the REPORT charset decoder
 * (matching {@link CompressionFactory#openDelimitedReader}) and splits the
 * physical lines exactly like {@link java.io.BufferedReader#readLine()}, then
 * streams the rows from the in-memory line list. This removes the per-line
 * read/decode overhead of the streaming path without changing the logical
 * line sequence, so multi-line quoted CSV rows still decode identically.
 */
final class DelimitedContent {

    private DelimitedContent() {
        throw new AssertionError("No instances");
    }

    /** Reads the whole (optionally decompressed) file into memory. */
    static byte[] readAllBytes(File file, CompressionCodec codec) throws IOException {
        try (var in = CompressionFactory.openDelimitedInputStream(file, codec)) {
            return in.readAllBytes();
        }
    }

    /**
     * Decodes file bytes with the same {@link CodingErrorAction#REPORT} semantics
     * as {@link CompressionFactory#openDelimitedReader}, so malformed input bytes
     * fail loudly instead of being silently replaced. The offending file is
     * named in the error message.
     */
    static String decode(byte[] bytes, Charset charset, File file) {
        CharsetDecoder decoder = charset.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(bytes)).toString();
        } catch (java.nio.charset.CharacterCodingException e) {
            throw new DieselIOException(
                    "Malformed " + charset.name() + " input in delimited file " + file.getPath(), e);
        }
    }

    /**
     * Splits a decoded file into physical lines exactly like
     * {@link java.io.BufferedReader#readLine()}: {@code \n}, {@code \r\n} and
     * lone {@code \r} terminators are stripped, no phantom line is produced
     * after a trailing terminator, and a final unterminated line is kept.
     *
     * <p>Implementation note: the scan uses the intrinsified
     * {@link String#indexOf(int, int)} for both {@code \n} and {@code \r},
     * which HotSpot lowers to a vector scan over the underlying char[].
     * This replaces the previous char-by-char branch loop and is
     * memory-bandwidth-bound instead of ALU-bound; on multi-MB CSV/TSV files
     * the split pass becomes a single cache-resident scan rather than
     * per-char work.
     */
    static List<String> splitLines(String text) {
        List<String> lines = new ArrayList<>();
        int len = text.length();
        int start = 0;
        while (start <= len) {
            int nl = text.indexOf('\n', start);
            int cr = text.indexOf('\r', start);
            int next;
            char termChar;
            if (nl < 0 && cr < 0) {
                if (start < len) {
                    lines.add(text.substring(start));
                }
                return lines;
            }
            if (nl < 0) {
                next = cr;
                termChar = '\r';
            } else if (cr < 0) {
                next = nl;
                termChar = '\n';
            } else if (nl < cr) {
                next = nl;
                termChar = '\n';
            } else {
                next = cr;
                termChar = '\r';
            }
            lines.add(text.substring(start, next));
            if (termChar == '\r' && next + 1 < len && text.charAt(next + 1) == '\n') {
                start = next + 2;
            } else {
                start = next + 1;
            }
        }
        return lines;
    }

    /**
     * Fast path: decodes the whole file into physical lines and streams every
     * data row through the given reader factory. Used for plain files whose
     * bytes can be read fully and decoded once.
     *
     * @return the decoded rows in file order (already compact Object[] rows)
     */
    static List<Object[]> readAllArrays(File file, CompressionCodec codec, List<String> columns,
                                        Map<String, Class<?>> columnTypes, RowReaderFactory factory,
                                        Charset charset) {
        byte[] bytes;
        try {
            bytes = readAllBytes(file, codec);
        } catch (IOException e) {
            throw new DieselIOException("Failed to read delimited file: " + file.getPath(), e);
        }
        String text = decode(bytes, charset, file);
        List<String> lines = splitLines(text);
        try (LineSource source = LineSource.over(lines);
             DelimitedRowReader reader = factory.create(source, columns, columnTypes)) {
            reader.readHeader();
            List<Object[]> rows = new ArrayList<>(Math.max(16, lines.size()));
            while (reader.hasNext()) {
                Object[] row = reader.nextArray();
                if (row != null) {
                    rows.add(row);
                }
            }
            return rows;
        } catch (IOException e) {
            throw new DieselIOException("Failed to read delimited file: " + file.getPath(), e);
        }
    }
}