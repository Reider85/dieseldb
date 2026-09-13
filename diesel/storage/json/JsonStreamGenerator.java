package diesel.storage.json;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * Library-neutral streaming JSON writer (prompt 42). The storage package writes
 * JSONL through this interface only; it never imports a JSON library directly.
 *
 * <p>Every write method throws {@link IOException} on a closed or failing
 * underlying stream. {@link #writeRaw(char)} writes a single character
 * verbatim (no JSON escaping) - used to reproduce non-structured text such as
 * escaped-UTF16 sequences during schema sidecar round-trips.
 */
public interface JsonStreamGenerator extends Closeable, Flushable {

    /** Opens an object; content must be closed with {@link #writeEndObject()}. */
    void writeStartObject() throws IOException;

    /** Closes the innermost open object. */
    void writeEndObject() throws IOException;

    /** Opens an array; content must be closed with {@link #writeEndArray()}. */
    void writeStartArray() throws IOException;

    /** Closes the innermost open array. */
    void writeEndArray() throws IOException;

    /** Emits a field name (must be called inside an open object). */
    void writeFieldName(String name) throws IOException;

    /** Writes a string value (quoted and escaped). */
    void writeString(String value) throws IOException;

    /** Writes a boolean value. */
    void writeBoolean(boolean value) throws IOException;

    /** Writes the JSON {@code null} literal. */
    void writeNull() throws IOException;

    /** Writes an integer number. */
    void writeNumber(int value) throws IOException;

    /** Writes a long number. */
    void writeNumber(long value) throws IOException;

    /** Writes a single-precision number (shortest exact representation). */
    void writeNumber(float value) throws IOException;

    /** Writes a floating-point number. */
    void writeNumber(double value) throws IOException;

    /** Writes a decimal number with arbitrary precision (kept verbatim). */
    void writeNumber(BigDecimal value) throws IOException;

    /** Writes a single character verbatim, without any JSON analysis. */
    void writeRaw(char c) throws IOException;

    /**
     * Copies the current value of {@code source} into this generator. The
     * source is positioned at a START_OBJECT/START_ARRAY (or a scalar), and
     * after the call is positioned just past the matching END_* event. Backends
     * may special-case their own parser type for a byte-identical copy.
     */
    void copyCurrentStructure(JsonStreamParser source) throws IOException;

    @Override
    void close() throws IOException;
}