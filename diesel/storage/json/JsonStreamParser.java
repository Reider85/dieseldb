package diesel.storage.json;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * Library-neutral forward-only streaming JSON parser (prompt 42). The storage
 * package reads JSONL through this interface only; it never imports a JSON
 * library directly.
 *
 * <p>Typical consumption loop:
 * <pre>
 * while (parser.nextToken() != JsonEvent.END_INPUT) {
 *     switch (parser.currentEvent()) { ... }
 * }
 * </pre>
 * A {@link JsonEvent#FIELD_NAME} event is always followed by the value event
 * whose name is reported by {@link #currentName()}.
 */
public interface JsonStreamParser extends Closeable {

    /**
     * Advances to the next event.
     *
     * @return the next {@link JsonEvent} (never {@code null} after the first
     *         call), or {@link JsonEvent#END_INPUT} once the stream is exhausted
     * @throws JsonStreamException on malformed JSON or a violated limit
     * @throws IOException         on underlying I/O errors
     */
    JsonEvent nextToken() throws IOException;

    /** Returns the current event (only meaningful after a {@link #nextToken()} call). */
    JsonEvent currentEvent();

    /** Returns the field name of the current value event (after FIELD_NAME), or {@code null}. */
    String currentName();

    /**
     * Textual value of the current event: string, number literal or scalar
     * (true/false/null return their canonical literals).
     */
    String getText() throws IOException;

    /** Integral value of the current VALUE_NUMBER_INT event. */
    long getLongValue() throws IOException;

    /** Integral value of the current VALUE_NUMBER_INT event with arbitrary precision. */
    BigDecimal getDecimalValue() throws IOException;

    /**
     * Skips all children of the current START_OBJECT/START_ARRAY, leaving the
     * parser positioned at the matching END_* event. No-op for scalar events.
     */
    void skipChildren() throws IOException;

    @Override
    void close() throws IOException;
}