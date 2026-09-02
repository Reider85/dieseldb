package diesel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Server-side cursor for paginated query results (Prompt 81).
 * Maintains an iterator over a fully-materialized result set, delivering
 * rows in batches of {@code fetchSize} on each {@link #fetch()} call.
 * Cursors are session-scoped: they live in the {@code ClientHandler}
 * and are removed on explicit close or client disconnect.
 *
 * <p>Supported pagination patterns:
 * <ul>
 *   <li><b>Server-side cursors</b> – the client fetches rows in batches of N</li>
 *   <li><b>Keyset pagination</b> – {@code WHERE id > last_seen_id LIMIT N}
 *       (client-side, no cursor needed)</li>
 *   <li><b>Stateless pagination</b> – {@code OFFSET/LIMIT} (client-side)</li>
 * </ul>
 */
final class Cursor {
    private static final Logger LOGGER = Logger.getLogger(Cursor.class.getName());

    private final UUID id;
    private final String query;
    private final int fetchSize;
    private final Iterator<Map<String, Object>> iterator;
    private boolean closed;
    private long totalFetched;

    Cursor(UUID id, String query, int fetchSize, Iterator<Map<String, Object>> iterator) {
        this.id = id;
        this.query = query;
        this.fetchSize = fetchSize;
        this.iterator = iterator;
        this.closed = false;
        this.totalFetched = 0;
        LOGGER.fine("Cursor opened: " + id + " for query: " + query + " fetchSize=" + fetchSize);
    }

    UUID getId() {
        return id;
    }

    String getQuery() {
        return query;
    }

    int getFetchSize() {
        return fetchSize;
    }

    boolean isClosed() {
        return closed;
    }

    long getTotalFetched() {
        return totalFetched;
    }

    /**
     * Returns {@code true} when the cursor still has un-fetched rows.
     */
    boolean hasNext() {
        return !closed && iterator.hasNext();
    }

    /**
     * Fetches the next batch of up to {@code fetchSize} rows from the
     * underlying iterator. Returns an empty list when exhausted or closed.
     *
     * @return up to {@code fetchSize} rows
     */
    synchronized List<Map<String, Object>> fetch() {
        if (closed) {
            LOGGER.fine("Fetch on closed cursor: " + id);
            return List.of();
        }
        List<Map<String, Object>> batch = new ArrayList<>(fetchSize);
        int count = 0;
        while (count < fetchSize && iterator.hasNext()) {
            batch.add(iterator.next());
            count++;
        }
        totalFetched += batch.size();
        if (!iterator.hasNext()) {
            LOGGER.fine("Cursor " + id + " exhausted after " + totalFetched + " total rows");
        }
        return batch;
    }

    /**
     * Closes this cursor, releasing the underlying iterator.
     * Subsequent {@link #fetch()} calls return an empty list.
     */
    synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        LOGGER.fine("Cursor closed: " + id + " (totalFetched=" + totalFetched + ")");
    }
}
