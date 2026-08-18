package diesel;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cache of parsed SELECT plans keyed by the normalized, literal-free SQL
 * structure (see {@link #normalize}). The parsed {@link Query} AST is the
 * executable "plan" of this engine (join structure, WHERE conditions, grouping,
 * ordering are all decided at parse time), so a cache hit skips the whole
 * parse phase for an identical repeated statement.
 *
 * <p>Two correctness guards protect the cached plan:
 * <ul>
 *   <li><b>Literal signature.</b> This engine has no parameter binding, so all
 *       literal values are inline. Reusing a plan across queries that differ
 *       only in their values would return stale data, therefore every cache
 *       entry records the exact literal values the plan was built from and a
 *       lookup only hits when the incoming literals match exactly. Queries with
 *       the same structure but different values are re-parsed (and replace the
 *       entry).</li>
 *   <li><b>Schema epoch.</b> The database bumps {@link Database}'s schema epoch
 *       on every DDL operation (CREATE/DROP TABLE, CREATE INDEX) and on
 *       {@code ANALYZE TABLE}, and a stale entry is evicted on the next lookup.
 *       Data mutations (INSERT/UPDATE/DELETE) deliberately do <em>not</em>
 *       invalidate the cache: a cached SELECT resolves the table and its
 *       statistics lazily at execution time, so a repeated statement always
 *       observes the current rows.</li>
 * </ul>
 *
 * <p>Only plain SELECT statements are cached: EXPLAIN, DDL, DML, transaction
 * commands, derived tables (which materialize their inner SELECT at parse
 * time) and queries carrying a MAX_ROWS hint are all excluded. Reusing one
 * parsed SELECT across concurrent threads is not guaranteed (per-execution
 * caches inside {@link SelectQuery} are cleared at the start of
 * {@link SelectQuery#execute}), matching the engine's single-writer posture.
 *
 * <p>Metrics track the cache effectiveness: hit/miss counters, the hit rate and
 * the (average) parse time saved per hit. {@link Database} exposes the cache
 * via {@link Database#getQueryCache}.
 *
 * @see Database
 * @see SelectQuery
 */
class QueryCache {

    /**
     * The normalized literal-free structural key plus the ordered list of the
     * literal values that produced it. Two SQL strings with the same key have
     * the same parse-time structure; they may still carry different values.
     */
    static final class NormalizedSql {
        final String key;
        final List<String> literals;

        NormalizedSql(String key, List<String> literals) {
            this.key = key;
            this.literals = literals;
        }
    }

    /** A cached entry: the parsed plan, its literal signature, epoch, and the parse duration it saved on a hit. */
    private static final class Entry {
        final Query<?> query;
        final List<String> literals;
        final long epoch;
        final long parseTimeNanos;

        Entry(Query<?> query, List<String> literals, long epoch, long parseTimeNanos) {
            this.query = query;
            this.literals = literals;
            this.epoch = epoch;
            this.parseTimeNanos = parseTimeNanos;
        }
    }

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();
    private final AtomicLong schemaEpoch = new AtomicLong();
    private final AtomicLong hitCount = new AtomicLong();
    private final AtomicLong missCount = new AtomicLong();
    private final AtomicLong parseTimeSavedNanos = new AtomicLong();
    private final AtomicLong parseTimeSpentNanos = new AtomicLong();

    /** @return the current schema epoch, bumped by {@link #invalidateAll} */
    long currentEpoch() {
        return schemaEpoch.get();
    }

    /**
     * Invalidates the whole cache: bumps the schema epoch and drops every
     * entry. Called by {@link Database} on DDL, ANALYZE TABLE and drops.
     */
    void invalidateAll() {
        schemaEpoch.incrementAndGet();
        entries.clear();
    }

    /** @return the number of cached plans */
    int size() {
        return entries.size();
    }

    long getHitCount() {
        return hitCount.get();
    }

    long getMissCount() {
        return missCount.get();
    }

    long getParseTimeSavedNanos() {
        return parseTimeSavedNanos.get();
    }

    long getParseTimeSpentNanos() {
        return parseTimeSpentNanos.get();
    }

    /** @return hit rate in [0, 1], or 0 when nothing has been looked up yet */
    double getHitRate() {
        long total = hitCount.get() + missCount.get();
        return total == 0 ? 0.0 : (double) hitCount.get() / total;
    }

    /** @return average parse time saved per cache hit, in nanoseconds */
    long getAverageParseTimeSavedNanos() {
        long hits = hitCount.get();
        return hits == 0 ? 0 : parseTimeSavedNanos.get() / hits;
    }

    /** @return a compact human-readable metrics summary */
    String getSummary() {
        return "QueryCache{size=" + entries.size()
                + ", hits=" + hitCount.get() + ", misses=" + missCount.get()
                + ", hitRate=" + String.format(Locale.ROOT, "%.4f", getHitRate())
                + ", parseTimeSavedMs=" + (parseTimeSavedNanos.get() / 1_000_000)
                + ", avgParseTimeSavedMs=" + String.format(Locale.ROOT, "%.6f", getAverageParseTimeSavedNanos() / 1_000_000.0)
                + "}";
    }

    /**
     * Looks up the normalized SQL and returns the cached plan when the literal
     * signature matches and the entry is from the current schema epoch; a
     * stale (evicted) or value-mismatched entry counts as a miss. A structural
     * miss or an eviction returns null.
     *
     * @param normalized   the normalized key and literal values
     * @param currentEpoch the database's current schema epoch
     * @return the cached plan, or null on a miss
     */
    Query<?> get(NormalizedSql normalized, long currentEpoch) {
        Entry entry = entries.get(normalized.key);
        if (entry == null) {
            missCount.incrementAndGet();
            return null;
        }
        if (entry.epoch != currentEpoch) {
            missCount.incrementAndGet();
            entries.remove(normalized.key, entry);
            return null;
        }
        if (!entry.literals.equals(normalized.literals)) {
            missCount.incrementAndGet();
            return null;
        }
        hitCount.incrementAndGet();
        parseTimeSavedNanos.addAndGet(entry.parseTimeNanos);
        return entry.query;
    }

    /**
     * Stores a freshly parsed plan under its normalized key.
     *
     * @param normalized     the normalized key and literal values
     * @param query          the parsed plan
     * @param parseTimeNanos how long parsing took (recorded as the saved time
     *                       on future hits and as the spent time for metrics)
     * @param currentEpoch   the database's current schema epoch
     */
    void put(NormalizedSql normalized, Query<?> query, long parseTimeNanos, long currentEpoch) {
        entries.put(normalized.key, new Entry(query, normalized.literals, currentEpoch, parseTimeNanos));
        parseTimeSpentNanos.addAndGet(parseTimeNanos);
    }

    /**
     * Builds the literal-free structural cache key and the ordered literal
     * signature of a SQL statement using {@link SqlLexer}: keywords are
     * already uppercased by the lexer, unquoted identifiers are uppercased,
     * quoted identifiers keep their exact case (wrapped in double quotes so a
     * quoted {@code "Name"} never collides with the unquoted {@code NAME}), and
     * every integer, decimal and string literal is replaced by a {@code ?}
     * marker whose actual text is appended to the literal signature. The SQL
     * literals TRUE/FALSE/NULL carry semantic meaning and stay in the key.
     * A trailing semicolon is dropped so {@code SELECT ...} and
     * {@code SELECT ...;} share one entry.
     *
     * @param sql the SQL statement
     * @return the normalized key and literal values
     * @throws IllegalArgumentException if the statement cannot be tokenized
     */
    static NormalizedSql normalize(String sql) {
        List<SqlLexer.Token> tokens = trimTrailingSemicolons(new SqlLexer().tokenize(sql));
        StringBuilder key = new StringBuilder();
        List<String> literals = new ArrayList<>();
        for (SqlLexer.Token token : tokens) {
            switch (token.type) {
                case KEYWORD -> key.append(token.value);
                case IDENTIFIER -> key.append(token.value.toUpperCase());
                case QUOTED_IDENTIFIER -> key.append('"').append(token.value).append('"');
                case INTEGER, DECIMAL, STRING_LITERAL -> {
                    key.append('?');
                    literals.add(token.value);
                }
                case LITERAL -> key.append(token.value);
                case COMPARISON_OPERATOR, PUNCTUATION -> key.append(token.value);
                default -> { /* exhaustive enum switch - all TokenType values covered */ }
            }
            key.append(' ');
        }
        return new NormalizedSql(key.toString().trim(), literals);
    }

    private static List<SqlLexer.Token> trimTrailingSemicolons(List<SqlLexer.Token> tokens) {
        int end = tokens.size();
        while (end > 0 && tokens.get(end - 1).type == SqlLexer.TokenType.PUNCTUATION
                && tokens.get(end - 1).value.equals(";")) {
            end--;
        }
        return tokens.subList(0, end);
    }
}
