package diesel.format;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Configuration for a format read operation. Held by the engine when it asks
 * a {@link TableFormat} to read a file: the set of columns to materialize
 * (projection pushdown), a list of {@link ColumnPredicate} for predicate
 * pushdown, an optional row limit, and opaque format-specific options.
 *
 * <p>All fields are immutable; use {@link #withProjection(List)} and friends
 * to derive variations.</p>
 */
public final class ReadOptions {

    /** No constraints: read everything. */
    public static final ReadOptions DEFAULT = new ReadOptions(null, List.of(), -1, Map.of());

    private final List<String> projection;
    private final List<ColumnPredicate> pushdownPredicates;
    private final long limit;
    private final Map<String, String> formatOptions;

    /**
     * Creates read options.
     *
     * @param projection         columns to materialize, or {@code null}/{@code empty} for all
     * @param pushdownPredicates predicates for file-level filtering, may be empty
     * @param limit              maximum number of rows, or negative for no limit
     * @param formatOptions      format-specific hints, may be empty
     */
    public ReadOptions(List<String> projection,
                       List<ColumnPredicate> pushdownPredicates,
                       long limit,
                       Map<String, String> formatOptions) {
        this.projection = projection == null || projection.isEmpty()
                ? null : List.copyOf(projection);
        this.pushdownPredicates = pushdownPredicates == null
                ? List.of() : List.copyOf(pushdownPredicates);
        this.limit = limit;
        this.formatOptions = formatOptions == null
                ? Map.of() : Collections.unmodifiableMap(formatOptions);
    }

    public List<String> getProjection() {
        return projection;
    }

    public List<ColumnPredicate> getPushdownPredicates() {
        return pushdownPredicates;
    }

    public long getLimit() {
        return limit;
    }

    public Map<String, String> getFormatOptions() {
        return formatOptions;
    }

    /**
     * Returns whether a projection was requested (non-empty column list).
     *
     * @return true when at least one column is projected
     */
    public boolean hasProjection() {
        return projection != null;
    }

    /**
     * Returns a copy with the projection replaced.
     *
     * @param columns the new projection
     * @return new options
     */
    public ReadOptions withProjection(List<String> columns) {
        return new ReadOptions(columns, pushdownPredicates, limit, formatOptions);
    }

    /**
     * Returns a copy with the pushdown predicates replaced.
     *
     * @param predicates the new predicates
     * @return new options
     */
    public ReadOptions withPredicates(List<ColumnPredicate> predicates) {
        return new ReadOptions(projection, predicates, limit, formatOptions);
    }

    /**
     * Returns a copy with the row limit replaced.
     *
     * @param maxRows the new limit, negative for unlimited
     * @return new options
     */
    public ReadOptions withLimit(long maxRows) {
        return new ReadOptions(projection, pushdownPredicates, maxRows, formatOptions);
    }
}