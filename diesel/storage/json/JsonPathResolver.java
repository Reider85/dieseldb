package diesel.storage.json;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single owner of dot-notation (JSON Path) resolution for the JSONL storage
 * (prompt 45). Both nested-storage modes share one path engine:
 * <ul>
 * <li>{@code flatten} - a dotted path {@code user.address.city} resolves to a
 * schema column literally named {@code user.address.city} (exact match, no
 * remaining segments); nested leaves are stored directly in such columns;</li>
 * <li>{@code json_column} - the path resolves to the longest schema-column
 * prefix that holds the nested value as compact JSON text (e.g. column
 * {@code user} + remaining segments {@code [address, city]}); the leaf is
 * extracted on read via {@link #extract}.</li>
 * </ul>
 * Resolution is case-insensitive and always prefers an exact column match
 * before any prefix match. Extraction is a pure token-level walk (no DOM
 * tree is built), mirroring the streaming-JSON rules of prompt 42.
 */
public final class JsonPathResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPathResolver.class);

    /**
     * The outcome of resolving a dotted path against a schema column set.
     *
     * @param columnIndex the schema column that owns the value
     * @param column the resolved schema column name
     * @param segments the remaining dot-path segments inside that column's
     *                 value (empty when the path is an exact column name)
     */
    public record ResolvedPath(int columnIndex, String column, List<String> segments) {

        /** Returns whether the path is an exact column name (no nested walk needed). */
        public boolean isPlain() {
            return columnIndex >= 0 && segments.isEmpty();
        }
    }

    /** An unresolved path (columnIndex {@code -1}). */
    public static final ResolvedPath UNRESOLVED = new ResolvedPath(-1, null, List.of());

    private JsonPathResolver() {
        throw new AssertionError("No instances");
    }

    /**
     * Resolves a dotted path against the schema column names, case-insensitively.
     * An exact column name wins; otherwise the longest schema-column prefix of
     * the dotted path wins and the remaining segments are returned.
     *
     * @param columns the ordered schema column names
     * @param path the dotted path to resolve
     * @return the {@link ResolvedPath}, or {@link #UNRESOLVED} when no column matches
     */
    public static ResolvedPath resolve(Collection<String> columns, String path) {
        if (columns == null || path == null || path.isBlank()) {
            return UNRESOLVED;
        }
        List<String> orderedColumns = new ArrayList<>(columns);
        TreeMap<String, Integer> index = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < orderedColumns.size(); i++) {
            index.putIfAbsent(orderedColumns.get(i), i);
        }
        Integer exact = index.get(path.trim());
        if (exact != null) {
            return new ResolvedPath(exact, orderedColumns.get(exact), List.of());
        }
        String[] parts = path.split("\\.");
        for (int prefix = parts.length; prefix > 0; prefix--) {
            String joined = parts[0];
            for (int k = 1; k < prefix; k++) {
                joined = joined + "." + parts[k];
            }
            Integer idx = index.get(joined);
            if (idx != null) {
                List<String> rest = new ArrayList<>();
                for (int k = prefix; k < parts.length; k++) {
                    rest.add(parts[k]);
                }
                return new ResolvedPath(idx, orderedColumns.get(idx), rest);
            }
        }
        return UNRESOLVED;
    }

    /**
     * Resolves a possibly table-qualified path against the schema column names.
     * Calls {@link #resolve} on the full path first; when that fails, drops one
     * leading "table." segment and retries. This supports parse-time type maps
     * whose keys are bare column names while conditions are normalized to the
     * {@code TABLE.column} form.
     */
    public static ResolvedPath resolveQualified(Collection<String> columns, String path) {
        ResolvedPath resolved = resolve(columns, path);
        if (resolved.columnIndex() >= 0) {
            return resolved;
        }
        int sep = path == null ? -1 : path.indexOf('.');
        if (sep > 0) {
            return resolve(columns, path.substring(sep + 1));
        }
        return UNRESOLVED;
    }

    /**
     * Extracts the value at the given dot-path segments from a JSON text value
     * (the compact JSON text captured into a STRING column). Token-level walk,
     * no DOM. Returns {@code null} when the path is absent or the container is
     * not an object; scalar leaves are returned as their raw token text and
     * nested leaves as compact JSON text.
     *
     * @param jsonText the JSON value text to walk (object/array/anything)
     * @param segments the remaining path segments
     * @param config the streaming JSON configuration for the walk
     */
    public static Object extract(String jsonText, List<String> segments, JsonParserConfig config) {
        if (jsonText == null || segments == null || segments.isEmpty()) {
            return null;
        }
        JsonParserConfig cfg = config != null ? config : JsonParserConfig.defaults();
        try (JsonStreamParser p = JsonStreams.createParser(new StringReader(jsonText), cfg)) {
            JsonEvent t = p.nextToken();
            for (int i = 0; i < segments.size(); i++) {
                if (t != JsonEvent.START_OBJECT) {
                    return null;
                }
                String wanted = segments.get(i);
                boolean found = false;
                while (p.nextToken() != JsonEvent.END_OBJECT) {
                    if (p.currentEvent() != JsonEvent.FIELD_NAME) {
                        return null;
                    }
                    if (!wanted.equals(p.currentName())) {
                        JsonEvent value = p.nextToken();
                        skipValue(p, value);
                        continue;
                    }
                    t = p.nextToken();
                    found = true;
                    break;
                }
                if (!found) {
                    return null;
                }
                if (i == segments.size() - 1) {
                    return leafValue(p, t, cfg);
                }
                if (t != JsonEvent.START_OBJECT) {
                    return null;
                }
            }
            return null;
        } catch (IOException e) {
            LOGGER.warn("Failed to extract JSON path {}: {}", String.join(".", segments), e.getMessage());
            return null;
        }
    }

    private static Object leafValue(JsonStreamParser p, JsonEvent t, JsonParserConfig config) throws IOException {
        return switch (t) {
            case VALUE_NULL -> null;
            case VALUE_STRING -> p.getText();
            case VALUE_TRUE, VALUE_FALSE, VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> p.getText();
            case START_OBJECT, START_ARRAY -> capture(p, config);
            default -> null;
        };
    }

    private static String capture(JsonStreamParser p, JsonParserConfig config) throws IOException {
        StringWriter sw = new StringWriter();
        try (JsonStreamGenerator g = JsonStreams.createGenerator(sw, config)) {
            g.copyCurrentStructure(p);
            g.flush();
        }
        return sw.toString();
    }

    private static void skipValue(JsonStreamParser p, JsonEvent t) throws IOException {
        if (t == JsonEvent.START_OBJECT || t == JsonEvent.START_ARRAY) {
            p.skipChildren();
        }
    }
}