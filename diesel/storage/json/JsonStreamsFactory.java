package diesel.storage.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cached {@link JsonFactory} keyed by {@link JsonParserConfig}. The cache is
 * shared by both the Jackson parser and generator implementations so a
 * configuration produces exactly one Jackson factory instance.
 */
final class JsonStreamsFactory {

    private static final ConcurrentHashMap<JsonParserConfig, JsonFactory> CACHE = new ConcurrentHashMap<>();

    private JsonStreamsFactory() {
    }

    static JsonFactory jsonFactory(JsonParserConfig config) {
        return CACHE.computeIfAbsent(config, c -> JsonFactory.builder()
                .streamReadConstraints(StreamReadConstraints.builder()
                        .maxNestingDepth(c.maxNestingDepth())
                        .maxStringLength(c.maxStringLength())
                        .build())
                .build());
    }
}