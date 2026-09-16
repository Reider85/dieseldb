package diesel;

import diesel.storage.JsonlRowReader;
import diesel.storage.JsonlRowWriter;
import diesel.storage.json.JsonEvent;
import diesel.storage.json.JsonParserConfig;
import diesel.storage.json.JsonStreamException;
import diesel.storage.json.JsonStreamGenerator;
import diesel.storage.json.JsonStreamParser;
import diesel.storage.json.JsonStreams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Prompt 42 - JSON no-thought abstraction over JSON parsing (single streaming
 * parser entry point).
 *
 * <p>Guards: (1) the storage package below {@code diesel.storage.json} must
 * never reference a JSON library (bytecode-level architectural test);
 * (2) swapping the backend (Jackson {@code <->} Gson) must not change reader
 * or writer behaviour; (3) nesting depth and string-length limits reject input
 * with {@code file:line} diagnostics; (4) lenient JSON (NaN/Infinity/comments)
 * is always an error on both backends.
 */
@Tag("storage")
class JsonStreamAbstractionTest {

    // ── Architecture guard (no JSON library below the json package) ──────

    @Test
    void storagePackageNeverReferencesJsonLibraries() throws Exception {
        List<String> violations = new ArrayList<>();
        Path storageClasses = Paths("target", "classes", "diesel", "storage");
        if (Files.isDirectory(storageClasses)) {
            try (Stream<Path> walk = Files.walk(storageClasses)) {
                for (Path file : walk.filter(Files::isRegularFile)
                        .filter(p -> p.toString().endsWith(".class"))
                        .toList()) {
                    String rel = storageClasses.relativize(file).toString().replace('\\', '/');
                    if (rel.startsWith("json/")) {
                        continue;
                    }
                    String bytes = new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1);
                    if (bytes.contains("com/fasterxml/jackson") || bytes.contains("com/google/gson")) {
                        violations.add(rel);
                    }
                }
            }
        } else {
            Path sourceRoot = Paths("diesel", "storage");
            try (Stream<Path> walk = Files.walk(sourceRoot)) {
                for (Path file : walk.filter(Files::isRegularFile)
                        .filter(p -> p.toString().endsWith(".java"))
                        .toList()) {
                    String rel = sourceRoot.relativize(file).toString().replace('\\', '/');
                    if (rel.startsWith("json/")) {
                        continue;
                    }
                    String text = Files.readString(file, StandardCharsets.UTF_8);
                    if (text.contains("com.fasterxml") || text.contains("com.google.gson")) {
                        violations.add(rel);
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(),
                "JSON library references must stay inside diesel/storage/json: " + violations);
    }

    // ── Backend swap must not change behaviour ───────────────────────────

    @Test
    void writerOutputByteIdenticalAcrossBackends() throws Exception {
        String jackson = writeLines(JsonParserConfig.defaultsFor(JsonParserConfig.Backend.JACKSON));
        String gson = writeLines(JsonParserConfig.defaultsFor(JsonParserConfig.Backend.GSON));
        assertEquals(jackson, gson, "Jackson and Gson backends must emit identical JSONL bytes");
    }

    @Test
    void readerBehaviourEquivalentAcrossBackends() throws Exception {
        String content = "{\"ID\":1,\"NAME\":\"Alice\",\"SCORE\":100.50,\"DATA\":{\"user\":{\"name\":\"A\",\"age\":30}},\"TAGS\":[\"a\",\"b\"]}\n"
                + "{\"ID\":2,\"NAME\":\"Bob\",\"SCORE\":null,\"DATA\":{\"user\":null},\"TAGS\":[]}\n";
        Map<String, Object> jackson = readOne(content, JsonParserConfig.defaultsFor(JsonParserConfig.Backend.JACKSON));
        Map<String, Object> gson = readOne(content, JsonParserConfig.defaultsFor(JsonParserConfig.Backend.GSON));
        assertEquals(jackson, gson, "reader rows must be identical across backends");
        assertEquals("{\"user\":{\"name\":\"A\",\"age\":30}}", jackson.get("DATA"));
    }

    @Test
    void crossBackendCapatureKeepsNestedTextEquivalent() throws Exception {
        String content = "{\"ID\":1,\"DATA\":{\"p\":1e3,\"f\":100.50,\"s\":\"x\"}}\n";
        Map<String, Object> jackson = readOne(content, JsonParserConfig.defaultsFor(JsonParserConfig.Backend.JACKSON));
        Map<String, Object> gson = readOne(content, JsonParserConfig.defaultsFor(JsonParserConfig.Backend.GSON));
        assertEquals(jackson.get("DATA"), gson.get("DATA"), "nested capture must be equivalent");
    }

    // ── Nesting depth limit with line coordinates ────────────────────────

    @Test
    void nestingDepthLimitRejectsDeepInputWithLineContext() {
        String deep = "{\"DATA\":" + nested(70) + "}";
        DieselIOException e = assertThrows(DieselIOException.class,
                () -> readOne(deep, JsonParserConfig.defaultsFor(JsonParserConfig.Backend.JACKSON)));
        assertTrue(e.getMessage().contains("line 1"), e.getMessage());

        DieselIOException gsonError = assertThrows(DieselIOException.class,
                () -> readOne(deep, JsonParserConfig.defaultsFor(JsonParserConfig.Backend.GSON)));
        assertTrue(gsonError.getMessage().contains("line 1"), gsonError.getMessage());
    }

    @Test
    void nestingDepthWithinLimitParsesFine() throws Exception {
        String ok = "{\"DATA\":" + nested(63) + "}";
        assertNotNull(readOne(ok, JsonParserConfig.defaults()).get("DATA"),
                "nested value under the depth limit must parse and capture");
    }

    @Test
    void nestingDepthRejectedByParserDirectlyForBothBackends() {
        String deep = "{\"DATA\":" + nested(70) + "}";
        for (JsonParserConfig.Backend backend : JsonParserConfig.Backend.values()) {
            assertThrows(JsonStreamException.class,
                    () -> drain(JsonStreams.createParser(deep, JsonParserConfig.defaultsFor(backend))),
                    "backend " + backend);
        }
    }

    // ── String length limit with line coordinates ────────────────────────

    @Test
    void maxStringLengthRejectsLongStringsWithLineContext() {
        for (JsonParserConfig.Backend backend : JsonParserConfig.Backend.values()) {
            JsonParserConfig config = JsonParserConfig.builder()
                    .backend(backend)
                    .maxStringLength(8)
                    .build();
            DieselIOException e = assertThrows(DieselIOException.class,
                    () -> readOne("{\"NAME\":\"123456789\"}\n", config),
                    "backend " + backend);
            assertTrue(e.getMessage().contains("line 1"), e.getMessage());
        }
    }

    @Test
    void maxStringLengthWithinLimitParsesFine() throws Exception {
        JsonParserConfig config = JsonParserConfig.builder().maxStringLength(8).build();
        assertEquals("12345678", readOne("{\"NAME\":\"12345678\"}\n", config).get("NAME"));
    }

    // ── Lenient mode is always off ───────────────────────────────────────

    @Test
    void nonStandardNumbersRejectedByBothBackends() {
        String nanLine = "{\"SCORE\":NaN}";
        String infinityLine = "{\"SCORE\":Infinity}";
        for (JsonParserConfig.Backend backend : JsonParserConfig.Backend.values()) {
            JsonParserConfig config = JsonParserConfig.defaultsFor(backend);
            assertThrows(JsonStreamException.class, () -> drain(JsonStreams.createParser(nanLine, config)),
                    "NaN rejected by " + backend);
            assertThrows(JsonStreamException.class, () -> drain(JsonStreams.createParser(infinityLine, config)),
                    "Infinity rejected by " + backend);
        }
    }

    @Test
    void jsonCommentsRejectedByBothBackendsWithLineContext() {
        String commentLine = "{/* c */\"SCORE\":1}\n";
        for (JsonParserConfig.Backend backend : JsonParserConfig.Backend.values()) {
            DieselIOException e = assertThrows(DieselIOException.class,
                    () -> readOne(commentLine, JsonParserConfig.defaultsFor(backend)),
                    "backend " + backend);
            assertTrue(e.getMessage().contains("line 1"), e.getMessage());
        }
    }

    @Test
    void nonFiniteWriteRejectedByBothBackends() throws Exception {
        for (JsonParserConfig.Backend backend : JsonParserConfig.Backend.values()) {
            JsonStreamGenerator generator = JsonStreams.createGenerator(new StringWriter(),
                    JsonParserConfig.defaultsFor(backend));
            assertThrows(IOException.class, () -> {
                generator.writeNumber(Double.NaN);
            }, "backend " + backend);
        }
    }

    // ── Duplicate-key hook (config only, semantics per prompt 43) ────────

    @Test
    void duplicateKeyFailsByDefault() {
        for (JsonParserConfig.Backend backend : JsonParserConfig.Backend.values()) {
            DieselIOException e = assertThrows(DieselIOException.class,
                    () -> readOne("{\"ID\":1,\"ID\":2}\n", JsonParserConfig.defaultsFor(backend)),
                    "backend " + backend);
            assertTrue(e.getMessage().contains("duplicate field 'ID'"), e.getMessage());
        }
    }

    @Test
    void duplicateKeyLastWinsModeKeepsLastValue() throws Exception {
        for (JsonParserConfig.Backend backend : JsonParserConfig.Backend.values()) {
            JsonParserConfig config = JsonParserConfig.builder()
                    .backend(backend)
                    .duplicateKeys(JsonParserConfig.DuplicateKeyMode.LAST_WINS)
                    .build();
            assertEquals(2L, readOne("{\"ID\":1,\"ID\":2}\n", config).get("ID"), "backend " + backend);
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private static List<String> columns() {
        return List.of("ID", "NAME", "SCORE", "DATA", "TAGS");
    }

    private static Map<String, Class<?>> types() {
        Map<String, Class<?>> t = new LinkedHashMap<>();
        t.put("ID", Long.class);
        t.put("NAME", String.class);
        t.put("SCORE", BigDecimal.class);
        t.put("DATA", String.class);
        t.put("TAGS", String.class);
        return t;
    }

    private static String writeLines(JsonParserConfig config) throws IOException {
        StringWriter sw = new StringWriter();
        try (JsonlRowWriter writer = new JsonlRowWriter(sw, columns(), types(), config)) {
            Map<String, Object> r1 = new LinkedHashMap<>();
            r1.put("ID", 1L);
            r1.put("NAME", "Alice <x> & c");
            r1.put("SCORE", new BigDecimal("100.50"));
            Map<String, Object> user = new LinkedHashMap<>();
            user.put("name", "A");
            user.put("age", 30);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("user", user);
            r1.put("DATA", data);
            r1.put("TAGS", List.of("a", "b"));
            writer.writeRow(r1);

            Map<String, Object> r2 = new LinkedHashMap<>();
            r2.put("ID", 2L);
            r2.put("NAME", "Bob");
            r2.put("SCORE", null);
            r2.put("DATA", Collections.singletonMap("user", null));
            r2.put("TAGS", List.of());
            writer.writeRow(r2);
        }
        return sw.toString();
    }

    private static Map<String, Object> readOne(String content, JsonParserConfig config) throws IOException {
        try (BufferedReader br = new BufferedReader(new StringReader(content));
             JsonlRowReader reader = new JsonlRowReader(br, columns(), types(), null, config)) {
            return reader.readAll().get(0);
        }
    }

    private static String nested(int depth) {
        StringBuilder sb = new StringBuilder(depth * 4 + 2);
        for (int i = 0; i < depth; i++) {
            sb.append("{\"k\":");
        }
        sb.append('1');
        for (int i = 0; i < depth; i++) {
            sb.append('}');
        }
        return sb.toString();
    }

    private static void drain(JsonStreamParser parser) throws IOException {
        try (JsonStreamParser p = parser) {
            JsonEvent event;
            do {
                event = p.nextToken();
            } while (event != JsonEvent.END_INPUT);
        }
    }

    private static Path Paths(String first, String... more) {
        return Path.of(first, more);
    }
}