package diesel.observability;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prompt4 #45: dashboards/dieseldb.json — the Grafana import criterion.
 * <p>
 * A real Grafana instance cannot run in CI, so this test is the proxy for the
 * acceptance item "dashboard JSON imports without errors": it parses the file
 * with Gson and validates the structural contract Grafana's importer enforces
 * (schema version, panel/panel targets layout, datasource templating) plus the
 * business contract that the panels actually query diesel_* metrics.
 */
@Tag("smoke")
public class GrafanaDashboardTest {

    private static final Path DASHBOARD = Paths.get("dashboards", "dieseldb.json");

    private static JsonObject loadDashboard() throws IOException {
        assertTrue(Files.exists(DASHBOARD),
                "dashboard file missing: " + DASHBOARD.toAbsolutePath()
                        + " (run tests from the repository root)");
        try (Reader reader = Files.newBufferedReader(DASHBOARD, StandardCharsets.UTF_8)) {
            JsonElement root = JsonParser.parseReader(reader);
            assertTrue(root.isJsonObject(), "dashboard root must be a JSON object");
            return root.getAsJsonObject();
        }
    }

    @Test
    void dashboardParsesAndDeclaresGrafanaSchema() throws IOException {
        JsonObject root = loadDashboard();

        // Import gate: Grafana refuses or warns on ancient schema versions.
        assertTrue(root.has("schemaVersion"), "schemaVersion required");
        assertTrue(root.get("schemaVersion").getAsInt() >= 30,
                "schemaVersion too old for a modern Grafana import: "
                        + root.get("schemaVersion").getAsInt());
        assertTrue(root.has("title") && !root.get("title").getAsString().isEmpty(),
                "dashboard title required");
        assertTrue(root.has("panels"), "panels array required");
        assertTrue(root.has("time"), "time range required");
        assertTrue(root.has("templating"), "templating block required");

        // The datasource variable lets an importing operator pick their
        // Prometheus connection instead of baking a uid in. Grafana's shape is
        // templating: { list: [ ...variables ] }.
        JsonObject templating = root.getAsJsonObject("templating");
        assertTrue(templating.has("list"), "templating.list required");
        JsonArray variables = templating.getAsJsonArray("list");
        assertFalse(variables.isEmpty(), "templating list must not be empty");
        boolean hasDatasourceVariable = false;
        for (JsonElement element : variables) {
            JsonObject variable = element.getAsJsonObject();
            if ("datasource".equals(variable.get("type").getAsString())) {
                hasDatasourceVariable = true;
            }
        }
        assertTrue(hasDatasourceVariable, "a datasource template variable is required");
    }

    @Test
    void everyPanelHasValidGridAndTargets() throws IOException {
        JsonObject root = loadDashboard();
        JsonArray panels = root.getAsJsonArray("panels");
        assertFalse(panels.isEmpty(), "dashboard must contain panels");

        for (JsonElement element : panels) {
            JsonObject panel = element.getAsJsonObject();
            String title = panel.has("title") ? panel.get("title").getAsString() : "<untitled>";
            assertNotNull(panel.get("type"), "panel type missing: " + title);

            // Grafana's importer needs gridPos to lay panels out.
            assertTrue(panel.has("gridPos"), "gridPos missing: " + title);
            JsonObject grid = panel.getAsJsonObject("gridPos");
            for (String key : new String[]{"h", "w", "x", "y"}) {
                assertTrue(grid.has(key), "gridPos." + key + " missing: " + title);
            }

            assertTrue(panel.has("targets"), "targets missing: " + title);
            JsonArray targets = panel.getAsJsonArray("targets");
            assertFalse(targets.isEmpty(), "panel has no targets: " + title);
            for (JsonElement targetElement : targets) {
                JsonObject target = targetElement.getAsJsonObject();
                assertTrue(target.has("expr") && !target.get("expr").getAsString().isEmpty(),
                        "target expr missing in panel: " + title);
            }
        }
    }

    @Test
    void panelsCoverThePromptAcceptanceMetrics() throws IOException {
        // Acceptance: QPS, p99 latency, hit ratio, connections.
        String allExprs = collectExprs(loadDashboard());

        assertTrue(allExprs.contains("diesel_queries_total"),
                "QPS panel must query diesel_queries_total:\n" + allExprs);
        assertTrue(allExprs.contains("histogram_quantile")
                        && allExprs.contains("diesel_query_duration_seconds_bucket"),
                "p99 panel must histogram_quantile over diesel_query_duration_seconds_bucket:\n"
                        + allExprs);
        assertTrue(allExprs.contains("diesel_buffer_pool_hit_ratio"),
                "hit-ratio panel missing:\n" + allExprs);
        assertTrue(allExprs.contains("diesel_connections_active"),
                "connections panel missing:\n" + allExprs);
    }

    @Test
    void panelExpressionsAreSyntacticallyPlausible() throws IOException {
        // Cheap sanity so a broken paste (unbalanced parens/quotes) fails here
        // instead of as an opaque Grafana "Invalid query" at import time.
        for (String expr : collectExprs(loadDashboard()).split("\n")) {
            if (expr.isEmpty()) {
                continue;
            }
            assertEquals(count(expr, '('), count(expr, ')'),
                    "unbalanced parentheses in: " + expr);
            assertEquals(count(expr, '"') % 2, 0,
                    "unbalanced quotes in: " + expr);
            assertEquals(count(expr, '\'') % 2, 0,
                    "unbalanced single quotes in: " + expr);
        }
    }

    // ─── Helpers ────────────────────────────────────────────────────

    private static String collectExprs(JsonObject root) {
        StringBuilder sb = new StringBuilder();
        for (JsonElement element : root.getAsJsonArray("panels")) {
            JsonObject panel = element.getAsJsonObject();
            for (JsonElement target : panel.getAsJsonArray("targets")) {
                sb.append(target.getAsJsonObject().get("expr").getAsString()).append('\n');
            }
        }
        return sb.toString();
    }

    private static int count(String haystack, char needle) {
        int n = 0;
        for (int i = 0; i < haystack.length(); i++) {
            if (haystack.charAt(i) == needle) {
                n++;
            }
        }
        return n;
    }
}
