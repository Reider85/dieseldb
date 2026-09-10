package diesel;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PerformanceRegressionTest extends AbstractDieselTest {
    private static final Logger LOGGER = Logger.getLogger(PerformanceRegressionTest.class.getName());

    private static final double REGRESSION_THRESHOLD = 1.2;
    private static final double NOISE_FLOOR_MS = 11.0;
    private static final int WARMUP_RUNS = 1;
    private static final int MEASURED_RUNS = 5;
    private static final String BASELINE_FILE = "analytics/regression_baseline.md";
    private static final String HISTORY_FILE = "analytics/performance_history.csv";
    private static final String UPDATE_BASELINE_PROPERTY = "diesel.updateBaseline";

    private static final List<KeyQuery> KEY_QUERIES = List.of(
            new KeyQuery("AdvancedTest", "simple select by primary key", "SELECT ID, NAME FROM USERS WHERE ID = 50"),
            new KeyQuery("AliasesTest", "complex select min max avg with join and group by", "SELECT u.NAME userName, t.TRANS_DATE transDate, MIN(u.AGE) minAge, MAX(u.AGE) maxAge, AVG(u.AGE) avgAge FROM USERS u INNER JOIN TRANSACTIONS t ON u.ID = t.USER_ID GROUP BY userName, transDate ORDER BY transDate DESC"),
            new KeyQuery("GroupByTest", "simple group by sum count", "SELECT NAME, SUM(AGE), COUNT(AGE) FROM USERS GROUP BY NAME"),
            new KeyQuery("GroupByTest", "complex group by join string date", "SELECT USERS.NAME, PROFILES.PROFILE_DATE, SUM(USERS.BALANCE), COUNT(USERS.BALANCE) FROM USERS INNER JOIN PROFILES ON USERS.ID = PROFILES.USER_ID GROUP BY USERS.NAME, PROFILES.PROFILE_DATE ORDER BY PROFILES.PROFILE_DATE DESC"),
            new KeyQuery("InTest", "simple in on btree index", "SELECT ID, NAME FROM USERS WHERE AGE IN (50, 51, 52)"),
            new KeyQuery("JoinTest", "simple inner join on primary key", "SELECT USERS.ID, USERS.NAME, USER_DETAILS.INFO FROM USERS INNER JOIN USER_DETAILS ON USERS.ID = USER_DETAILS.USER_ID WHERE USERS.ID IN (50, 51, 52)"),
            new KeyQuery("LikeTest", "simple like on name", "SELECT ID, NAME FROM USERS WHERE NAME LIKE '%er50' AND NAME LIKE '%User50%' AND NAME LIKE 'User50%'"),
            new KeyQuery("OrderByTest", "simple order by age desc", "SELECT ID, AGE FROM USERS ORDER BY AGE DESC"),
            new KeyQuery("PerformanceTest", "simple select clustered index", "SELECT NAME, AGE FROM USERS WHERE USER_CODE = 'CODE50'"),
            new KeyQuery("SubqueriesTest", "complex subquery in column where group by having", "SELECT (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) AS user_name, COUNT(*) AS user_count FROM USERS u WHERE AGE > (SELECT AGE FROM USERS WHERE ID = 50 LIMIT 1) GROUP BY (SELECT NAME FROM USERS WHERE ID = u.ID LIMIT 1) HAVING COUNT(*) > (SELECT ID FROM USERS WHERE ID = 1 LIMIT 1) LIMIT 10"));

    @Test
    public void performanceRegression() {
        boolean updateBaseline = Boolean.getBoolean(UPDATE_BASELINE_PROPERTY);
        Map<String, Double> baseline = loadBaseline();
        boolean reseed = updateBaseline || baseline.isEmpty();
        List<String> errors = new ArrayList<>();
        List<Measurement> measurements = new ArrayList<>();

        for (KeyQuery keyQuery : KEY_QUERIES) {
            try {
                double measured = measure(keyQuery.sql());
                Double base = baseline.get(keyQuery.group() + "|" + keyQuery.test());
                measurements.add(new Measurement(keyQuery, base == null ? -1.0 : base, measured));
            } catch (Exception e) {
                errors.add("ERROR " + keyQuery.group() + " / " + keyQuery.test() + ": " + e.getMessage());
                LOGGER.log(Level.SEVERE, "Key query failed", e);
            }
        }

        if (reseed) {
            writeBaseline(measurements);
            appendHistory(measurements, updateBaseline ? "SEED_UPDATE" : "SEED");
            LOGGER.log(Level.INFO, "Performance baseline {0} written to {1} ({2} queries)",
                    new Object[]{updateBaseline ? "updated" : "seeded", BASELINE_FILE, measurements.size()});
            return;
        }

        List<String> regressionReport = new ArrayList<>();
        List<String> okSummary = new ArrayList<>();
        for (Measurement m : measurements) {
            if (m.base() < 0) {
                appendHistoryRow(m, "NO_BASELINE");
                LOGGER.log(Level.INFO, "NO BASELINE (skipped check): {0} / {1} = {2} ms",
                        new Object[]{m.kq().group(), m.kq().test(), String.format(Locale.US, "%.2f", m.measured())});
                continue;
            }
            double ratio = m.measured() / m.base();
            String result;
            if (m.base() >= NOISE_FLOOR_MS && ratio > REGRESSION_THRESHOLD) {
                result = "REGRESSION";
                regressionReport.add(String.format(Locale.US, "REGRESSION: %-45s %.2fx (baseline=%.2f ms, measured=%.2f ms)",
                        m.kq().group() + " / " + m.kq().test(), ratio, m.base(), m.measured()));
            } else if (ratio > REGRESSION_THRESHOLD) {
                result = "SLOW_NOISE";
                LOGGER.log(Level.INFO, "Ignored sub-{0} ms noise: {1} / {2} = {3} ms (baseline {4} ms)",
                        new Object[]{(int) NOISE_FLOOR_MS, m.kq().group(), m.kq().test(),
                                String.format(Locale.US, "%.2f", m.measured()), String.format(Locale.US, "%.2f", m.base())});
            } else if (ratio < 0.8) {
                result = "IMPROVEMENT";
                okSummary.add(String.format(Locale.US, "IMPROVEMENT: %-45s %.2fx faster (was %.2f ms, now %.2f ms)",
                        m.kq().group() + " / " + m.kq().test(), 1.0 / ratio, m.base(), m.measured()));
            } else {
                result = "OK";
            }
            appendHistoryRow(m, result);
        }

        for (String line : okSummary) {
            LOGGER.log(Level.INFO, line);
        }

        if (!regressionReport.isEmpty()) {
            StringBuilder sb = new StringBuilder("Performance regression detected (" + regressionReport.size() + " key query(ies)):\n");
            for (String line : regressionReport) {
                sb.append("  ").append(line).append("\n");
            }
            sb.append("History recorded in ").append(HISTORY_FILE).append(". To accept new times, run with -D")
                    .append(UPDATE_BASELINE_PROPERTY).append("=true.\n");
            throw new AssertionError(sb.toString());
        }
        LOGGER.log(Level.INFO, "Performance regression check passed: {0} key queries, no regression > {1}x",
                new Object[]{measurements.size(), REGRESSION_THRESHOLD});
    }

    private double measure(String sql) {
        for (int i = 0; i < WARMUP_RUNS; i++) {
            database.executeQuery(sql, null);
        }
        List<Double> times = new ArrayList<>();
        for (int i = 0; i < MEASURED_RUNS; i++) {
            long start = System.nanoTime();
            database.executeQuery(sql, null);
            long end = System.nanoTime();
            times.add((end - start) / 1_000_000.0);
        }
        times.sort(Double::compareTo);
        return times.get(times.size() / 2);
    }

    private Map<String, Double> loadBaseline() {
        Map<String, Double> baseline = new HashMap<>();
        Path path = Paths.get(BASELINE_FILE);
        if (!Files.exists(path)) {
            LOGGER.log(Level.INFO, "Baseline file {0} not found - will be seeded from this run", BASELINE_FILE);
            return baseline;
        }
        try {
            for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                String[] parts = trimmed.split("\\s+\\|\\s+", 4);
                if (parts.length == 4) {
                    baseline.put(parts[0] + "|" + parts[1], Double.parseDouble(parts[2]));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read baseline " + BASELINE_FILE, e);
        }
        return baseline;
    }

    private void writeBaseline(List<Measurement> measurements) {
        try {
            Path path = Paths.get(BASELINE_FILE);
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            List<String> lines = new ArrayList<>();
            lines.add("# DieselDB performance regression baseline (tracked)");
            lines.add("Generated: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            lines.add("");
            lines.add("# Format: group | test | baseline_ms | query");
            lines.add("");
            for (Measurement m : measurements) {
                lines.add(String.format(Locale.US, "%s | %s | %.2f | %s", m.kq().group(), m.kq().test(), m.measured(), m.kq().sql()));
            }
            Files.write(path, lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write baseline " + BASELINE_FILE, e);
        }
    }

    private void appendHistory(List<Measurement> measurements, String result) {
        for (Measurement m : measurements) {
            appendHistoryRow(m, result);
        }
    }

    private void appendHistoryRow(Measurement m, String result) {
        try {
            Path path = Paths.get(HISTORY_FILE);
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            if (!Files.exists(path)) {
                Files.write(path, List.of("timestamp,group,test,query,baseline_ms,measured_ms,ratio,result"), StandardCharsets.UTF_8);
            }
            String ratio = m.base() >= 0 ? String.format(Locale.US, "%.3f", m.measured() / m.base()) : "N/A";
            String line = String.format(Locale.US, "%s,%s,%s,\"%s\",%.3f,%.3f,%s,%s",
                    LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    m.kq().group(), m.kq().test(), m.kq().sql().replace("\"", "\"\""),
                    m.base(), m.measured(), ratio, result);
            Files.write(path, List.of(line), StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append history to " + HISTORY_FILE, e);
        }
    }

    private record KeyQuery(String group, String test, String sql) {
    }

    private record Measurement(KeyQuery kq, double base, double measured) {
    }
}