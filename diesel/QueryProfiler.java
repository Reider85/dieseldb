package diesel;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.io.File;
import java.io.FileInputStream;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-query phase profiler (Prompt 18). Every query executed through
 * {@link Database#executeQuery} reports its phase breakdown - parse time,
 * plan time, execute time and sort time - to this singleton, which
 * accumulates the counters and emits a {@code "Slow query breakdown: ..."}
 * log line whenever a query's total time reaches the configured threshold.
 *
 * <p>The threshold comes from the {@code -Ddiesel.profile.slow.threshold.ms}
 * system property (default 1000 ms), falling back to the
 * {@code diesel.profile.slow.threshold.ms} key in {@code config.properties}.
 *
 * <p>Monitoring: the profiler is registered on the platform MBean server as a
 * read-only {@link DynamicMBean} under the name {@code diesel:type=QueryProfiler}.
 * The DynamicMBean form keeps the whole engine package-private (a standard
 * MBean would force a public interface). The slow-query log line itself is
 * emitted through SLF4J.
 *
 * <p>The engine has no planner separate from the parser: parsing builds the
 * executable AST (the plan), so "parse" covers the SQL-to-AST phase while
 * "plan" covers the per-execution planning inside {@link SelectQuery#execute}
 * (join reordering, ORDER BY key resolution, projection plan). "Execute" is the
 * remaining data processing (scan, join, filter, grouping, projection, LIMIT)
 * and "sort" is the ORDER BY phase.
 */
class QueryProfiler implements DynamicMBean {

    /** System property that overrides the slow-query threshold. */
    static final String SLOW_THRESHOLD_PROPERTY = "diesel.profile.slow.threshold.ms";

    /** Default slow-query threshold in milliseconds. */
    static final long DEFAULT_SLOW_THRESHOLD_MS = 1000;

    /** JMX object name of the profiler MBean. */
    static final String OBJECT_NAME = "diesel:type=QueryProfiler";

    private static final org.slf4j.Logger LOGGER =
            org.slf4j.LoggerFactory.getLogger(QueryProfiler.class);

    private static final QueryProfiler INSTANCE = new QueryProfiler();

    private final AtomicLong totalQueries = new AtomicLong();
    private final AtomicLong slowQueries = new AtomicLong();
    private final AtomicLong totalParseMs = new AtomicLong();
    private final AtomicLong totalPlanMs = new AtomicLong();
    private final AtomicLong totalExecuteMs = new AtomicLong();
    private final AtomicLong totalSortMs = new AtomicLong();
    private final AtomicLong maxTotalMs = new AtomicLong();

    /** Prompt 83: cumulative index-only scan counters. */
    private final AtomicLong totalIndexLookups = new AtomicLong();
    private final AtomicLong totalIndexOnlyScans = new AtomicLong();

    private volatile long slowThresholdMs;
    private volatile String lastSlowQuery = "";
    private volatile long lastSlowTotalMs;
    private volatile long lastParseMs;
    private volatile long lastPlanMs;
    private volatile long lastExecuteMs;
    private volatile long lastSortMs;
    private volatile long lastTotalMs;

    /** Prompt 83: last-execution index metrics. */
    private volatile long lastIndexLookups;
    private volatile long lastIndexOnlyScans;

    private QueryProfiler() {
        slowThresholdMs = loadSlowThresholdMs();
        registerMBean();
    }

    /** @return the JVM-wide profiler singleton */
    static QueryProfiler getInstance() {
        return INSTANCE;
    }

    /**
     * Records one executed query's phase breakdown. Always updates the
     * cumulative counters; when the total time reaches the threshold, also
     * increments the slow-query counter, stores the query and logs the
     * {@code "Slow query breakdown: ..."} line via SLF4J.
     *
     * @param sql              the executed SQL text
     * @param parseNanos       time spent parsing the SQL (0 for cached plans)
     * @param planNanos        time spent on the per-execution plan setup
     * @param executeNanos     time spent on the data processing phases
     * @param sortNanos        time spent on the ORDER BY phase
     * @param indexLookups     number of index lookups in this execution (Prompt 83)
     * @param indexOnlyScans   number of index-only scans in this execution (Prompt 83)
     */
    void record(String sql, long parseNanos, long planNanos, long executeNanos, long sortNanos,
                long indexLookups, long indexOnlyScans) {
        long parseMs = parseNanos / 1_000_000;
        long planMs = planNanos / 1_000_000;
        long executeMs = executeNanos / 1_000_000;
        long sortMs = sortNanos / 1_000_000;
        long totalMs = parseMs + planMs + executeMs + sortMs;

        totalQueries.incrementAndGet();
        totalParseMs.addAndGet(parseMs);
        totalPlanMs.addAndGet(planMs);
        totalExecuteMs.addAndGet(executeMs);
        totalSortMs.addAndGet(sortMs);
        maxTotalMs.accumulateAndGet(totalMs, Math::max);

        totalIndexLookups.addAndGet(indexLookups);
        totalIndexOnlyScans.addAndGet(indexOnlyScans);

        lastParseMs = parseMs;
        lastPlanMs = planMs;
        lastExecuteMs = executeMs;
        lastSortMs = sortMs;
        lastTotalMs = totalMs;
        lastIndexLookups = indexLookups;
        lastIndexOnlyScans = indexOnlyScans;

        if (totalMs >= slowThresholdMs) {
            slowQueries.incrementAndGet();
            lastSlowQuery = sql;
            lastSlowTotalMs = totalMs;
            LOGGER.warn("Slow query breakdown: parse={}ms, plan={}ms, execute={}ms, sort={}ms, total={}ms, sql={}",
                    parseMs, planMs, executeMs, sortMs, totalMs, sql);
        }
    }

    /** @return the configured slow-query threshold in milliseconds */
    long getSlowThresholdMs() {
        return slowThresholdMs;
    }

    /**
     * Test override for the slow-query threshold.
     *
     * @param thresholdMs new threshold in milliseconds, 0 makes every query slow
     */
    void setSlowThresholdMsForTest(long thresholdMs) {
        slowThresholdMs = Math.max(0, thresholdMs);
    }

    /** Resets every cumulative counter and the last-recorded breakdown. */
    void resetForTest() {
        totalQueries.set(0);
        slowQueries.set(0);
        totalParseMs.set(0);
        totalPlanMs.set(0);
        totalExecuteMs.set(0);
        totalSortMs.set(0);
        maxTotalMs.set(0);
        totalIndexLookups.set(0);
        totalIndexOnlyScans.set(0);
        lastSlowQuery = "";
        lastSlowTotalMs = 0;
        lastParseMs = 0;
        lastPlanMs = 0;
        lastExecuteMs = 0;
        lastSortMs = 0;
        lastTotalMs = 0;
        lastIndexLookups = 0;
        lastIndexOnlyScans = 0;
    }

    long getTotalQueries() {
        return totalQueries.get();
    }

    long getSlowQueryCount() {
        return slowQueries.get();
    }

    long getTotalParseMs() {
        return totalParseMs.get();
    }

    long getTotalPlanMs() {
        return totalPlanMs.get();
    }

    long getTotalExecuteMs() {
        return totalExecuteMs.get();
    }

    long getTotalSortMs() {
        return totalSortMs.get();
    }

    long getMaxTotalMs() {
        return maxTotalMs.get();
    }

    String getLastSlowQuery() {
        return lastSlowQuery;
    }

    long getLastSlowTotalMs() {
        return lastSlowTotalMs;
    }

    long getLastParseMs() {
        return lastParseMs;
    }

    long getLastPlanMs() {
        return lastPlanMs;
    }

    long getLastExecuteMs() {
        return lastExecuteMs;
    }

    long getLastSortMs() {
        return lastSortMs;
    }

    long getLastTotalMs() {
        return lastTotalMs;
    }

    long getTotalIndexLookups() {
        return totalIndexLookups.get();
    }

    long getTotalIndexOnlyScans() {
        return totalIndexOnlyScans.get();
    }

    long getLastIndexLookups() {
        return lastIndexLookups;
    }

    long getLastIndexOnlyScans() {
        return lastIndexOnlyScans;
    }

    private static long loadSlowThresholdMs() {
        String property = System.getProperty(SLOW_THRESHOLD_PROPERTY);
        if (property != null) {
            try {
                long parsed = Long.parseLong(property.trim());
                if (parsed >= 0) {
                    return parsed;
                }
            } catch (NumberFormatException ignored) {
                // Fall through to the config file / default
                LOGGER.debug("Invalid slow threshold property, using config file/default: {}", ignored.getMessage());
            }
        }
        try {
            File configFile = new File("config.properties");
            if (configFile.exists()) {
                Properties props = new Properties();
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                }
                String raw = props.getProperty(SLOW_THRESHOLD_PROPERTY);
                if (raw != null) {
                    long parsed = Long.parseLong(raw.trim());
                    if (parsed >= 0) {
                        return parsed;
                    }
                }
            }
        } catch (Exception ignored) {
            // Keep the default on any config error
            LOGGER.debug("Config file error, using default: {}", ignored.getMessage());
        }
        return DEFAULT_SLOW_THRESHOLD_MS;
    }

    private void registerMBean() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            server.registerMBean(this, new ObjectName(OBJECT_NAME));
        } catch (InstanceAlreadyExistsException ignored) {
            // Already registered on an earlier touch in this JVM
            LOGGER.debug("MBean already registered: {}", ignored.getMessage());
        } catch (Exception e) {
            LOGGER.warn("Failed to register QueryProfiler MBean: {}", e.getMessage());
        }
    }

    @Override
    public Object getAttribute(String attribute)
            throws AttributeNotFoundException, MBeanException, ReflectionException {
        switch (attribute) {
            case "ThresholdMs":
                return slowThresholdMs;
            case "TotalQueries":
                return totalQueries.get();
            case "SlowQueryCount":
                return slowQueries.get();
            case "TotalParseMs":
                return totalParseMs.get();
            case "TotalPlanMs":
                return totalPlanMs.get();
            case "TotalExecuteMs":
                return totalExecuteMs.get();
            case "TotalSortMs":
                return totalSortMs.get();
            case "MaxTotalMs":
                return maxTotalMs.get();
            case "LastSlowQuery":
                return lastSlowQuery;
            case "LastSlowTotalMs":
                return lastSlowTotalMs;
            case "TotalIndexLookups":
                return totalIndexLookups.get();
            case "TotalIndexOnlyScans":
                return totalIndexOnlyScans.get();
            case "LastIndexLookups":
                return lastIndexLookups;
            case "LastIndexOnlyScans":
                return lastIndexOnlyScans;
            default:
                throw new AttributeNotFoundException("Unknown attribute: " + attribute);
        }
    }

    @Override
    @SuppressWarnings("unused")
    public void setAttribute(Attribute attribute)
            throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
        throw new AttributeNotFoundException("QueryProfiler attributes are read-only");
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
        AttributeList result = new AttributeList();
        for (String attribute : attributes) {
            try {
                result.add(new Attribute(attribute, getAttribute(attribute)));
            } catch (Exception ignored) {
                // Skip attributes that cannot be read
                LOGGER.debug("Skipping unreadable attribute: {}", attribute);
            }
        }
        return result;
    }

    @Override
    @SuppressWarnings("unused")
    public AttributeList setAttributes(AttributeList attributes) {
        return new AttributeList();
    }

    @Override
    @SuppressWarnings("unused")
    public Object invoke(String actionName, Object[] params, String[] signature)
            throws MBeanException, ReflectionException {
        throw new ReflectionException(new UnsupportedOperationException(
                "No operations on QueryProfiler; attributes are read-only"));
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        String[] names = {
                "ThresholdMs", "TotalQueries", "SlowQueryCount", "TotalParseMs", "TotalPlanMs",
                "TotalExecuteMs", "TotalSortMs", "MaxTotalMs", "LastSlowQuery", "LastSlowTotalMs",
                "TotalIndexLookups", "TotalIndexOnlyScans", "LastIndexLookups", "LastIndexOnlyScans"
        };
        String[] descriptions = {
                "Slow-query threshold in milliseconds",
                "Total number of profiled queries",
                "Number of queries whose total time reached the threshold",
                "Cumulative parse time in milliseconds",
                "Cumulative plan time in milliseconds",
                "Cumulative execute time in milliseconds",
                "Cumulative sort time in milliseconds",
                "Maximum query total time in milliseconds",
                "SQL text of the most recent slow query",
                "Total time of the most recent slow query in milliseconds",
                "Total number of index lookups (Prompt 83)",
                "Total number of index-only scans (Prompt 83)",
                "Index lookups in the last query (Prompt 83)",
                "Index-only scans in the last query (Prompt 83)"
        };
        MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[names.length];
        for (int i = 0; i < names.length; i++) {
            attributes[i] = new MBeanAttributeInfo(names[i], "long", descriptions[i],
                    true, false, false);
        }
        return new MBeanInfo(QueryProfiler.class.getName(),
                "DieselDB per-query phase profiler (parse/plan/execute/sort)",
                attributes, null, null, null);
    }
}
