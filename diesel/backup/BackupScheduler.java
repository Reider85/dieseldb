package diesel.backup;

import java.io.File;
import java.io.FileInputStream;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scheduled backup runner (prompt4 #42, R3-011 step 3/4).
 *
 * <p>Fires a {@link Runnable} backup action on a cron-like schedule resolved
 * from {@code backup.schedule} (5 fields: {@code minute hour day-of-month month
 * day-of-week}; supports {@code *}, {@code ?} (== {@code *}), numbers, lists
 * {@code 1,15}, ranges {@code 1-5} and steps {@code *&#47;15}, {@code 10-40&#47;5}).
 * A daemon thread sleeps until the next matching minute and runs the action;
 * an exception thrown by the action is logged and does not stop the schedule.
 *
 * <p>Config resolution follows the project-wide pattern
 * (system property -&gt; {@code config.properties} in CWD -&gt; default),
 * self-contained because {@code diesel.ConfigLoader} is package-private in a
 * different package (same precedent as {@code PageConfig.loadRootProps}):
 * <ul>
 *   <li>{@code backup.enabled} (default {@code false})</li>
 *   <li>{@code backup.schedule} (default {@code 0 2 * * *} = daily at 02:00)</li>
 * </ul>
 *
 * <p>Usage: {@code new BackupScheduler(action).start()} - typically wired by an
 * operator tool or the server bootstrap; the scheduler does not start by itself.
 */
public final class BackupScheduler {

    public static final String ENABLED_KEY = "backup.enabled";
    public static final String SCHEDULE_KEY = "backup.schedule";
    public static final String DEFAULT_SCHEDULE = "0 2 * * *";

    private static final Logger LOGGER = Logger.getLogger(BackupScheduler.class.getName());

    private final Runnable action;
    private final CronExpression cron;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread worker;
    private volatile boolean stopped;

    /** Next scheduled run, visible for monitoring/tests; null before {@link #start()}. */
    private volatile LocalDateTime nextRun;

    public BackupScheduler(Runnable action) {
        this(action, resolveConfig(SCHEDULE_KEY, DEFAULT_SCHEDULE), Clock.systemDefaultZone());
    }

    public BackupScheduler(Runnable action, String cronExpression) {
        this(action, cronExpression, Clock.systemDefaultZone());
    }

    /**
     * Full constructor; the clock is injectable so tests can control time
     * instead of waiting for real minute boundaries.
     */
    public BackupScheduler(Runnable action, String cronExpression, Clock clock) {
        if (action == null) {
            throw new IllegalArgumentException("action must not be null");
        }
        this.action = action;
        this.cron = CronExpression.parse(cronExpression);
        this.clock = clock == null ? Clock.systemDefaultZone() : clock;
    }

    /** Whether {@code backup.enabled} resolves to true (sysprop -&gt; config -&gt; default). */
    public static boolean isEnabled() {
        return Boolean.parseBoolean(resolveConfig(ENABLED_KEY, "false"));
    }

    static String resolveConfig(String key, String defaultValue) {
        String raw = System.getProperty(key);
        if (raw == null) {
            Properties props = new Properties();
            File configFile = new File("config.properties");
            if (configFile.exists()) {
                try (FileInputStream fis = new FileInputStream(configFile)) {
                    props.load(fis);
                } catch (Exception ignored) {
                    // Fail-safe: keep defaults, same as ConfigLoader.
                }
            }
            raw = props.getProperty(key);
        }
        return raw == null ? defaultValue : raw.trim();
    }

    /** Starts the daemon thread; no-op if already running. */
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        Thread t = new Thread(this::loop, "diesel-backup-scheduler");
        t.setDaemon(true);
        worker = t;
        t.start();
        LOGGER.info(() -> "BackupScheduler started, cron='" + cron.source + "'");
    }

    /** Stops the scheduler and wakes the sleeping worker; idempotent. */
    public void stop() {
        stopped = true;
        Thread t = worker;
        if (t != null) {
            t.interrupt();
        }
        running.set(false);
    }

    public boolean isRunning() {
        return running.get();
    }

    /** Next scheduled run in local time, or null if not started/stopped. */
    public LocalDateTime getNextRun() {
        return nextRun;
    }

    private void loop() {
        while (!stopped) {
            LocalDateTime next = cron.nextExecutionTime(LocalDateTime.now(clock));
            nextRun = next;
            // Sleep in bounded chunks and recheck the clock each time: an
            // injected (test) clock can jump forward, and stop() must take
            // effect promptly without waiting out a multi-hour sleep.
            while (!stopped) {
                long remainingMs = java.time.Duration.between(LocalDateTime.now(clock), next).toMillis();
                if (remainingMs <= 0) {
                    break;
                }
                try {
                    Thread.sleep(Math.min(remainingMs, 1000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (stopped) {
                break;
            }
            try {
                action.run();
            } catch (Throwable t) {
                // A failed backup must not kill the schedule.
                LOGGER.log(Level.WARNING, "Scheduled backup failed", t);
            }
        }
        nextRun = null;
        running.set(false);
    }

    // ------------------------------------------------------------------
    // Cron expression
    // ------------------------------------------------------------------

    /**
     * Minimal 5-field cron: minute (0-59), hour (0-23), day-of-month (1-31),
     * month (1-12), day-of-week (0-6, 0=Sunday; 7 is accepted as Sunday).
     *
     * <p>Semantics: a date matches when every restricted field matches. When
     * both day-of-month and day-of-week are restricted, the classic cron OR
     * rule applies (matches if either side matches).
     */
    public static final class CronExpression {

        final String source;
        private final int[] min = new int[5];
        private final int[] max = new int[5];
        /** Bitsets over the field value range (minute/hour 0..59/0..23, dom 1..31, month 1..12, dow 0..7). */
        private final long[] mask = new long[5];
        private final boolean[] domRestricted = new boolean[1];
        private final boolean[] dowRestricted = new boolean[1];

        private CronExpression(String source) {
            this.source = source;
        }

        static CronExpression parse(String expression) {
            if (expression == null || expression.isBlank()) {
                throw new IllegalArgumentException("Empty cron expression");
            }
            String[] fields = expression.trim().split("\\s+");
            if (fields.length != 5) {
                throw new IllegalArgumentException(
                        "Cron expression must have 5 fields (minute hour dom month dow): '" + expression + "'");
            }
            CronExpression cron = new CronExpression(expression.trim());
            int[][] ranges = {{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}};
            for (int i = 0; i < 5; i++) {
                cron.min[i] = ranges[i][0];
                cron.max[i] = ranges[i][1];
                cron.mask[i] = parseField(fields[i], ranges[i][0], ranges[i][1], i, expression);
            }
            // Classic cron: only dom/dow both restricted triggers the OR rule.
            cron.domRestricted[0] = !isWildcard(fields[2]);
            cron.dowRestricted[0] = !isWildcard(fields[4]);
            return cron;
        }

        private static boolean isWildcard(String field) {
            return field.equals("*") || field.equals("?");
        }

        private static long parseField(String field, int lo, int hi, int idx, String expression) {
            long mask = 0L;
            for (String part : field.split(",")) {
                int step = 1;
                String range = part;
                int slash = part.indexOf('/');
                if (slash >= 0) {
                    range = part.substring(0, slash);
                    try {
                        step = Integer.parseInt(part.substring(slash + 1));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Bad step in field '" + part + "' of '" + expression + "'");
                    }
                    if (step <= 0) {
                        throw new IllegalArgumentException("Non-positive step in field '" + part + "' of '" + expression + "'");
                    }
                }
                int start;
                int end;
                if (range.equals("*") || range.equals("?")) {
                    start = lo;
                    end = hi;
                } else {
                    int dash = range.indexOf('-', 1);
                    if (dash >= 0) {
                        start = parseValue(range.substring(0, dash), lo, hi, idx, expression);
                        end = parseValue(range.substring(dash + 1), lo, hi, idx, expression);
                        if (start > end) {
                            throw new IllegalArgumentException("Empty range '" + range + "' in '" + expression + "'");
                        }
                    } else {
                        start = parseValue(range, lo, hi, idx, expression);
                        // "N/step" means "N-max/step" per classic cron.
                        end = slash >= 0 ? hi : start;
                    }
                }
                for (int v = start; v <= end; v += step) {
                    mask |= 1L << v;
                }
            }
            if (mask == 0) {
                throw new IllegalArgumentException("Field matches nothing: '" + field + "' in '" + expression + "'");
            }
            return mask;
        }

        private static int parseValue(String token, int lo, int hi, int idx, String expression) {
            int v;
            try {
                v = Integer.parseInt(token.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Bad value '" + token + "' in field " + idx
                        + " of '" + expression + "'");
            }
            // dow: accept 7 as Sunday (== 0).
            if (idx == 4 && v == 7) {
                v = 0;
            }
            if (v < lo || v > hi) {
                throw new IllegalArgumentException("Value " + v + " out of range [" + lo + "," + hi
                        + "] in field " + idx + " of '" + expression + "'");
            }
            return v;
        }

        /** Whether the given minute matches every field of this expression. */
        public boolean matches(LocalDateTime time) {
            if ((mask[0] & (1L << time.getMinute())) == 0
                    || (mask[1] & (1L << time.getHour())) == 0
                    || (mask[3] & (1L << time.getMonthValue())) == 0) {
                return false;
            }
            boolean dom = (mask[2] & (1L << time.getDayOfMonth())) != 0;
            int dow = time.getDayOfWeek().getValue() % 7; // Mon=1..Sun=7 -> Sun=0
            boolean dowMatches = (mask[4] & (1L << dow)) != 0;
            if (domRestricted[0] && dowRestricted[0]) {
                return dom || dowMatches;
            }
            return dom && dowMatches;
        }

        /**
         * Next matching strictly-after minute; scans forward minute-by-minute.
         *
         * @throws IllegalStateException if no match within ~8 years (e.g. Feb 30)
         */
        public LocalDateTime nextExecutionTime(LocalDateTime after) {
            LocalDateTime candidate = after.truncatedTo(java.time.temporal.ChronoUnit.MINUTES).plusMinutes(1);
            // 8 years covers leap-day schedules (Feb 29 within ~4 years).
            LocalDateTime limit = after.plusYears(8);
            while (candidate.isBefore(limit)) {
                if (matches(candidate)) {
                    return candidate;
                }
                candidate = candidate.plusMinutes(1);
            }
            throw new IllegalStateException("Cron expression '" + source + "' never matches (checked 8 years ahead)");
        }
    }
}
