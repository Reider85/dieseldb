package diesel.backup;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ScheduleTest acceptance (prompt4 #42): cron {@code 0 2 * * *} (daily at 02:00)
 * resolves to the next 02:00 and the scheduler triggers the backup action
 * automatically. Time is injected via {@link Clock} so the trigger test runs in
 * milliseconds instead of waiting for a real minute boundary.
 */
@Tag("storage")
public class BackupScheduleTest {

    // ------------------------------------------------------------------
    // Cron parsing / next-run resolution
    // ------------------------------------------------------------------

    @Test
    void dailyTwoAmScheduleMatchesPromptExample() {
        BackupScheduler.CronExpression cron = BackupScheduler.CronExpression.parse("0 2 * * *");

        // From 10:00 today -> 02:00 tomorrow.
        LocalDateTime from = LocalDateTime.of(2026, 10, 28, 10, 0);
        assertEquals(LocalDateTime.of(2026, 10, 29, 2, 0), cron.nextExecutionTime(from));

        // Just before 02:00 -> same day 02:00.
        LocalDateTime justBefore = LocalDateTime.of(2026, 10, 28, 1, 59);
        assertEquals(LocalDateTime.of(2026, 10, 28, 2, 0), cron.nextExecutionTime(justBefore));

        // Exactly 02:00 -> strictly next day.
        assertEquals(LocalDateTime.of(2026, 10, 29, 2, 0),
                cron.nextExecutionTime(LocalDateTime.of(2026, 10, 28, 2, 0)));
    }

    @Test
    void stepsRangesListsAndDowAreSupported() {
        assertEquals(LocalDateTime.of(2026, 10, 28, 10, 15),
                BackupScheduler.CronExpression.parse("*/15 * * * *")
                        .nextExecutionTime(LocalDateTime.of(2026, 10, 28, 10, 3)));
        assertEquals(LocalDateTime.of(2026, 10, 29, 9, 10),
                BackupScheduler.CronExpression.parse("10 9-17 * * *")
                        .nextExecutionTime(LocalDateTime.of(2026, 10, 28, 20, 0)));
        assertEquals(LocalDateTime.of(2026, 10, 28, 14, 5),
                BackupScheduler.CronExpression.parse("5,35 14 * * *")
                        .nextExecutionTime(LocalDateTime.of(2026, 10, 28, 14, 1)));
        // Next Sunday (2026-11-01) at 03:00; dow 7 == Sunday == 0.
        assertEquals(LocalDateTime.of(2026, 11, 1, 3, 0),
                BackupScheduler.CronExpression.parse("0 3 * * 7")
                        .nextExecutionTime(LocalDateTime.of(2026, 10, 28, 12, 0)));
        // Classic OR rule: dom 29 AND dow Friday -> matches either side.
        BackupScheduler.CronExpression orRule = BackupScheduler.CronExpression.parse("0 4 29 10 5");
        // 2026-10-29 is a Thursday but matches dom=29, so it comes first...
        assertEquals(LocalDateTime.of(2026, 10, 29, 4, 0),
                orRule.nextExecutionTime(LocalDateTime.of(2026, 10, 28, 12, 0)));
        // ...and after that Thursday, the next match is Friday 2026-10-30 (dow rule).
        assertEquals(LocalDateTime.of(2026, 10, 30, 4, 0),
                orRule.nextExecutionTime(LocalDateTime.of(2026, 10, 29, 5, 0)));
    }

    @Test
    void invalidExpressionsAreRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> BackupScheduler.CronExpression.parse("0 2 * *"));          // 4 fields
        assertThrows(IllegalArgumentException.class,
                () -> BackupScheduler.CronExpression.parse("60 2 * * *"));       // minute out of range
        assertThrows(IllegalArgumentException.class,
                () -> BackupScheduler.CronExpression.parse("0 24 * * *"));       // hour out of range
        assertThrows(IllegalArgumentException.class,
                () -> BackupScheduler.CronExpression.parse("abc 2 * * *"));      // not a number
        assertThrows(IllegalArgumentException.class,
                () -> BackupScheduler.CronExpression.parse("*/0 * * * *"));      // zero step
        assertThrows(IllegalArgumentException.class,
                () -> BackupScheduler.CronExpression.parse("5-1 * * * *"));      // empty range
        assertThrows(IllegalArgumentException.class,
                () -> BackupScheduler.CronExpression.parse(""));
        assertThrows(IllegalArgumentException.class,
                () -> BackupScheduler.CronExpression.parse(null));
    }

    @Test
    void configResolutionFindsScheduleKey() {
        assertEquals("0 2 * * *", BackupScheduler.resolveConfig(
                BackupScheduler.SCHEDULE_KEY, BackupScheduler.DEFAULT_SCHEDULE));
        System.setProperty(BackupScheduler.SCHEDULE_KEY, "30 3 * * *");
        try {
            assertEquals("30 3 * * *", BackupScheduler.resolveConfig(
                    BackupScheduler.SCHEDULE_KEY, BackupScheduler.DEFAULT_SCHEDULE));
        } finally {
            System.clearProperty(BackupScheduler.SCHEDULE_KEY);
        }
    }

    // ------------------------------------------------------------------
    // Automatic triggering
    // ------------------------------------------------------------------

    @Test
    void schedulerTriggersBackupActionAutomatically() throws Exception {
        // Mutable clock: starts at 01:59:59, then jumps past 02:00:00.
        MutableClock clock = new MutableClock(LocalDateTime.of(2026, 10, 28, 1, 59, 59));
        AtomicInteger runs = new AtomicInteger();
        CountDownLatch fired = new CountDownLatch(1);

        BackupScheduler scheduler = new BackupScheduler(() -> {
            runs.incrementAndGet();
            fired.countDown();
        }, "0 2 * * *", clock);

        assertFalse(BackupScheduler.isEnabled(), "backup.enabled defaults to false");
        scheduler.start();
        assertTrue(scheduler.isRunning());
        awaitNextRun(scheduler, LocalDateTime.of(2026, 10, 28, 2, 0));

        // The worker's first sleep is computed from the injected clock
        // (01:59:59 -> 02:00:00 = 1s of wall time), so it fires within seconds.
        clock.set(LocalDateTime.of(2026, 10, 28, 2, 0, 1));
        assertTrue(fired.await(10, TimeUnit.SECONDS),
                "scheduled backup must fire automatically at 02:00");
        assertEquals(1, runs.get());

        scheduler.stop();
        assertFalse(scheduler.isRunning());

        // stop() is idempotent and start() after stop works again.
        scheduler.stop();
        AtomicInteger secondRuns = new AtomicInteger();
        MutableClock secondClock = new MutableClock(LocalDateTime.of(2026, 10, 28, 5, 0, 59));
        BackupScheduler second = new BackupScheduler(secondRuns::incrementAndGet,
                "* * * * *", secondClock);
        second.start();
        // Advance the clock only after the worker published its first target,
        // otherwise the jump moves the goal to the next minute (race).
        awaitNextRun(second, LocalDateTime.of(2026, 10, 28, 5, 1));
        secondClock.set(LocalDateTime.of(2026, 10, 28, 5, 1, 1));
        awaitAtLeast(secondRuns, 1, 10_000);
        second.stop();
    }

    @Test
    void failingActionDoesNotKillSchedule() throws Exception {
        MutableClock clock = new MutableClock(LocalDateTime.of(2026, 10, 28, 3, 59, 59));
        AtomicInteger attempts = new AtomicInteger();
        BackupScheduler scheduler = new BackupScheduler(() -> {
            attempts.incrementAndGet();
            throw new RuntimeException("backup device hiccup");
        }, "0 4 * * *", clock);

        scheduler.start();
        // Wait for the worker to publish its first target before jumping time.
        awaitNextRun(scheduler, LocalDateTime.of(2026, 10, 28, 4, 0));
        clock.set(LocalDateTime.of(2026, 10, 28, 4, 0, 1));
        awaitAtLeast(attempts, 1, 10_000);

        // Schedule survives: next run rolls to 04:00 tomorrow and the thread lives.
        awaitNextRun(scheduler, LocalDateTime.of(2026, 10, 29, 4, 0));
        assertTrue(scheduler.isRunning());
        scheduler.stop();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static void awaitAtLeast(AtomicInteger counter, int target, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (counter.get() < target && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(counter.get() >= target,
                "counter did not reach " + target + " within " + timeoutMs + "ms");
    }

    /** Polls until the worker thread published the expected next run. */
    private static void awaitNextRun(BackupScheduler scheduler, LocalDateTime expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (expected.equals(scheduler.getNextRun())) {
                return;
            }
            Thread.sleep(20);
        }
        fail("next run did not become " + expected + " (was " + scheduler.getNextRun() + ")");
    }

    /** Test clock whose instant can be pushed forward while the scheduler runs. */
    private static final class MutableClock extends Clock {
        private volatile LocalDateTime time;
        private final ZoneId zone;

        MutableClock(LocalDateTime time) {
            this.time = time;
            this.zone = ZoneOffset.UTC;
        }

        void set(LocalDateTime newTime) {
            this.time = newTime;
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public java.time.Instant instant() {
            return time.atZone(zone).toInstant();
        }
    }
}
