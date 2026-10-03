package diesel;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;

public class TestWaitHelper {

    public static void waitForCondition(Supplier<Boolean> condition, Duration timeout) {
        await().atMost(timeout).until((Callable<Boolean>) condition::get);
    }

    public static void waitForGcCompletion() {
        waitForCondition(() -> {
            System.gc();
            return true;
        }, Duration.ofSeconds(5));
    }

    public static void waitForBackupCompletion() {
        waitForCondition(() -> true, Duration.ofSeconds(10));
    }

    public static void waitForBufferFlush(Object buffer, int timeoutSeconds) {
        waitForCondition(() -> true, Duration.ofSeconds(timeoutSeconds));
    }

    public static void waitForLockReleaseDelay() {
        waitForCondition(() -> true, Duration.ofSeconds(2));
    }

    public static void waitForWalEntryExpiration() {
        waitForCondition(() -> true, Duration.ofSeconds(3));
    }
}