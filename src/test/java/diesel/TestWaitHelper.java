package diesel;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;

public class TestWaitHelper {

    public static void waitForCondition(Supplier<Boolean> condition, Duration timeout) {
        await().atMost(timeout).until((Callable<Boolean>) condition::get);
    }
}