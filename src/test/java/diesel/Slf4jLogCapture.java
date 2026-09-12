package diesel;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Test helper (prompt 37) for capturing slf4j/logback log events emitted by the
 * engine. Attaches a {@link ListAppender} to the named logger for the lifetime
 * of the capture and restores the logger level and appender set on
 * {@link #close()}.
 *
 * <pre>
 * try (Slf4jLogCapture capture = new Slf4jLogCapture("diesel.storage.CsvRowReader")) {
 *     reader.readAll();
 *     List&lt;ILoggingEvent&gt; warnings = capture.eventsMatching(Level.WARN, "extra fields");
 *     assertEquals(1, warnings.size());
 * }
 * </pre>
 */
public class Slf4jLogCapture implements AutoCloseable {

    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender;
    private final Level originalLevel;

    public Slf4jLogCapture(Class<?> loggerClass) {
        this(loggerClass.getName());
    }

    public Slf4jLogCapture(String loggerName) {
        this.logger = (Logger) LoggerFactory.getLogger(loggerName);
        this.appender = new ListAppender<>();
        this.appender.start();
        this.originalLevel = this.logger.getLevel();
        this.logger.setLevel(Level.TRACE);
        this.logger.addAppender(appender);
    }

    /** Returns the events captured so far, in emission order. */
    public List<ILoggingEvent> events() {
        return new ArrayList<>(appender.list);
    }

    /** Returns events at the given level whose formatted message contains {@code substring}. */
    public List<ILoggingEvent> eventsMatching(Level level, String substring) {
        return events().stream()
                .filter(e -> e.getLevel() == level && e.getFormattedMessage().contains(substring))
                .toList();
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        appender.stop();
        logger.setLevel(originalLevel);
    }
}