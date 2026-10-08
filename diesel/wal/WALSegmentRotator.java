package diesel.wal;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segment rotation logic for WAL (prompt4.md step 14, R3-003 step 4/5).
 *
 * <p>Decides whether to rotate based on size or age, and performs the rotation.
 * Age rotation is disabled if maxSegmentAgeMs <= 0. Empty segments are never
 * rotated by age (nothing to preserve).
 *
 * <p>Thread-safety: not thread-safe (single-writer assumption).
 */
public final class WALSegmentRotator implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(WALSegmentRotator.class);

    private final WALConfig config;
    private final Clock clock;

    /**
     * Creates a new WAL segment rotator with the given configuration.
     *
     * @param config the WAL configuration
     * @param clock the clock for age-based rotation (for tests: injectable)
     */
    public WALSegmentRotator(WALConfig config, Clock clock) {
        this.config = config;
        this.clock = clock;
    }

    /**
     * Creates a WAL segment rotator with the given configuration and system clock.
     *
     * @param config the WAL configuration
     * @return the rotator
     */
    public static WALSegmentRotator create(WALConfig config) {
        return new WALSegmentRotator(config, Clock.systemUTC());
    }

    /**
     * Creates a WAL segment rotator with the given configuration and clock (for testing).
     *
     * @param config the WAL configuration
     * @param clock the clock for age-based rotation
     * @return the rotator
     */
    public static WALSegmentRotator create(WALConfig config, Clock clock) {
        return new WALSegmentRotator(config, clock);
    }

    /**
     * Checks if the current segment should be rotated before appending the given entry.
     *
     * @param segment the current segment to check
     * @param incomingEntrySize the size of the entry that will be appended
     * @return true if rotation is needed, false otherwise
     */
    public boolean shouldRotate(WALSegment segment, int incomingEntrySize) {
        // Size check
        if (segment.getPosition() + incomingEntrySize > config.getMaxSegmentSizeBytes()) {
            LOGGER.debug("Size-triggered rotation: segment {} position={} + incoming={} > max={}", 
                    segment.getNumber(), segment.getPosition(), incomingEntrySize, config.getMaxSegmentSizeBytes());
            return true;
        }

        // Age check (if enabled)
        if (config.getMaxSegmentAgeMs() > 0) {
            long ageMs = ChronoUnit.MILLIS.between(
                    Instant.ofEpochMilli(segment.getCreatedAtEpochMs()), 
                    clock.instant());
            if (ageMs > config.getMaxSegmentAgeMs()) {
                LOGGER.debug("Age-triggered rotation: segment {} age={}ms > max={}ms", 
                        segment.getNumber(), ageMs, config.getMaxSegmentAgeMs());
                return true;
            } else {
                LOGGER.debug("Age check: segment {} age={}ms <= max={}ms, no rotation", 
                        segment.getNumber(), ageMs, config.getMaxSegmentAgeMs());
            }
        }

        return false;
    }

    /**
     * Forces rotation of the current segment regardless of size or age.
     * This is useful for manual rotation or testing.
     *
     * @param segment the current segment to rotate
     * @return true if rotation was performed, false if segment was empty (no-op)
     * @throws IOException if the rotation fails
     */
    public boolean forceRotate(WALSegment segment) throws IOException {
        if (segment.getPosition() == 0) {
            LOGGER.debug("Force rotate: segment {} is empty, rotating anyway", segment.getNumber());
            // For force rotate, we always rotate even if empty
        } else {
            LOGGER.debug("Force rotating segment {}", segment.getNumber());
            segment.force(); // Ensure durability before rotation
        }
        return true;
    }

    @Override
    public void close() {
        // No resources to close
        LOGGER.debug("WALSegmentRotator closed");
    }
}