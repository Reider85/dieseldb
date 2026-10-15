package diesel.storage.page;

import java.util.concurrent.atomic.AtomicLong;

/**
 * JMX MBean interface for CheckpointManager monitoring.
 */
public interface CheckpointManagerMXBean {
    /**
     * Returns the number of checkpoints performed.
     *
     * @return checkpoint count
     */
    long getCheckpointCount();

    /**
     * Returns the LSN of the last checkpoint (0 = none).
     *
     * @return last checkpoint LSN
     */
    long getLastCheckpointLsn();

    /**
     * Returns the duration of the last checkpoint in milliseconds.
     *
     * @return last checkpoint duration in milliseconds
     */
    long getLastCheckpointDurationMs();

    /**
     * Returns the total number of checkpoints performed.
     *
     * @return total checkpoints
     */
    long getTotalCheckpoints();

    /**
     * Returns the current adaptive interval in milliseconds.
     *
     * @return current interval in milliseconds
     */
    int getCurrentIntervalMs();

    /**
     * Returns the base interval configured at construction.
     *
     * @return base interval in milliseconds
     */
    int getBaseIntervalMs();

    /**
     * Returns the total number of dirty pages flushed during checkpoints.
     *
     * @return total flushed pages
     */
    long getTotalFlushedPages();

    /**
     * Returns the total time spent flushing in milliseconds.
     *
     * @return total flush duration in milliseconds
     */
    long getTotalFlushDurationMs();
}