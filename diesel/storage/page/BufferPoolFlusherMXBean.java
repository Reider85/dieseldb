package diesel.storage.page;

/**
 * JMX interface for BufferPoolFlusher monitoring (prompt4.md #20, R3-005).
 *
 * <p>Exposes the flusher metrics required by the prompt:
 * dirty page count ({@code DirtyPageCount} - the {@code bufferpool.dirty.pages}
 * metric) and flush durations ({@code LastFlushDurationMs} /
 * {@code TotalFlushDurationMs} - the {@code flusher.duration.ms} metric).
 */
public interface BufferPoolFlusherMXBean {
    long getDirtyPageCount();
    int getCapacityPages();
    int getCurrentIntervalMs();
    int getBaseIntervalMs();
    long getFlushCount();
    long getTotalFlushedPages();
    long getLastFlushDurationMs();
    long getTotalFlushDurationMs();
}
