package diesel.storage.page;

/**
 * JMX management interface for {@link BufferPool} (prompt4.md step 7,
 * R3-002 acceptance: "BufferPoolMXBean экспонирует hit/miss/pinned в JMX").
 *
 * <p>Registered on the platform MBean server under
 * {@code diesel:type=BufferPool,id=N} (sequence suffix keeps multiple pool
 * instances in one JVM independent), lazily on construction and unregistered
 * by {@link BufferPool#close()}.
 *
 * <p>All counters are cumulative since pool creation; {@link #getHitRate()}
 * is {@code hits / (hits + misses)} and {@code 0.0} when nothing was requested
 * yet. Attribute names exposed to JMX are the decapitalised getter names
 * ({@code Hits}, {@code Misses}, {@code Evictions}, {@code ResidentPages},
 * {@code CapacityPages}, {@code PinnedPages}, {@code HitRate}).
 */
public interface BufferPoolMXBean {

    /** Cumulative cache hits (pin requests served from a resident frame). */
    long getHits();

    /** Cumulative cache misses (pin requests that had to load a page). */
    long getMisses();

    /** Cumulative pages evicted to make room (LRU victim selection). */
    long getEvictions();

    /** Pages currently resident in the pool (frames held). */
    int getResidentPages();

    /** Maximum number of frames the pool can hold. */
    int getCapacityPages();

    /** Distinct resident pages currently pinned (pin count > 0). */
    int getPinnedPages();

    /** Hit ratio {@code hits / (hits + misses)}, {@code 0.0} when no requests yet. */
    double getHitRate();
}
