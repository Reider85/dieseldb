package diesel.wal;

/**
 * Fsync policy for WAL writes (prompt4.md #15).
 * Controls when fsync() is called to ensure durability.
 */
public enum FsyncPolicy {
    /** fsync() on every write (highest durability, lowest throughput) */
    ALWAYS,
    
    /** fsync() on group commit timeout or max size (balanced durability/throughput) */
    GROUP,
    
    /** fsync() every second (medium durability, good throughput) */
    EVERYSEC,
    
    /** No fsync() (lowest durability, highest throughput) */
    NONE
}