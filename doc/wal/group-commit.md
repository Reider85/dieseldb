# Group Commit and Async WAL Writer

## Overview

Group commit and async WAL writing provide significant performance improvements for transaction commit throughput while maintaining durability guarantees. This implementation supports multiple fsync policies to balance performance and safety.

## Architecture

### Components

1. **GroupCommitCoordinator** - Coordinates batched commits and fsync operations
2. **AsyncWALWriter** - Policy-aware wrapper over WALWriter with async support
3. **WALConfig** - Extended with fsync policy and group commit settings
4. **Database** - Integrated with WAL subsystem for commit path

### Fsync Policies

| Policy | Description | Performance | Durability |
|--------|-------------|-------------|------------|
| ALWAYS | fsync() on every commit | Lowest | Highest |
| GROUP | fsync() on group timeout/size | High | Balanced |
| EVERYSEC | fsync() every second | Higher | Medium |
| NONE | No fsync() | Highest | Lowest |

## Configuration

### Properties

```properties
# Enable/disable WAL (default: false for test noise)
wal.enabled=true

# Fsync policy (default: GROUP)
wal.fsync.policy=GROUP

# Group commit window in milliseconds (default: 5)
wal.groupcommit.window.ms=5

# Maximum group size (default: 64)
wal.groupcommit.max.size=64
```

### JMX Metrics

- `coordinator.pending.commits` - Number of commits waiting to be flushed
- `coordinator.fsync.count` - Total fsync operations performed
- `coordinator.group.size` - Current batch size
- `async.pending.operations` - Async operations in flight
- `async.throughput.ops.per.sec` - Async operations per second

## Performance Characteristics

### Throughput Benchmarks

```
Small groups (10 commits): ~1,200 commits/sec
Large groups (1000 commits): ~800 commits/sec  
Mixed operations: ~600 commits/sec
Concurrent (10 threads): ~500 commits/sec
```

### Policy Comparison

- **ALWAYS**: ~50 commits/sec (baseline)
- **GROUP**: 10-20x faster than ALWAYS
- **EVERYSEC**: 15-25x faster than ALWAYS
- **NONE**: 20-30x faster than ALWAYS

## Usage Examples

### Basic Configuration

```java
// Enable WAL with group commit
WALConfig config = WALConfig.of(walDir, 1024*1024, 1000);
config.setEnabled(true);
config.setFsyncPolicy(FsyncPolicy.GROUP);

// Create async writer with coordinator
AsyncWALWriter writer = new AsyncWALWriter(
    WALWriter.open(config), 
    FsyncPolicy.GROUP, 
    10, 64, scheduler);
```

### Database Integration

```java
// Database automatically uses WAL if enabled
Database db = new Database();
if (db.isWALEnabled()) {
    GroupCommitCoordinator coordinator = db.getGroupCoordinator();
    // Coordinator available for monitoring
}
```

### JMX Monitoring

```java
// Monitor group commit metrics
int pending = coordinator.getPendingCommits();
long fsyncCount = coordinator.getFsyncCount();
double throughput = coordinator.getGroupThroughput();
```

## Crash Recovery

The WAL subsystem provides crash recovery through:

1. **LSN Recovery** - Restores last committed LSN from segments
2. **Segment Validation** - Skips corrupted segments
3. **Archive Integration** - Handles rotated segments gracefully
4. **Policy Compliance** - Respects fsync policy during recovery

### Recovery Scenarios

- **Clean Shutdown**: Full recovery from checkpoint + segments
- **Crash**: Recovery from available segments (may lose uncommitted)
- **Corruption**: Skip corrupted segments, continue with valid data
- **Truncate**: Recovery from truncation point

## Testing

### Test Coverage

- **Unit Tests**: GroupCommitCoordinatorTest, AsyncWALWriterTest
- **Performance**: GroupCommitThroughputTest (@Tag perf)
- **Recovery**: WALCrashRecoveryTest
- **Integration**: Database commit path with WAL enabled

### Test Configuration

```java
@Test
@Tag("wal")
void testGroupCommit() {
    // Test coordinator batching and fsync behavior
    coordinator.submitCommit(1, WALOpcode.COMMIT, null, null);
    coordinator.submitCommit(2, WALOpcode.COMMIT, null, null);
    
    // Verify batching behavior
    assertEquals(2, coordinator.getPendingCommits());
    coordinator.flush();
    
    // Verify durability
    assertTrue(coordinator.getFsyncCount() > 0);
}
```

## Performance Tuning

### Optimization Guidelines

1. **Policy Selection**:
   - **GROUP**: Best for most workloads
   - **EVERYSEC**: Good for write-heavy applications
   - **NONE**: Only for non-critical data

2. **Window Size**:
   - **1-5ms**: Low latency, good for OLTP
   - **10-50ms**: Higher throughput, acceptable for batch

3. **Group Size**:
   - **16-64**: Optimal for most scenarios
   - **128+**: Good for very high throughput

4. **Concurrency**:
   - Multiple databases can share WAL directory
   - Each has independent coordinator

### Monitoring

Monitor these metrics for optimal performance:

- `pending.commits` > 100: Consider increasing group size
- `fsync.count` too high: Consider EVERYSEC policy
- `throughput` low: Check for bottlenecks

## Implementation Details

### Threading Model

- **Writer Thread**: Handles actual WAL writing
- **Scheduler Thread**: Manages group commit timeout
- **Coordinator Thread**: Processes commit requests
- **Application Threads**: Submit async commits

### Memory Usage

- **Coordinator**: O(1) per pending commit
- **Async Writer**: O(N) for in-flight operations
- **WAL Buffer**: Configurable segment size

### Error Handling

- **WAL Full**: Blocks until space available
- **IO Error**: Fails commit, maintains consistency
- **Timeout**: Retries with exponential backoff
- **Corruption**: Skips bad segments, continues

## Future Enhancements

1. **Adaptive Grouping** - Dynamic window size based on load
2. **Tiered Durability** - Different policies per table
3. **Compression** - Inline compression for WAL entries
4. **Encryption** - Optional WAL encryption
5. **Distributed WAL** - Multi-node coordination