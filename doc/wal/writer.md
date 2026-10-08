# WAL Writer

**File:** `diesel/wal/WALWriter.java`  
**Prompt:** Prompt 4 #13 (R3-003 step 3/5)  
**Status:** Implemented with WALQueue, WALWriteRequest, JMX metrics, and 3 test suites  

## Overview

The `WALWriter` provides a single-writer thread for processing Write-Ahead Log entries with a bounded queue and backpressure handling. It decodes producer requests from a thread-safe queue and serializes all WAL file operations through a single background thread, ensuring strict LSN monotonicity and consistent I/O.

## Architecture

### Core Components

1. **`WALWriter`** - Main consumer thread and JMX metrics provider
2. **`WALQueue`** - Bounded `ArrayBlockingQueue<WALWriteRequest>` with backpressure tracking
3. **`WALWriteRequest`** - Immutable record containing transaction data and `CompletableFuture<WALEntry>`
4. **`WALManager`** - Existing WAL segment manager (single-writer assumption now satisfied)

### Thread Safety Model

- **Writer thread only:** Calls `WALManager.appendBatch()`, `WALManager.allocateLsn()`, `WALManager.flush()`
- **Producers only:** Enqueue `WALWriteRequest` objects, never touch WALManager file APIs
- **Queue:** Thread-safe `ArrayBlockingQueue` with atomic backpressure counters
- **Readers:** Should run after `flush()`/`close()` or tolerate concurrent `FileChannel` reads

## API

### Construction

```java
// Uses existing WAL manager
WALWriter writer = new WALWriter(walManager, config);

// Creates and owns WAL manager (convenience for tests)
WALWriter writer = WALWriter.open(config);
```

### Producer Methods

```java
// Asynchronous append with future completion
CompletableFuture<WALEntry> future = writer.appendAsync(txid, op, before, after);

// Synchronous append (blocking convenience)
WALEntry entry = writer.append(txid, op, before, after);

// Force fsync of all pending entries
writer.flush();
```

### Lifecycle

```java
// Graceful shutdown with thread join
writer.close();

// Idempotent close
writer.close();
```

### Metrics (JMX)

Object name: `diesel:type=WALWriter,id=N`

| Attribute | Description | Type |
|-----------|-------------|------|
| `wal.queue.size` | Current queue size | long |
| `wal.append.latency.p99` | 99th percentile append latency (µs) | long |
| `wal.append.count` | Total successful appends | long |
| `wal.append.errors` | Total failed appends | long |
| `wal.queue.blocked` | Times queue was full and put() blocked | long |
| `wal.queue.max.size` | Configured maximum queue size | long |

## Design Decisions

### LSN Allocation at Dequeue Time

**Problem:** If producers allocate LSNs at enqueue time, concurrent producers can interleave LSNs in the queue, violating strict monotonicity when the writer processes them.

**Solution:** Writer allocates LSNs via `manager.allocateLsn()` at dequeue time. This guarantees:
- Strictly monotonic LSNs under any concurrency level
- Compatible with prompt 15 `AsyncWALWriter` (same allocation point)
- No need for queue sorting or complex synchronization

**Producer API:**
```java
// Producer: enqueue request (no LSN needed)
WALWriteRequest request = new WALWriteRequest(txid, op, before, after, future, enqueueNanos);

// Writer: allocate LSN at dequeue time
long lsn = manager.allocateLsn();
WALEntry entry = new WALEntry(lsn, txid, op, before, after);
```

### Batched Segment Writes

**Problem:** A per-entry positional `channel.write` costs ~12µs on Windows (~85k/s
ceiling), leaving too little headroom for the 50k/s hard gate once queue handoff
and future completion are added (~30k/s measured).

**Solution:** The writer loop drains up to 256 requests per wakeup
(`WALQueue.drainTo`) and hands the entries to `WALManager.appendBatch()`, which
groups them into runs that fit the current segment; each run is encoded into one
buffer and written with a single `channel.write` (`WALSegment.appendBatch`).
Measured throughput: 67k–98k inserts/sec.

**Invariants preserved:**
- LSNs are still allocated at dequeue time, in queue order
- A flush barrier splits the pending batch: prior entries are written first,
  then `manager.flush()` runs, then the barrier future completes
- Batch size is capped by count (256) and encoded bytes (4MB) to bound both
  batch latency and buffer size; segment rotation happens at run boundaries

### Flush Barrier Semantics

**Design:** Special `WALWriteRequest` with `CompletableFuture<Void>` that blocks until all prior entries are durable.

**Implementation:**
```java
// Producer: enqueue flush barrier
CompletableFuture<Void> flushFuture = new CompletableFuture<>();
WALWriteRequest flushRequest = WALWriteRequest.createFlushBarrier(flushFuture);
queue.put(flushRequest);

// Writer: call manager.flush() and complete future
manager.flush();
flushFuture.complete(null);
```

**Guarantee:** Everything enqueued before `flush()` returns is durable on disk.

### Backpressure Implementation

**Queue:** `ArrayBlockingQueue<WALWriteRequest>` with configurable capacity.

**Backpressure Evidence:**
- `blockedCount`: Times `put()` waited on full queue
- `maxSizeSeen`: Maximum observed queue size

**Producer Experience:**
- Queue not full: `put()` returns immediately
- Queue full: `put()` blocks until space available
- Writer closed: `put()` throws `IllegalStateException`

### P99 Latency Calculation

**Implementation:** Fixed ring buffer (1024 samples) written only by writer thread (no contention).

```java
// Record latency at append completion
long latencyNanos = System.nanoTime() - request.enqueueNanos;
recordLatency(latencyNanos);

// P99 calculation (copy + sort + index)
long[] samples = copyRingBuffer();
Arrays.sort(samples, 0, count);
int p99Index = (int) Math.ceil(0.99 * count) - 1;
return samples[p99Index] / 1000L; // Convert to microseconds
```

**Edge Cases:**
- No samples: Return 0
- Concurrent reads: Copy ring buffer under lock
- Ring buffer wrap: Circular indexing handled automatically

## Configuration

### Properties

| Key | Default | Description |
|-----|---------|-------------|
| `wal.queue.max.size` | 100,000 | Maximum queue capacity |

### System Property Override

```bash
-Dwal.queue.max.size=50000
```

## Error Handling

### Writer Thread Resilience

- **Per-entry failures:** Complete future exceptionally, continue processing
- **Uncaught errors:** Log ERROR, keep thread alive
- **Repeated fatal errors:** Log SEVERE, document impact

### Producer Failures

```java
try {
    WALEntry entry = writer.append(txid, op, before, after);
} catch (CompletionException e) {
    Throwable cause = e.getCause();
    if (cause instanceof IOException) {
        // Handle I/O failure
    } else if (cause instanceof RuntimeException) {
        // Handle runtime error
    }
}
```

## Testing

### Test Coverage

| Test Suite | Tags | Purpose |
|------------|------|---------|
| `WALWriterTest` | storage, smoke | 100×1000 concurrent appends, LSN monotonicity, metrics, close semantics |
| `WALBackpressureTest` | storage, smoke | Queue full blocking, no data loss, sustained load handling |
| `WALWriterThroughputTest` | perf | 50k inserts/sec hard gate, queue < 1000, payload size variation |

### Key Test Scenarios

1. **Concurrent Appends:** 100 threads × 1000 inserts → 100k entries, LSNs = 1..100000
2. **Backpressure:** Queue size=8, 4×100 inserts of 1KB payloads → all succeed, `blockedCount > 0`
3. **Throughput:** 8 threads × 12.5k inserts → ≥50k/sec, max queue < 1000

## Integration Points

### Current (Prompt 13)

- **Greenfield implementation:** No engine integration yet
- **WALManager:** Now single-writer (only writer thread calls file APIs)
- **Producers:** Enqueue requests, no direct file access

### Future (Prompt 15)

- **`AsyncWALWriter`:** Wrapper returning futures, same LSN allocation point
- **Engine integration:** `InsertQuery`/`UpdateQuery`/`DeleteQuery` enqueue requests
- **Transaction COMMIT:** Path to `flush()` barrier for durability guarantees

## Performance Characteristics

### Throughput Targets

- **Minimum:** 50,000 inserts/sec (hard gate in `WALWriterThroughputTest`)
- **Measured (batched writes):** 67,330 inserts/sec (8 threads, string payloads),
  98,203 inserts/sec (16 threads, 4-byte payloads), 51,851 inserts/sec (4 threads, 1KB payloads)
- **Queue limit:** Maximum 1,000 entries during sustained load
- **Latency:** P99 measured in microseconds, ring buffer for low overhead

### Memory Usage

- **Queue:** ~800KB for 100k entries (8 bytes per request overhead)
- **Latency ring:** 8KB (1024 × 8-byte longs)
- **Thread:** Minimal overhead (single daemon thread)

### Scalability

- **Writer thread:** Single thread avoids contention, maximizes I/O throughput
- **Queue:** Bounded prevents OOM, backpressure controls producer load
- **LSN allocation:** No contention (writer thread only)

## Monitoring

### JMX Metrics

Monitor via `jconsole` or `visualvm`:
- Queue size and blocking events
- Append latency distribution
- Success/error rates
- Queue utilization patterns

### Log Events

- **INFO:** Writer thread start/stop, flush completion
- **WARN:** Per-entry failures, queue backpressure
- **ERROR:** Uncaught thread errors (document impact)

## Changelog

### R3-003 Step 3/5 (Prompt 13)

- **Added:** `WALWriter` single-writer thread with queue
- **Added:** `WALQueue` bounded queue with backpressure tracking  
- **Added:** `WALWriteRequest` immutable request records
- **Added:** JMX metrics (queue size, latency p99, counts, errors)
- **Added:** 3 test suites: WALWriterTest, WALBackpressureTest, WALWriterThroughputTest
- **Added:** Batched segment writes (`WALManager.appendBatch` / `WALSegment.appendBatch`) for throughput
- **Fixed:** `close()` enqueues a flush-barrier pill instead of stalling a 30s join
- **Updated:** `WALConfig` with queue size settings
- **Updated:** Configuration properties and test mappings
- **Key feature:** LSN allocation at dequeue time for strict monotonicity