# WAL Checkpoint (ARIES)

Prompt 4 #16 — ARIES Recovery Manager checkpoint: `CheckpointRecord` + atomic `checkpoint.ptr`.

## Components

| Class | File | Purpose |
|-------|------|---------|
| `CheckpointRecord` | `diesel/recovery/CheckpointRecord.java` | Immutable checkpoint: `lastLSN`, `activeTxids[]`, `timestampEpochMs`; binary serialization with CRC32C |
| `CheckpointPointerFile` | `diesel/recovery/CheckpointPointerFile.java` | Atomic 8-byte LSN pointer (`checkpoint.ptr`) via `AtomicFileWriter` |
| `CheckpointFormatException` | `diesel/recovery/CheckpointFormatException.java` | Raised on corrupt/short/mismatched-CRC checkpoint records |

## On-disk layout

### `checkpoint.ptr` (8 bytes)

| Offset | Size | Field | Type |
|--------|------|-------|------|
| 0      | 8    | checkpointLsn | long (big-endian) |

Backward compatible with the pre-ARIES format (same 8-byte LSN), but the semantics changed:
it now points at the LSN of the last **CHECKPOINT WAL entry**, not merely the last appended LSN.

### CheckpointRecord (CHECKPOINT entry after-image)

| Offset | Size | Field | Type |
|--------|------|-------|------|
| 0      | 4    | magic | int (0x44574350 "DWCP") |
| 4      | 2    | version | short |
| 6      | 2    | reserved | short (0) |
| 8      | 8    | lastLSN | long |
| 16     | 8    | timestampEpochMs | long |
| 24     | 4    | activeTxidCount | int |
| 28     | 8*N  | activeTxids | long[] |
| 28+8N  | 4    | crc32c | int over all preceding bytes |

The serialized record is stored as the **after-image** of a `WALOpcode.CHECKPOINT` entry (txid=0, system).

## Write path

```
WALManager.writeCheckpoint(activeTxids):
  1. Build CheckpointRecord(lastAppendedLsn, activeTxids, clock.millis())
  2. Append CHECKPOINT WALEntry (new LSN) to current segment
  3. segment.force()  — record durable before pointer moves
  4. CheckpointPointerFile.write(checkpointLsn)  — atomic update
```

Atomicity: the pointer only advances after the record is durable; a crash between
steps 3 and 4 leaves a stale pointer (older checkpoint still valid).

## Read path

```
Database.initializeWAL() → WALManager.loadCheckpointRecord():
  1. CheckpointPointerFile.read() → LSN (0 = no checkpoint)
  2. readByLsn(LSN); verify op == CHECKPOINT
  3. CheckpointRecord.fromBytes(afterImage)
  Any failure → warn + return null (recover from WAL entries instead)
```

## API

- `WALManager.writeCheckpoint(List<Long> activeTxids)` — explicit checkpoint
- `WALManager.loadCheckpointRecord()` — returns `CheckpointRecord` or `null`
- `TxStatusTracker.getActiveTxids()` — supplies active txids for the checkpoint
- `WALManager.close()` — persists `checkpoint.ptr` for restart recovery

## Tests

- `CheckpointRecordTest` (20) — serialization round-trips, CRC, validation
- `CheckpointPointerFileTest` (5) — atomic write/read, missing file, corrupt size
- `CheckpointIntegrationTest` (3) — write→load, empty txids, multi-checkpoint overwrite
- `WALManagerTest` — `checkpointPtrPersistedOnClose` (close persists pointer)
