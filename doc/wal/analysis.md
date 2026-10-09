# WAL Analysis Phase (ARIES)

Prompt 4 #17 — R3-004 step 2/4: rebuild the committed/active transaction sets
from the WAL after a crash. See `doc/wal/checkpoint.md` for the checkpoint
record itself.

## Components

| Component | File | Role |
|-----------|------|------|
| `AnalysisPhase` | `diesel/recovery/AnalysisPhase.java` | Scans the WAL from the last checkpoint to the end of the log |
| `AnalysisResult` | `diesel/recovery/AnalysisResult.java` | Immutable output: `{committed, active, lastLSN}` |

## Algorithm

1. **Window** — `startLsn = checkpoint.lastLSN + 1` (the checkpoint entry itself
   is included), `endLsn = wal.getLastLsn()` captured once so a concurrently
   growing WAL does not change the snapshot. No checkpoint → scan from LSN 1.
2. **Seed** — the active set starts from `CheckpointRecord.getActiveTxids()`.
3. **Scan** — every entry with `startLsn <= lsn <= endLsn`:
   - `BEGIN` / `INSERT` / `UPDATE` / `DELETE` / `TRUNCATE` → txid becomes active
     (unless it already committed; DML counts even without BEGIN, since BEGIN
     emission is not wired into the engine yet — prompt 4 #17 reserves opcode 0);
   - `COMMIT` → txid moves to the committed set and leaves the active set;
   - `ABORT` → txid leaves the active set (never committed);
   - `CHECKPOINT` → the active set is re-seeded from the checkpoint's embedded
     list (fuzzy-checkpoint semantics; the committed set keeps accumulating).
4. **Output** — `{committed, active, lastLSN}` where `lastLSN = endLsn`.

Scanning is **segment-by-segment** (`WALSegment.readAll()`): only one segment
body is materialized at a time, so memory stays bounded by the segment size
even for multi-gigabyte WALs.

## Semantics notes

- Only **post-checkpoint** commits land in `committed` — commits before the
  checkpoint are durable and outside the scan window.
- After a clean `close()`, `checkpoint.ptr` holds the LSN of the last
  CHECKPOINT entry (0 if none was ever written), so a restart reloads the
  checkpoint; LSN recovery itself uses the segment scan (`recoverLsn()` takes
  `max(ptr, segment LSNs)`).

## Consumers

- Redo phase (prompt 4 #18): replays records with `checkpoint.lastLSN < lsn <= lastLSN`.
- Undo phase (prompt 4 #19): rolls back the `active` set.

## Tests

| Test | Tag | Coverage |
|------|-----|----------|
| `AnalysisTest` (7) | smoke, storage | 50 commit / 50 no-commit separation, empty WAL, ABORT, checkpoint seeding + scan window, mid-scan checkpoint re-seed, restart persistence, DML-without-BEGIN |
| `AnalysisPerformanceTest` | perf | 1 GiB WAL analyzed in < 5 s (acceptance criterion; override with `-Ddiesel.analysis.perf.bytes=N`) |
