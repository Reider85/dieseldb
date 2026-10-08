# WAL Entry Format (R3-003)

This document specifies the binary layout and invariants for WAL entries introduced in Prompt 11 of prompt4.md.

## Overview

The write-ahead log records every modifying operation before it reaches the data files. Each entry is a self-describing, length-prefixed binary record with a trailing CRC32C checksum, serialized explicitly (`WALEntry.writeTo` / `WALEntry.readFrom`) — no Java object serialization. Segment-level framing (magic, version, file naming `wal-0001.log`) is added by the WALManager prompt (step 12); this document covers the entry itself.

## Entry Layout

### Header + payload (big-endian)

| Offset | Size | Field       | Type   | Description |
|--------|------|-------------|--------|-------------|
| 0      | 8    | lsn         | long   | Monotonic log sequence number |
| 8      | 8    | txid        | long   | Transaction id |
| 16     | 1    | op          | byte   | `WALOpcode` wire code (see below) |
| 17     | 1    | flags       | byte   | Reserved, must be 0 |
| 18     | 2    | reserved    | short  | Reserved, must be 0 |
| 20     | 4    | beforeLen   | int    | Before-image length; 0 = absent |
| 24     | 4    | afterLen    | int    | After-image length; 0 = absent |
| 28     | B    | before-image | byte[] | Pre-image payload (beforeLen bytes) |
| 28+B   | A    | after-image  | byte[] | Post-image payload (afterLen bytes) |
| 28+B+A | 4    | crc32c      | int    | CRC32C over bytes `[0, 28+B+A)` |
| —      | P    | padding     | byte[] | Zero bytes to the next 8-byte boundary |

- **Fixed header size:** 28 bytes (`WALFormat.ENTRY_FIXED_HEADER_SIZE`)
- **Minimum entry:** 32 bytes (empty images; 28 + 4 CRC, already 8-aligned)
- **Alignment:** every entry starts (and the next entry begins) at an 8-byte boundary — required for direct-IO compatibility. Padding `P = (8 - (32+B+A) mod 8) mod 8`, always 0..7 zero bytes.

### Byte diagram (empty images, 32 bytes)

```
 byte:  0        8       16 17 18  20       24       28   32
       +--------+--------+--+--+--+--------+--------+----+
       |  lsn   |  txid  |op|fl|rv|beforeLn| afterLn|CRC |
       +--------+--------+--+--+--+--------+--------+----+
        8 bytes  8 bytes  1  1  2   4 bytes  4 bytes  4
```

## CRC32C

- Algorithm: CRC32C (Castagnoli) via `java.util.zip.CRC32C` (hardware-accelerated on supporting CPUs; JNI acceleration prompt 52).
- **Scope:** the fixed header **plus** both image payloads, i.e. every byte from offset 0 up to (but excluding) the CRC field itself.
- **Excluded:** the zero padding after the CRC field (padding bytes are validated to be zero on read instead).
- On mismatch `WALEntry.readFrom` throws `InvalidCRCException` carrying the expected (computed) and actual (stored) values.

## Opcodes

| Code | Opcode     | Notes |
|------|------------|-------|
| 0    | BEGIN      | Reserved for ARIES analysis phase (prompt 17) |
| 1    | INSERT     | After-image only |
| 2    | UPDATE     | Before- and after-image |
| 3    | DELETE     | Before-image only |
| 4    | COMMIT     | No images |
| 5    | ABORT      | No images |
| 6    | TRUNCATE   | No images |
| 7    | CHECKPOINT | No images (prompt 16) |

Unknown opcode bytes are rejected with `WALFormatException`.

## Validation Rules (read path)

`WALEntry.readFrom(ByteBuffer)` enforces, in order:

1. At least 28 bytes remain for the header — otherwise `WALFormatException`.
2. `flags == 0` and `reserved == 0` — otherwise `WALFormatException`.
3. The opcode byte is a known `WALOpcode` — otherwise `WALFormatException`.
4. `beforeLen >= 0`, `afterLen >= 0`, and `beforeLen + afterLen + 4 <= remaining` — otherwise `WALFormatException`.
5. Recomputed CRC32C equals the stored value — otherwise `InvalidCRCException`.
6. The padding bytes are present and all zero — otherwise `WALFormatException`.

## Image Convention (v1)

Which ops carry which images:

| Op       | before-image | after-image |
|----------|--------------|-------------|
| INSERT   | absent       | row image   |
| UPDATE   | row image    | row image   |
| DELETE   | row image    | absent      |
| others   | absent       | absent      |

`null` images are normalized to zero-length arrays; the two are indistinguishable on the wire (`beforeLen`/`afterLen` = 0).

## API Summary

```java
WALEntry entry = new WALEntry(lsn, txid, WALOpcode.UPDATE, before, after);
int size = entry.encodedSize();          // padded size, multiple of 8
byte[] raw = entry.toBytes();            // exact encodedSize bytes

ByteBuffer buf = ByteBuffer.allocate(size);
entry.writeTo(buf);                      // advances position by encodedSize
buf.rewind();
WALEntry decoded = WALEntry.readFrom(buf); // advances position; verifies CRC
```

`WALEntry` is immutable; image arrays are copied defensively on construction and access. The format constants live in `WALFormat`, opcodes in `WALOpcode`.

## Segment Framing (prompt4.md step 12)

WAL entries are stored in segment files named `wal-NNNN.log` (4-digit zero-padded, 1-based numbering). Each segment begins with a 24-byte header:

### Segment Header Layout (big-endian)

| Offset | Size | Field       | Type   | Description |
|--------|------|-------------|--------|-------------|
| 0      | 4    | magic       | String | "DWAL" (identifies DieselDB WAL segment) |
| 4      | 2    | formatVersion | short  | Segment format version (currently 1) |
| 6      | 2    | reserved    | short  | Must be 0 (future extension point) |
| 8      | 4    | segmentNumber | int   | Segment sequence number (1-based) |
| 12     | 8    | firstLSN    | long   | LSN of first entry in segment (0 if empty) |
| 20     | 4    | crc32c      | int    | CRC32C over bytes [0,20) |

- **Segment file naming**: `wal-0001.log`, `wal-0002.log`, etc. (created sequentially)
- **Segment rotation**: Occurs when an entry would exceed the segment size limit (configurable, default 64MB). The current segment is closed and a new segment is created.
- **Header writing**: Headers are written lazily on the first append to an empty segment (0-byte files are valid empty segments). The `firstLSN` is updated when the first entry is appended.
- **Torn tail handling**: Readers stop at the first incomplete entry (structural error or CRC mismatch), treating partial writes as a valid end-of-segment condition for crash recovery.
- **Checkpoint persistence**: The last appended LSN is persisted to `checkpoint.ptr` in the WAL directory (atomic temp+rename) on flush/close and periodically during appends.

## Compatibility

- Format version is implicit in the entry (no version field yet); segment headers introduced by prompt 12 carry magic + format version.
- The `flags` byte and `reserved` short are the designated extension points: readers must reject non-zero values today, so future versions can flip them only together with a segment-level version bump.
- The `lsn` field pairs with `PageHeader.lsn` (page-based storage, prompt 6) for ARIES redo (prompts 16–19).
