# Page-Based Storage Layout (R3-002)

This document specifies the binary layout and invariants for the page-based storage layer introduced in Prompt 6 of prompt4.md.

## Overview

The page-based storage layer replaces the current `List<Map<String,Object>>` heap-based storage with fixed-size pages containing slotted tuple data. Each page has a 64-byte header followed by a slot directory and tuple data.

## Page Layout

### Page Header (64 bytes, big-endian)

| Offset | Size  | Field          | Type   | Description |
|--------|-------|----------------|--------|-------------|
| 0      | 4     | magic          | int    | `0x44504745` ("DPGE" in little-endian) |
| 4      | 2     | formatVersion  | short  | `1` (current version) |
| 6      | 1     | pageType       | byte   | `0`=NORMAL, `1`=CATALOG (reserved for Prompt 9) |
| 7      | 1     | flags          | byte   | Reserved (future use) |
| 8      | 8     | tablespaceId   | long   | Tablespace identifier |
| 16     | 8     | fileId         | long   | File identifier within tablespace |
| 24     | 8     | pageNum        | long   | Page number within file |
| 32     | 8     | lsn            | long   | Log sequence number (for WAL, Prompt 11+) |
| 40     | 4     | checksum       | int    | Placeholder (CRC32C in Prompt 52) |
| 44     | 4     | slotCount      | int    | Number of slots in slot directory |
| 48     | 4     | freeSpaceStart | int    | "Free-space-pointer": end of slot directory |
| 52     | 4     | freeSpaceEnd   | int    | Upper bound of tuple data |
| 56     | 4     | pageSize       | int    | Self-describing page size |
| 60     | 4     | reserved       | int    | Must be 0 (future use) |

### Slot Directory

- Starts at offset 64 (immediately after header)
- Each slot entry is 8 bytes: 4 bytes offset + 4 bytes length
- Grows upward with increasing slot IDs
- Maximum slots: `(pageSize - 64) / 8`

### Tuple Data

- Starts at page end (offset = pageSize), grows downward
- Tuples are stored as raw byte arrays (no encoding)
- No alignment padding between tuples (simple v1 design)
- Free space region: `[freeSpaceStart, freeSpaceEnd)`

## Invariants

### Free Space Calculation
```
freeSpace = freeSpaceEnd - freeSpaceStart
usedSpace = pageSize - freeSpace
```

### Insert Operation
1. Place tuple at `freeSpaceEnd - tupleLength`
2. Decrement `freeSpaceEnd` by `tupleLength`
3. Append slot entry at `freeSpaceStart` (increment `freeSpaceStart` by 8)
4. Increment `slotCount` by 1

### Delete Operation
1. Mark slot as deleted: `offset = -1`, `length = 0`
2. `freeSpaceStart` and `freeSpaceEnd` remain unchanged (space reclaimed only at defrag)

### Defragmentation
1. Collect all live tuples (offset != -1) in slot ID order
2. Compact them contiguously against page end:
   - New free space starts at `pageSize - totalLiveBytes`
   - Write tuples in slot ID order from this position upward
3. Update slot offsets to point to new positions
4. Set `freeSpaceEnd = pageSize - totalLiveBytes`
5. Return reclaimed bytes = `oldFreeSpace - newFreeSpace`

## Page Sizes

Supported page sizes (Prompt 6 acceptance criteria):
- 8192 bytes (8K)
- 16384 bytes (16K)
- 65536 bytes (64K)

Configuration key: `page.size` (supports "8K", "16K", "64K" suffixes)

## Page Class API

### Core Operations
- `create(PageId, int pageSize)` - Create new page (strict size validation)
- `readFrom(ByteBuffer)` - Deserialize from buffer (self-describing via header)
- `writeTo(ByteBuffer)` - Serialize to buffer
- `insert(byte[])` - Insert tuple, return slot ID
- `get(int slotId)` - Read tuple from slot (null if deleted)
- `delete(int slotId)` - Mark slot as deleted
- `defrag()` - Compact live tuples, return reclaimed bytes

### Metadata
- `getPageId()` - Returns PageId (tablespaceId/fileId/pageNum)
- `getPageSize()` - Returns page size in bytes
- `getSlotCount()` - Returns number of slots (including deleted)
- `getFreeSpace()` - Returns current free space in bytes
- `isDirty()/setDirty(boolean)` - Track modification status
- `getLsn()/setLsn(long)` - Access log sequence number
- `getChecksum()/setChecksum(int)` - Access checksum field

### Validation
- `validate()` - Checks layout consistency (throws PageFormatException on errors)
- `toBytes()` - Returns defensive copy of full page data

## Error Handling

### PageFormatException
Thrown when:
- Magic number doesn't match `0x44504745`
- Format version is not `1`
- Page size is not in {8192, 16384, 65536}
- Header fields are out of bounds
- Slot/tuple data is corrupt
- Free space accounting is inconsistent

### PageFullException
Thrown when `insert()` cannot fit the tuple in available free space.

## Future Extensions

The reserved fields and page type field support future enhancements:

1. **Prompt 9 (CatalogTable)**: `pageType=1` for system catalog pages
2. **Prompt 52 (CRC32C)**: Replace checksum placeholder with actual CRC32C
3. **Prompt 123-125 (TDE)**: Use reserved bytes for encryption IV
4. **Prompt 11 (WAL)**: LSN field for redo log integration
5. **Prompt 20 (Background Flusher)**: Dirty flag coordination with BufferPool

## Integration Points

### BufferPool (Prompt 7)
- Pages are cached as `Page` objects keyed by `PageId`
- `isDirty()` flag tracks pages needing flush
- `getLsn()` for WAL checkpoint coordination

### PageManager (Prompt 8)
- `readPage(pageId)` → reads from disk via `FileChannel`
- `writePage(page)` → writes to disk, updates LSN/checksum
- `allocatePage(tablespaceId)` → extends file, returns new `PageId`

### CatalogTable (Prompt 9)
- Uses `pageType=1` for system catalog pages
- Schema stored as JSON in tuple data
- Special handling for `pageId=0` in each tablespace

### Tablespace (Prompt 10)
- Files contain pages at `pageNum * pageSize` offsets
- `tablespaceId` in `PageId` maps to directory path

## Performance Characteristics

- **Insert O(1)**: Amortized constant time (slot append + tuple write)
- **Delete O(1)**: Mark slot as deleted (no data movement)
- **Get O(1)**: Direct slot access + tuple read
- **Defrag O(n)**: Linear in number of live tuples
- **Memory overhead**: ~64 bytes per page (header only)

## I/O Modes

PageManager supports configurable I/O modes for page access:

- **positional** (default): Heap ByteBuffer + FileChannel.read/write at exact offsets
- **mapped**: FileChannel.map(READ_ONLY, offset, pageSize) for zero-copy read (fallback to positional on failure)
- **odirect**: Reserved for Linux O_DIRECT (not implemented; falls back to positional with warning)

Mode configuration: `page.io.mode` system property or `config.properties` (default: "positional").

## Testing Strategy

Acceptance criteria (Prompt 6):
1. **PageTest**: 8KB page, 100×50-byte rows, lossless round-trip
2. **DefragTest**: 50 inserts + 25 deletes → defrag restores ≥30% free space
3. **Page size configurable**: 8K/16K/64K pass tests

Acceptance criteria (Prompt 8):
4. **PageManagerTest**: Round-trip I/O, page allocation, 10k-page restart simulation, pool integration
5. **PageManagerCrashTest**: Interrupted atomic writes leave target intact; flushed pages survive crash
6. **PageManagerThroughputTest**: 50,000 pages/sec write rate with 256MB buffer

Test coverage includes:
- Round-trip serialization
- Header field preservation
- Delete/isolation guarantees
- Full-page insertion failure
- Configuration parsing (with fallback)
- Corruption detection
- Defragmentation accuracy and space reclamation
- File I/O mode configuration and fallbacks
- Atomic write/recovery semantics
- BufferPool integration (hit/miss, dirty flush, eviction)
- Performance benchmarks for production readiness