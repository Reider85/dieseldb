package diesel.storage.page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Slotted page layout utilities for page-based storage (R3-002).
 * 
 * <p>Provides algorithms for tuple insertion, deletion, defragmentation, and slot management.
 * Operates on byte arrays representing pages and PageHeader instances.
 * 
 * <p>Layout invariant:
 * - Slot directory starts at offset PageHeader.HEADER_SIZE (64), grows upward (8 bytes per slot)
 * - Tuple data starts at page end, grows downward (decreasing offsets)
 * - freeSpaceStart = end of slot directory (64 + slotCount * 8)
 * - freeSpaceEnd = lowest offset of any live tuple (or pageSize if empty)
 * - Free space = freeSpaceEnd - freeSpaceStart
 * - Insert: place tuple at freeSpaceEnd - length, decrement freeSpaceEnd; reuse a
 *   tombstoned slot id popped from the free-slot stack, else append a new slot
 * - Delete: mark slot offset = -1 (tombstone) and push its id onto the free-slot
 *   stack; freeSpaceEnd unchanged (tuple space reclaimed only at defrag)
 * - Defrag: compact live tuples contiguously against page end, renumber live slots to
 *   a contiguous 0..L-1 directory (slotCount = L, tombstone slots reclaimed), maximize free space.
 *   Slot ids are page-local and transient (o2 Buffer-6): no structure outside the page
 *   persists them, so renumbering cannot invalidate external references.
 * - Free-slot stack (o2 Buffer-7): an in-memory accelerator held by PageHeader
 *   (NOT persisted), rebuilt lazily from directory tombstones on first insert/delete
 *   after create/load/copy, and reset by defragment. Reuse keeps slotCount bounded
 *   by the high-water mark of concurrent live tuples instead of historical inserts.
 *   INVARIANT: the stack is only mutated through insertTuple/deleteTuple/defragment -
 *   direct writeSlot callers would desynchronize it (none exist outside this class).
 */
public final class SlottedPageLayout {

    // Constants
    public static final int SLOT_ENTRY_SIZE = 8; // 4 bytes offset + 4 bytes length
    public static final int EMPTY_SLOT_OFFSET = -1;

    private SlottedPageLayout() {
        throw new AssertionError("No instances");
    }

    /**
     * Computes the offset of a slot in the page's slot directory.
     * 
     * @param slotId the slot index (0-based)
     * @return byte offset in the page array
     */
    public static int slotOffset(int slotId) {
        return PageHeader.HEADER_SIZE + slotId * SLOT_ENTRY_SIZE;
    }

    /**
     * Reads the tuple offset from a slot entry.
     * 
     * @param data the full page byte array
     * @param slotId the slot index (0-based)
     * @return tuple offset (EMPTY_SLOT_OFFSET if deleted/empty)
     */
    public static int readSlotOffset(byte[] data, int slotId) {
        int offset = slotOffset(slotId);
        if (offset + 4 > data.length) {
            throw new IllegalArgumentException("Slot " + slotId + " exceeds page bounds");
        }
        ByteBuffer bb = ByteBuffer.wrap(data, offset, 4);
        bb.order(java.nio.ByteOrder.BIG_ENDIAN);
        return bb.getInt();
    }

    /**
     * Reads the tuple length from a slot entry.
     * 
     * @param data the full page byte array
     * @param slotId the slot index (0-based)
     * @return tuple length (0 if empty)
     */
    public static int readSlotLength(byte[] data, int slotId) {
        int offset = slotOffset(slotId) + 4;
        if (offset + 4 > data.length) {
            throw new IllegalArgumentException("Slot " + slotId + " length exceeds page bounds");
        }
        ByteBuffer bb = ByteBuffer.wrap(data, offset, 4);
        bb.order(java.nio.ByteOrder.BIG_ENDIAN);
        return bb.getInt();
    }

    /**
     * Writes a slot entry (offset and length).
     * 
     * @param data the full page byte array
     * @param slotId the slot index (0-based)
     * @param offset tuple offset (EMPTY_SLOT_OFFSET for deleted)
     * @param length tuple length (0 for empty)
     */
    public static void writeSlot(byte[] data, int slotId, int offset, int length) {
        int slotPos = slotOffset(slotId);
        if (slotPos + SLOT_ENTRY_SIZE > data.length) {
            throw new IllegalArgumentException("Slot " + slotId + " write exceeds page bounds");
        }
        
        ByteBuffer bb = ByteBuffer.wrap(data, slotPos, SLOT_ENTRY_SIZE);
        bb.order(java.nio.ByteOrder.BIG_ENDIAN);
        bb.putInt(offset);
        bb.putInt(length);
    }

    /**
     * Reads a tuple from a slot.
     * 
     * @param data the full page byte array
     * @param header the page header (for bounds checking)
     * @param slotId the slot index (0-based)
     * @return tuple bytes, or null if slot is deleted/empty
     * @throws PageFormatException if slot data is corrupt (offset out of bounds)
     */
    public static byte[] readTuple(byte[] data, PageHeader header, int slotId) {
        if (slotId >= header.getSlotCount()) {
            return null; // slot beyond the live directory (defrag renumbers to 0..L-1, o2 Buffer-6)
        }
        int offset = readSlotOffset(data, slotId);
        if (offset == EMPTY_SLOT_OFFSET) {
            return null; // deleted slot
        }
        
        int length = readSlotLength(data, slotId);
        if (length == 0) {
            return null; // empty slot
        }
        
        // Validate bounds
        if (offset < 0 || offset + length > data.length) {
            throw new PageFormatException("Tuple at slot " + slotId + " has invalid bounds: offset=" + offset + ", length=" + length);
        }
        
        byte[] tuple = new byte[length];
        System.arraycopy(data, offset, tuple, 0, length);
        return tuple;
    }

    /**
     * Marks a slot as deleted (offset = -1, length = 0) and registers its id
     * on the free-slot stack for reuse by the next insert (o2 Buffer-7).
     *
     * @param data the full page byte array
     * @param header the page header (updated slotCount if needed)
     * @param slotId the slot index (0-based)
     */
    public static void deleteTuple(byte[] data, PageHeader header, int slotId) {
        if (slotId < 0 || slotId >= header.getSlotCount()) {
            return; // outside the live directory - nothing to delete
        }
        if (readSlotOffset(data, slotId) == EMPTY_SLOT_OFFSET) {
            return; // already a tombstone - do not push twice (double reuse would corrupt data)
        }
        writeSlot(data, slotId, EMPTY_SLOT_OFFSET, 0);
        ensureFreeSlotsBuilt(data, header);
        header.pushFreeSlot(slotId);
        // Note: slotCount remains the same; the directory doesn't shrink until defrag
    }

    /**
     * Inserts a tuple into the page, allocating space at the end.
     *
     * <p>Reuses a tombstoned slot id popped from the free-slot stack when one is
     * available (o2 Buffer-7), so slotCount stays bounded by the high-water mark
     * of concurrent live tuples; otherwise appends a new slot as before.
     *
     * @param data the full page byte array
     * @param header the page header (updated)
     * @param tuple the tuple bytes to insert
     * @return the slotId where the tuple was inserted (a reused tombstone id, or slotCount++)
     * @throws PageFullException if there is not enough free space
     */
    public static int insertTuple(byte[] data, PageHeader header, byte[] tuple) {
        int tupleLength = tuple.length;
        int freeSpace = header.getFreeSpace();
        
        if (tupleLength > freeSpace) {
            throw new PageFullException("Cannot insert tuple of length " + tupleLength + 
                    " into page with only " + freeSpace + " free bytes");
        }
        
        // Allocate space at freeSpaceEnd - tupleLength
        int tupleOffset = header.getFreeSpaceEnd() - tupleLength;
        
        // Write tuple data
        System.arraycopy(tuple, 0, data, tupleOffset, tupleLength);
        
        // Update slot directory: reuse a tombstoned slot if available, else append
        ensureFreeSlotsBuilt(data, header);
        int slotId = header.pollFreeSlot();
        if (slotId >= 0) {
            writeSlot(data, slotId, tupleOffset, tupleLength);
            // slotCount and freeSpaceStart unchanged: the directory already covers this slot
        } else {
            slotId = header.getSlotCount();
            writeSlot(data, slotId, tupleOffset, tupleLength);
            header.setSlotCount(slotId + 1);
            header.setFreeSpaceStart(PageHeader.HEADER_SIZE + (slotId + 1) * SLOT_ENTRY_SIZE);
        }
        header.setFreeSpaceEnd(tupleOffset);
        
        return slotId;
    }

    /**
     * Builds the free-slot stack by scanning the slot directory for tombstones,
     * once per page lifetime after create/load/copy (o2 Buffer-7). O(slotCount)
     * once, then insert/delete are O(1).
     */
    private static void ensureFreeSlotsBuilt(byte[] data, PageHeader header) {
        if (header.freeSlotsInitialized()) {
            return;
        }
        for (int slotId = 0; slotId < header.getSlotCount(); slotId++) {
            if (readSlotOffset(data, slotId) == EMPTY_SLOT_OFFSET) {
                header.pushFreeSlot(slotId);
            }
        }
        header.markFreeSlotsInitialized();
    }

    /**
     * Defragments the page by compacting all live tuples and renumbering the
     * slot directory to a contiguous 0..L-1 range (o2 Buffer-6).
     *
     * <p>Algorithm:
     * 1. Collect all live tuples (offset != -1) in slotId order
     * 2. Write them contiguously starting from (pageSize - totalLiveBytes) toward the page end
     * 3. Rewrite the slot directory: the i-th live tuple gets slotId i (L = live count),
     *    so tombstone slots disappear and slotCount shrinks to L
     * 4. Reclaim the tombstone tail of the slot directory (freeSpaceStart = HEADER_SIZE + L * 8)
     * 5. Update freeSpaceEnd to pageSize - totalLiveBytes
     *
     * <p>Slot ids are page-local and transient: no structure outside the page
     * persists them (the only production consumer, CatalogTable, discards the
     * insert slotId and re-scans slots on read), so renumbering cannot
     * invalidate external references. After this call, every O(slotCount)
     * operation (validateLayout, full scans, subsequent defrags) costs
     * O(live count) instead of O(historical inserts).
     *
     * @param data the full page byte array
     * @param header the page header (updated)
     * @return the number of bytes reclaimed (always >= 0; includes both tuple
     *         space and reclaimed slot-directory entries)
     */
    public static int defragment(byte[] data, PageHeader header) {
        int pageSize = header.getPageSize();
        int oldFreeSpace = header.getFreeSpace();
        int oldFreeSpaceStart = header.getFreeSpaceStart();

        // Collect live tuples in slotId order
        List<byte[]> liveTuples = new ArrayList<>();
        for (int slotId = 0; slotId < header.getSlotCount(); slotId++) {
            int offset = readSlotOffset(data, slotId);
            if (offset != EMPTY_SLOT_OFFSET) {
                byte[] tuple = readTuple(data, header, slotId);
                liveTuples.add(tuple);
            }
        }

        int totalLiveBytes = liveTuples.stream().mapToInt(t -> t.length).sum();
        int newFreeSpaceEnd = pageSize - totalLiveBytes;
        int newSlotCount = liveTuples.size();
        int newFreeSpaceStart = PageHeader.HEADER_SIZE + newSlotCount * SLOT_ENTRY_SIZE;

        // Write live tuples contiguously from newFreeSpaceEnd upward; slot i holds the i-th live tuple
        int writePtr = newFreeSpaceEnd;
        for (int i = 0; i < newSlotCount; i++) {
            byte[] tuple = liveTuples.get(i);
            System.arraycopy(tuple, 0, data, writePtr, tuple.length);
            writeSlot(data, i, writePtr, tuple.length);
            writePtr += tuple.length;
        }

        // Zero the reclaimed tail of the slot directory (old entries L..oldSlotCount-1)
        if (newFreeSpaceStart < oldFreeSpaceStart) {
            Arrays.fill(data, newFreeSpaceStart, oldFreeSpaceStart, (byte) 0);
        }

        // Update header: directory shrinks to the live count, tombstones are gone
        header.setSlotCount(newSlotCount);
        header.setFreeSpaceStart(newFreeSpaceStart);
        header.setFreeSpaceEnd(newFreeSpaceEnd);
        // No tombstones remain after renumbering - reset the free-slot stack (o2 Buffer-7)
        header.resetFreeSlotsAfterCompaction();

        // Return reclaimed bytes (increase in free space: tuple bytes + tombstone directory entries)
        return header.getFreeSpace() - oldFreeSpace;
    }

    /**
     * Validates the consistency of a slotted page layout.
     * 
     * @param data the full page byte array
     * @param header the page header
     * @return true if the layout is consistent
     * @throws PageFormatException if inconsistencies are found
     */
    public static boolean validateLayout(byte[] data, PageHeader header) {
        // Basic header validation
        if (!header.isValid()) {
            return false;
        }
        
        // Check each slot
        for (int slotId = 0; slotId < header.getSlotCount(); slotId++) {
            int offset = readSlotOffset(data, slotId);
            int length = readSlotLength(data, slotId);
            
            if (offset == EMPTY_SLOT_OFFSET) {
                if (length != 0) {
                    throw new PageFormatException("Slot " + slotId + " has empty offset but non-zero length: " + length);
                }
                continue; // deleted slot is valid
            }
            
            if (length <= 0) {
                throw new PageFormatException("Slot " + slotId + " has non-empty offset but invalid length: " + length);
            }
            
            // Check tuple bounds
            if (offset < 0 || offset + length > data.length) {
                throw new PageFormatException("Slot " + slotId + " tuple out of bounds: offset=" + offset + ", length=" + length);
            }
            
            // Check if tuple is within free space (should not overlap with free space)
            if (offset >= header.getFreeSpaceStart() && offset < header.getFreeSpaceEnd()) {
                throw new PageFormatException("Slot " + slotId + " tuple overlaps with free space");
            }
        }
        
        // Check free space accounting
        int expectedFreeSpace = header.getFreeSpaceEnd() - header.getFreeSpaceStart();
        int actualFreeSpace = data.length - header.getUsedSpace();
        
        if (expectedFreeSpace != actualFreeSpace) {
            throw new PageFormatException("Free space mismatch: expected " + expectedFreeSpace + ", actual " + actualFreeSpace);
        }
        
        return true;
    }
}