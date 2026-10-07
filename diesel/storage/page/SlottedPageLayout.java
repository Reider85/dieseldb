package diesel.storage.page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
 * - Insert: place tuple at freeSpaceEnd - length, decrement freeSpaceEnd, append slot
 * - Delete: mark slot offset = -1 (tombstone), freeSpaceEnd unchanged (space reclaimed only at defrag)
 * - Defrag: compact live tuples contiguously against page end, update slot offsets, maximize free space
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
     * Marks a slot as deleted (offset = -1, length = 0).
     * 
     * @param data the full page byte array
     * @param header the page header (updated slotCount if needed)
     * @param slotId the slot index (0-based)
     */
    public static void deleteTuple(byte[] data, PageHeader header, int slotId) {
        writeSlot(data, slotId, EMPTY_SLOT_OFFSET, 0);
        // Note: slotCount remains the same; slot directory doesn't shrink until defrag
    }

    /**
     * Inserts a tuple into the page, allocating space at the end.
     * 
     * @param data the full page byte array
     * @param header the page header (updated)
     * @param tuple the tuple bytes to insert
     * @return the slotId where the tuple was inserted
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
        
        // Update slot directory
        int slotId = header.getSlotCount();
        writeSlot(data, slotId, tupleOffset, tupleLength);
        
        // Update header
        header.setSlotCount(slotId + 1);
        header.setFreeSpaceStart(PageHeader.HEADER_SIZE + (slotId + 1) * SLOT_ENTRY_SIZE);
        header.setFreeSpaceEnd(tupleOffset);
        
        return slotId;
    }

    /**
     * Defragments the page by compacting all live tuples contiguously against the page end.
     * 
     * <p>Algorithm:
     * 1. Collect all live tuples (offset != -1) in slotId order
     * 2. Write them contiguously starting from (pageSize - totalLiveBytes) toward the page end
     * 3. Update slot offsets to point to new positions
     * 4. Update freeSpaceEnd to pageSize - totalLiveBytes
     * 
     * @param data the full page byte array
     * @param header the page header (updated)
     * @return the number of bytes reclaimed (always >= 0)
     */
    public static int defragment(byte[] data, PageHeader header) {
        int pageSize = header.getPageSize();
        int oldFreeSpace = header.getFreeSpace();
        
        // Collect live tuples in slotId order
        List<byte[]> liveTuples = new ArrayList<>();
        List<Integer> liveSlotIds = new ArrayList<>();
        
        for (int slotId = 0; slotId < header.getSlotCount(); slotId++) {
            int offset = readSlotOffset(data, slotId);
            if (offset != EMPTY_SLOT_OFFSET) {
                byte[] tuple = readTuple(data, header, slotId);
                liveTuples.add(tuple);
                liveSlotIds.add(slotId);
            }
        }
        
        int totalLiveBytes = liveTuples.stream().mapToInt(t -> t.length).sum();
        int newFreeSpaceEnd = pageSize - totalLiveBytes;
        
        // If no live tuples, nothing to do
        if (liveTuples.isEmpty()) {
            header.setFreeSpaceEnd(newFreeSpaceEnd);
            return header.getFreeSpace() - oldFreeSpace; // reclaimed = new free space - old free space
        }

        // Write live tuples contiguously from newFreeSpaceEnd upward
        int writePtr = newFreeSpaceEnd;
        for (int i = 0; i < liveTuples.size(); i++) {
            byte[] tuple = liveTuples.get(i);
            int slotId = liveSlotIds.get(i);

            // Write tuple at writePtr
            System.arraycopy(tuple, 0, data, writePtr, tuple.length);

            // Update slot offset
            writeSlot(data, slotId, writePtr, tuple.length);

            writePtr += tuple.length;
        }

        // Update header
        header.setFreeSpaceEnd(newFreeSpaceEnd);

        // Return reclaimed bytes (increase in free space)
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