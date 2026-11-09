package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * The base class for all Buffer Builders.
 * A "Builder" is used when we don't know the final row count of our data upfront (like reading a CSV).
 * It dynamically grows in memory. This base class specifically manages the "Validity Bitmap"
 * (which tracks nulls) because EVERY Arrow column requires one, regardless of its data type.
 */
public abstract sealed class AbstractBufferBuilder permits
        BooleanBufferBuilder, Int32BufferBuilder, Int64BufferBuilder,
        Float32BufferBuilder, Float64BufferBuilder, UTF8BufferBuilder {

    // The arena controls when memory is freed. The name is the column's logical name (e.g., "age").
    protected final Arena arena;
    protected final String name;

    // 'logicalCapacity' is how many rows we can hold right now before needing to resize.
    // 'logicalLength' is how many rows we have actually written so far.
    protected long logicalCapacity;
    protected long logicalLength = 0;

    // The off-heap memory segment holding the 1-bit-per-row null trackers.
    protected MemorySegment validityBitmap;

    protected AbstractBufferBuilder(Arena arena, String name, long initialLogicalCapacity) {
        this.arena = arena;
        this.name = name;
        // Start with a minimum of 8 rows to avoid fractional byte math
        this.logicalCapacity = Math.max(initialLogicalCapacity, 8);

        // Arrow requires 1 bit per row for the validity bitmap.
        // We divide by 8 to convert bits to bytes, adding 7 to round up for remainders.
        long bitmapBytes = (logicalCapacity + 7) / 8;

        // Allocate off-heap memory aligned to 64 bytes for hardware speed
        this.validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);

        // Arrow spec says 1 means "valid" (not null) and 0 means "null".
        // We fill with 0xFF (all 1s) so every row starts as valid by default.
        this.validityBitmap.fill((byte) 0xFF);
    }

    /**
     * Checks if we have enough room for new rows. If not, it doubles our capacity
     * and copies the old memory into the new, larger memory block.
     */
    protected void ensureLogicalCapacity(long additionalRows) {
        long targetLogical = logicalLength + additionalRows;

        // If the target rows exceed our current limit, we must resize.
        if (targetLogical > logicalCapacity) {
            // Double the capacity (like a standard Java ArrayList) for O(1) amortized appends
            long newLogicalCapacity = Math.max(logicalCapacity * 2, targetLogical);
            long newBitmapBytes = (newLogicalCapacity + 7) / 8;

            // Allocate the new, larger memory block
            MemorySegment newValidity = ArrowMemoryAllocator.allocateAligned(arena, newBitmapBytes);
            newValidity.fill((byte) 0xFF); // Default to valid

            // Native copy of the old bits into the new segment
            MemorySegment.copy(validityBitmap, 0, newValidity, 0, (logicalCapacity + 7) / 8);

            this.validityBitmap = newValidity;

            // Tell the child class (like Int64 or Boolean) to also double its specific data buffers
            resizeAdditionalLogicalBuffers(newLogicalCapacity);

            // Update the capacity tracker
            this.logicalCapacity = newLogicalCapacity;
        }
    }

    /**
     * An abstract hook. When the base class resizes the bitmap, the child class
     * MUST use this method to resize its physical data buffers in lockstep.
     */
    protected abstract void resizeAdditionalLogicalBuffers(long newLogicalCapacity);

    /**
     * Marks the currently appending row as "null" by flipping its bit to 0.
     */
    protected void markCurrentNull() {
        // Find which byte holds our bit, and exactly which of the 8 bits it is
        long byteIndex = logicalLength / 8;
        int bitIndex = (int) (logicalLength % 8);

        // Read the byte, force the specific bit to 0 using a bitwise AND inverse mask, and write it back
        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);
        validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (b & ~(1 << bitIndex)));
    }
}