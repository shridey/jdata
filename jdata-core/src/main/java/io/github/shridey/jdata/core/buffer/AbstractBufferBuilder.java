package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * The base class for all Buffer Builders.
 * A "Builder" is used when we don't know the final row count of our data upfront (like reading a CSV).
 * It dynamically grows in memory. This base class specifically manages the "Validity Bitmap"
 * (which tracks nulls) because EVERY Arrow column requires one, regardless of its data type.
 */
public abstract sealed class AbstractBufferBuilder permits
        BooleanBufferBuilder, Int32BufferBuilder, Int64BufferBuilder,
        Float32BufferBuilder, Float64BufferBuilder, UTF8BufferBuilder {

    protected final Arena arena;
    protected final String name;

    protected long logicalCapacity;
    protected long logicalLength = 0;

    protected MemorySegment validityBitmap;

    protected AbstractBufferBuilder(Arena arena, String name, long initialLogicalCapacity) {
        this.arena = Objects.requireNonNull(arena, "Arena cannot be null.");
        this.name = Objects.requireNonNull(name, "Column name cannot be null.");
        if (initialLogicalCapacity < 0) {
            throw new IllegalArgumentException("initialLogicalCapacity cannot be negative: " + initialLogicalCapacity);
        }

        // Start with a minimum of 8 rows to avoid fractional byte math
        this.logicalCapacity = Math.max(initialLogicalCapacity, 8L);

        long bitmapBytes = (this.logicalCapacity + 7L) >>> 3;
        this.validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);
        this.validityBitmap.fill((byte) 0xFF);
    }

    /**
     * Checks if we have enough room for new rows. If not, it doubles our capacity
     * and copies the old memory into the new, larger memory block.
     */
    protected void ensureLogicalCapacity(long additionalRows) {
        if (additionalRows < 0) {
            throw new IllegalArgumentException("additionalRows cannot be negative: " + additionalRows);
        }
        long targetLogical = Math.addExact(logicalLength, additionalRows);

        if (targetLogical > logicalCapacity) {
            long doubled = (logicalCapacity > Long.MAX_VALUE / 2) ? Long.MAX_VALUE : logicalCapacity * 2;
            long newLogicalCapacity = Math.max(doubled, targetLogical);

            long oldBitmapBytes = (logicalCapacity + 7L) >>> 3;
            long newBitmapBytes = (newLogicalCapacity + 7L) >>> 3;

            MemorySegment newValidity = ArrowMemoryAllocator.allocateAligned(arena, newBitmapBytes);
            MemorySegment.copy(validityBitmap, 0, newValidity, 0, oldBitmapBytes);
            if (newBitmapBytes > oldBitmapBytes) {
                newValidity.asSlice(oldBitmapBytes, newBitmapBytes - oldBitmapBytes).fill((byte) 0xFF);
            }

            this.validityBitmap = newValidity;
            resizeAdditionalLogicalBuffers(newLogicalCapacity);
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
        long byteIndex = logicalLength >>> 3;
        int bitIndex = (int) (logicalLength & 7L);

        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);
        validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (b & ~(1 << bitIndex)));
    }

    /**
     * Seals the builder into an immutable DataBuffer.
     */
    public abstract DataBuffer build();
}