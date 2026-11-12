package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * The base class for all Buffer Builders.
 */
public abstract sealed class AbstractBufferBuilder permits
        BooleanBufferBuilder, Int32BufferBuilder, Int64BufferBuilder,
        Float32BufferBuilder, Float64BufferBuilder, UTF8BufferBuilder, DictionaryBufferBuilder {

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

        this.logicalCapacity = Math.max(initialLogicalCapacity, 8L);

        long bitmapBytes = (this.logicalCapacity + 7L) >>> 3;
        this.validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);
        this.validityBitmap.fill((byte) 0xFF);
    }

    protected void ensureLogicalCapacity(long additionalRows) {
        if (additionalRows < 0) {
            throw new IllegalArgumentException("additionalRows cannot be negative: " + additionalRows);
        }
        long targetLogical = Math.addExact(logicalLength, additionalRows);

        if (targetLogical > logicalCapacity) {
            long doubled = (logicalCapacity > Long.MAX_VALUE / 2) ? Long.MAX_VALUE : logicalCapacity * 2;
            long newLogicalCapacity = Math.max(doubled, targetLogical);

            // 1. Resize the child data buffer FIRST while it is still at the tip of the slab
            resizeAdditionalLogicalBuffers(newLogicalCapacity);

            // 2. Only reallocate validityBitmap if required bytes exceed current physical capacity (512 bits per 64B).
            // Grow bitmap by 8x (64B -> 512B -> 4096B) so it doesn't leapfrog dataBuffer on every doubling.
            long requiredBitmapBytes = (newLogicalCapacity + 7L) >>> 3;
            if (requiredBitmapBytes > validityBitmap.byteSize()) {
                long oldPhysicalBytes = validityBitmap.byteSize();
                long scaledBitmapBytes = Math.clamp(oldPhysicalBytes * 8L, requiredBitmapBytes, Integer.MAX_VALUE);
                MemorySegment newValidity = ArrowMemoryAllocator.reallocateAligned(
                        arena, validityBitmap, oldPhysicalBytes, scaledBitmapBytes
                );
                newValidity.asSlice(oldPhysicalBytes, newValidity.byteSize() - oldPhysicalBytes).fill((byte) 0xFF);
                this.validityBitmap = newValidity;
            }

            this.logicalCapacity = newLogicalCapacity;
        }
    }

    protected abstract void resizeAdditionalLogicalBuffers(long newLogicalCapacity);

    protected void markCurrentNull() {
        long byteIndex = logicalLength >>> 3;
        int bitIndex = (int) (logicalLength & 7L);

        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);
        validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (b & ~(1 << bitIndex)));
    }

    public abstract DataBuffer build();
}