package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * The sealed base class for all contiguous columnar buffer builders in {@code jdata-core}.
 *
 * <p>Manages dynamic capacity expansion, 64-byte alignment and tail padding, and the
 * off-heap validity bitmap for null tracking.
 */
public abstract sealed class AbstractBufferBuilder permits
        BooleanBufferBuilder, Int32BufferBuilder, Int64BufferBuilder,
        Float32BufferBuilder, Float64BufferBuilder, UTF8BufferBuilder, DictionaryBufferBuilder {

    /** The memory arena governing the off-heap allocations of this builder. */
    protected final Arena arena;

    /** The column name assigned to the buffer being built. */
    protected final String name;

    /** The current logical row capacity before a resize is required. */
    protected long logicalCapacity;

    /** The number of logical rows appended so far. */
    protected long logicalLength = 0;

    /** The off-heap validity bitmap segment (1 bit per row, LSB-0 ordered, 1 = valid, 0 = null). */
    protected MemorySegment validityBitmap;

    /**
     * Initializes the base builder state and allocates an initial 64-byte-aligned validity bitmap.
     *
     * @param arena                  the memory arena governing allocations
     * @param name                   the column name
     * @param initialLogicalCapacity the initial row capacity (clamped to at least 8 rows)
     * @throws NullPointerException     if {@code arena} or {@code name} is {@code null}
     * @throws IllegalArgumentException if {@code initialLogicalCapacity < 0}
     */
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

    /**
     * Ensures that this builder can accommodate at least {@code additionalRows} more elements,
     * expanding the underlying off-heap data and validity buffers if necessary.
     *
     * @param additionalRows the number of additional rows to reserve space for
     * @throws IllegalArgumentException if {@code additionalRows < 0}
     */
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

    /**
     * Resizes type-specific physical data or offset buffers to accommodate {@code newLogicalCapacity} rows.
     *
     * @param newLogicalCapacity the new target logical row capacity
     */
    protected abstract void resizeAdditionalLogicalBuffers(long newLogicalCapacity);

    /**
     * Clears the validity bit at {@link #logicalLength} to mark the current row as null.
     */
    protected void markCurrentNull() {
        long byteIndex = logicalLength >>> 3;
        int bitIndex = (int) (logicalLength & 7L);

        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);
        validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (b & ~(1 << bitIndex)));
    }

    /**
     * Returns the column name of the buffer being constructed.
     *
     * @return the column name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the number of rows appended to this builder so far.
     *
     * @return the current logical row count
     */
    public long length() {
        return logicalLength;
    }

    /**
     * Returns the current logical row capacity of this builder.
     *
     * @return the current logical capacity
     */
    public long capacity() {
        return logicalCapacity;
    }

    /**
     * Seals the accumulated off-heap segments into an immutable, 64-byte tail-padded {@link DataBuffer}.
     *
     * @return the constructed {@link DataBuffer}
     */
    public abstract DataBuffer build();
}