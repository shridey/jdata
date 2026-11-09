package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Dynamically builds a 64-bit floating-point (double) column.
 * Used for streaming data or query filtering where the total row count is unknown.
 */
public final class Float64BufferBuilder extends AbstractBufferBuilder {

    // The physical off-heap memory block storing the 8-byte doubles.
    private MemorySegment dataBuffer;

    public Float64BufferBuilder(Arena arena, String name, long initialCapacity) {
        super(arena, name, initialCapacity);

        // A 64-bit double takes exactly 8 bytes of physical memory.
        long dataBytes = logicalCapacity * ValueLayout.JAVA_DOUBLE.byteSize();
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        // Allocate a new, larger memory segment for the doubles
        long newDataBytes = newLogicalCapacity * ValueLayout.JAVA_DOUBLE.byteSize();
        MemorySegment newData = ArrowMemoryAllocator.allocateAligned(arena, newDataBytes);

        // Copy the old data into the new segment using native OS memory copy
        long currentBytes = logicalCapacity * ValueLayout.JAVA_DOUBLE.byteSize();
        MemorySegment.copy(dataBuffer, 0, newData, 0, currentBytes);

        this.dataBuffer = newData;
    }

    /**
     * Appends a single 64-bit double to the end of the column.
     */
    public Float64BufferBuilder append(double value) {
        ensureLogicalCapacity(1); // Double memory capacity if we are full

        // Calculate exact byte offset. E.g., Row 3 lives at byte 24 (3 * 8).
        long byteOffset = logicalLength * ValueLayout.JAVA_DOUBLE.byteSize();
        dataBuffer.set(ValueLayout.JAVA_DOUBLE, byteOffset, value);

        logicalLength++; // Advance the write-head
        return this;
    }

    /**
     * Appends a null row to the column.
     */
    public Float64BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull(); // Parent flips the validity bit to 0

        // Explicitly write 0.0 to ensure clean memory and predictable compression
        long byteOffset = logicalLength * ValueLayout.JAVA_DOUBLE.byteSize();
        dataBuffer.set(ValueLayout.JAVA_DOUBLE, byteOffset, 0.0);

        logicalLength++;
        return this;
    }

    /**
     * Bulk appends an array of doubles instantly using a native memory copy.
     */
    public Float64BufferBuilder appendAll(double[] values) {
        if (values.length == 0) return this;

        ensureLogicalCapacity(values.length); // Resize exactly once if needed

        long startByteOffset = logicalLength * ValueLayout.JAVA_DOUBLE.byteSize();
        long copyBytes = (long) values.length * ValueLayout.JAVA_DOUBLE.byteSize();

        // Blast the Java heap array directly into the off-heap segment
        MemorySegment heapSegment = MemorySegment.ofArray(values);
        MemorySegment.copy(heapSegment, 0, dataBuffer, startByteOffset, copyBytes);

        logicalLength += values.length;
        return this;
    }

    /**
     * Seals the builder into a read-only, immutable Float64Buffer.
     */
    public Float64Buffer build() {
        // Slice memory strictly to the written boundaries to hide padded bytes
        MemorySegment sealedValidity = validityBitmap.asSlice(0, (logicalLength + 7) / 8);
        MemorySegment sealedData = dataBuffer.asSlice(0, logicalLength * ValueLayout.JAVA_DOUBLE.byteSize());

        return new Float64Buffer(name, arena, logicalLength, sealedValidity, sealedData);
    }
}