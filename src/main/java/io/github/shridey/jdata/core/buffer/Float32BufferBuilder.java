package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Dynamically builds a 32-bit floating-point (float) column.
 * Used for streaming data or query filtering where the total row count is unknown.
 */
public final class Float32BufferBuilder extends AbstractBufferBuilder {

    // The physical off-heap memory block storing the 4-byte floats.
    private MemorySegment dataBuffer;

    public Float32BufferBuilder(Arena arena, String name, long initialCapacity) {
        super(arena, name, initialCapacity);

        // A 32-bit float takes exactly 4 bytes of physical memory.
        long dataBytes = logicalCapacity * ValueLayout.JAVA_FLOAT.byteSize();
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        // Allocate a new, larger memory segment for the floats
        long newDataBytes = newLogicalCapacity * ValueLayout.JAVA_FLOAT.byteSize();
        MemorySegment newData = ArrowMemoryAllocator.allocateAligned(arena, newDataBytes);

        // Copy the old data into the new segment using native OS memory copy
        long currentBytes = logicalCapacity * ValueLayout.JAVA_FLOAT.byteSize();
        MemorySegment.copy(dataBuffer, 0, newData, 0, currentBytes);

        this.dataBuffer = newData;
    }

    /**
     * Appends a single 32-bit float to the end of the column.
     */
    public Float32BufferBuilder append(float value) {
        ensureLogicalCapacity(1); // Double memory capacity if we are full

        // Calculate exact byte offset. E.g., Row 3 lives at byte 12 (3 * 4).
        long byteOffset = logicalLength * ValueLayout.JAVA_FLOAT.byteSize();
        dataBuffer.set(ValueLayout.JAVA_FLOAT, byteOffset, value);

        logicalLength++; // Advance the write-head
        return this;
    }

    /**
     * Appends a null row to the column.
     */
    public Float32BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull(); // Parent flips the validity bit to 0

        // Explicitly write 0.0f to ensure clean memory and predictable compression
        long byteOffset = logicalLength * ValueLayout.JAVA_FLOAT.byteSize();
        dataBuffer.set(ValueLayout.JAVA_FLOAT, byteOffset, 0.0f);

        logicalLength++;
        return this;
    }

    /**
     * Bulk appends an array of floats instantly using a native memory copy.
     */
    public Float32BufferBuilder appendAll(float[] values) {
        if (values.length == 0) return this;

        ensureLogicalCapacity(values.length); // Resize exactly once if needed

        long startByteOffset = logicalLength * ValueLayout.JAVA_FLOAT.byteSize();
        long copyBytes = (long) values.length * ValueLayout.JAVA_FLOAT.byteSize();

        // Blast the Java heap array directly into the off-heap segment
        MemorySegment heapSegment = MemorySegment.ofArray(values);
        MemorySegment.copy(heapSegment, 0, dataBuffer, startByteOffset, copyBytes);

        logicalLength += values.length;
        return this;
    }

    /**
     * Seals the builder into a read-only, immutable Float32Buffer.
     */
    public Float32Buffer build() {
        // Slice memory strictly to the written boundaries to hide padded bytes
        MemorySegment sealedValidity = validityBitmap.asSlice(0, (logicalLength + 7) / 8);
        MemorySegment sealedData = dataBuffer.asSlice(0, logicalLength * ValueLayout.JAVA_FLOAT.byteSize());

        return new Float32Buffer(name, arena, logicalLength, sealedValidity, sealedData);
    }
}