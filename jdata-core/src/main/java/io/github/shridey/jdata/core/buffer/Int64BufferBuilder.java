package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Dynamically builds a 64-bit signed integer (long) column.
 * Used for streaming data or query filtering where the total row count is unknown.
 */
public final class Int64BufferBuilder extends AbstractBufferBuilder {

    // The physical off-heap memory block storing the 8-byte integers.
    private MemorySegment dataBuffer;

    public Int64BufferBuilder(Arena arena, String name, long initialCapacity) {
        super(arena, name, initialCapacity);

        // A 64-bit integer takes exactly 8 bytes of physical memory.
        // Multiply the logical row capacity by 8 to get total required bytes.
        long dataBytes = logicalCapacity * ValueLayout.JAVA_LONG.byteSize();
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        // Allocate a new, larger memory segment for the integers
        long newDataBytes = newLogicalCapacity * ValueLayout.JAVA_LONG.byteSize();
        MemorySegment newData = ArrowMemoryAllocator.allocateAligned(arena, newDataBytes);

        // Copy the old data into the new segment using native OS memory copy
        long currentBytes = logicalCapacity * ValueLayout.JAVA_LONG.byteSize();
        MemorySegment.copy(dataBuffer, 0, newData, 0, currentBytes);

        this.dataBuffer = newData;
    }

    /**
     * Appends a single 64-bit integer to the end of the column.
     */
    public Int64BufferBuilder append(long value) {
        ensureLogicalCapacity(1); // Double memory capacity if we are full

        // Calculate exact byte offset. E.g., Row 3 lives at byte 24 (3 * 8).
        long byteOffset = logicalLength * ValueLayout.JAVA_LONG.byteSize();
        dataBuffer.set(ValueLayout.JAVA_LONG, byteOffset, value);

        logicalLength++; // Advance the write-head
        return this;
    }

    /**
     * Appends a null row to the column.
     */
    public Int64BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull(); // Parent flips the validity bit to 0

        // Arrow spec allows garbage data under a null bit, but explicitly
        // writing 0 ensures clean memory and predictable compression.
        long byteOffset = logicalLength * ValueLayout.JAVA_LONG.byteSize();
        dataBuffer.set(ValueLayout.JAVA_LONG, byteOffset, 0L);

        logicalLength++;
        return this;
    }

    /**
     * Bulk appends an array of longs instantly using a native memory copy.
     */
    public Int64BufferBuilder appendAll(long[] values) {
        if (values.length == 0) return this;

        ensureLogicalCapacity(values.length); // Resize exactly once if needed

        long startByteOffset = logicalLength * ValueLayout.JAVA_LONG.byteSize();
        long copyBytes = (long) values.length * ValueLayout.JAVA_LONG.byteSize();

        // Blast the Java heap array directly into the off-heap segment
        MemorySegment heapSegment = MemorySegment.ofArray(values);
        MemorySegment.copy(heapSegment, 0, dataBuffer, startByteOffset, copyBytes);

        logicalLength += values.length;
        return this;
    }

    public Int64BufferBuilder appendBlock(Int64Buffer source, long srcIndex, long length) {
        if (length <= 0) return this;

        ensureLogicalCapacity(length);

        long byteSize = ValueLayout.JAVA_LONG.byteSize();
        long srcPhysical = srcIndex + source.offset();
        long srcByteOffset = srcPhysical * byteSize;
        long destByteOffset = logicalLength * byteSize;
        long copyBytes = length * byteSize;

        MemorySegment.copy(source.buffer(1), srcByteOffset, this.dataBuffer, destByteOffset, copyBytes);

        for (long i = 0; i < length; i++) {
            if (source.isNull(srcIndex + i)) {
                markCurrentNull();
            }
            logicalLength++;
        }
        return this;
    }

    /**
     * Safely copies data from a foreign, generic DataBuffer imported via FFI.
     */
    public Int64BufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        if (source.type() != DataType.INT64) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into an Int64 builder.");
        }

        if (length <= 0) return this;
        ensureLogicalCapacity(length);

        long byteSize = ValueLayout.JAVA_LONG.byteSize();
        long srcPhysical = srcIndex + source.offset();
        long srcByteOffset = srcPhysical * byteSize;
        long destByteOffset = logicalLength * byteSize;
        long copyBytes = length * byteSize;

        // Copy physical 8-byte integers from foreign Buffer 1
        MemorySegment.copy(source.buffer(1), srcByteOffset, this.dataBuffer, destByteOffset, copyBytes);

        for (long i = 0; i < length; i++) {
            if (source.isNull(srcIndex + i)) {
                markCurrentNull();
            }
            logicalLength++;
        }

        return this;
    }

    /**
     * Seals the builder into a read-only, immutable Int64Buffer.
     */
    public Int64Buffer build() {
        // Slice memory strictly to the written boundaries to hide padded bytes
        // from downstream Arrow C-Data exports.
        MemorySegment sealedValidity = validityBitmap.asSlice(0, (logicalLength + 7) / 8);
        MemorySegment sealedData = dataBuffer.asSlice(0, logicalLength * ValueLayout.JAVA_LONG.byteSize());

        // Pass 0 as the absolute physical offset for newly built buffers
        return new Int64Buffer(name, arena, logicalLength, 0, sealedValidity, sealedData);
    }
}