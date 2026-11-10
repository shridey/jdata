package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Dynamically builds a 32-bit integer column.
 * Use this class when you are reading data (like a CSV) and do not know
 * how many rows you will have upfront. It grows automatically.
 */
public final class Int32BufferBuilder extends AbstractBufferBuilder {

    // The physical off-heap memory block where our actual numbers live.
    private MemorySegment dataBuffer;

    public Int32BufferBuilder(Arena arena, String name, long initialLogicalCapacity) {
        super(arena, name, initialLogicalCapacity);

        // A 32-bit integer takes exactly 4 bytes.
        // We multiply our row capacity by 4 to get the total required physical bytes.
        long dataBytes = logicalCapacity * ValueLayout.JAVA_INT.byteSize();
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        // The parent class just doubled our row capacity.
        // We must allocate a new, larger data buffer to match.
        long newDataBytes = newLogicalCapacity * ValueLayout.JAVA_INT.byteSize();
        MemorySegment newData = ArrowMemoryAllocator.allocateAligned(arena, newDataBytes);

        // Copy the old data into the new, larger segment using hardware speed.
        long currentBytes = logicalCapacity * ValueLayout.JAVA_INT.byteSize();
        MemorySegment.copy(dataBuffer, 0, newData, 0, currentBytes);

        this.dataBuffer = newData;
    }

    /**
     * Appends a single integer to the end of the column.
     */
    public Int32BufferBuilder append(int value) {
        ensureLogicalCapacity(1); // Make sure we have room for 1 more row

        // Calculate the exact memory byte offset.
        // E.g., Row 5 lives at byte offset 20 (5 * 4 bytes).
        long byteOffset = logicalLength * ValueLayout.JAVA_INT.byteSize();
        dataBuffer.set(ValueLayout.JAVA_INT, byteOffset, value);

        logicalLength++; // Advance the write-head
        return this;
    }

    /**
     * Appends a null row to the column.
     */
    public Int32BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull(); // Tells the parent class to write a '0' in the validity bitmap

        // Arrow spec dictates that physical data under a null bit is arbitrary,
        // but writing an explicit 0 is the safest practice to prevent garbage data leaks.
        long byteOffset = logicalLength * ValueLayout.JAVA_INT.byteSize();
        dataBuffer.set(ValueLayout.JAVA_INT, byteOffset, 0);

        logicalLength++;
        return this;
    }

    /**
     * Bulk appends an entire array of integers at hardware speed.
     * Bypasses row-by-row iteration for massive performance gains.
     */
    public Int32BufferBuilder appendAll(int[] values) {
        if (values.length == 0) return this;

        ensureLogicalCapacity(values.length); // Resize exactly once

        // Calculate where to start writing and exactly how many bytes to move
        long startByteOffset = logicalLength * ValueLayout.JAVA_INT.byteSize();
        long copyBytes = (long) values.length * ValueLayout.JAVA_INT.byteSize();

        // Blast the memory directly from the JVM heap into our off-heap segment instantly
        MemorySegment heapSegment = MemorySegment.ofArray(values);
        MemorySegment.copy(heapSegment, 0, dataBuffer, startByteOffset, copyBytes);

        logicalLength += values.length;
        return this;
    }

    /**
     * Bulk appends data directly from an existing sealed Int32Buffer.
     * Bypasses the JVM heap entirely for zero-allocation, hardware-speed copying.
     */
    public Int32BufferBuilder appendBlock(Int32Buffer source, long srcIndex, long length) {
        if (length <= 0) return this;

        ensureLogicalCapacity(length); // Guarantee we have enough memory

        long byteSize = ValueLayout.JAVA_INT.byteSize();
        long srcPhysical = srcIndex + source.offset();
        long srcByteOffset = srcPhysical * byteSize;
        long destByteOffset = logicalLength * byteSize;
        long copyBytes = length * byteSize;

        // 1. Instantly copy the physical 4-byte integers using native OS speed
        MemorySegment.copy(source.buffer(1), srcByteOffset, this.dataBuffer, destByteOffset, copyBytes);

        // 2. Synchronize the validity bits and advance the write-head
        for (long i = 0; i < length; i++) {
            if (source.isNull(srcIndex + i)) {
                markCurrentNull(); // Parent marks the validity bitmap as 0
            }
            logicalLength++;
        }

        return this;
    }

    /**
     * Safely copies data from a foreign, generic DataBuffer imported via FFI.
     */
    public Int32BufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        // 1. Strict Runtime Guardrail
        if (source.type() != DataType.INT32) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into an Int32 builder.");
        }

        if (length <= 0) return this;
        ensureLogicalCapacity(length);

        long byteSize = ValueLayout.JAVA_INT.byteSize();
        long srcPhysical = srcIndex + source.offset();
        long srcByteOffset = srcPhysical * byteSize;
        long destByteOffset = logicalLength * byteSize;
        long copyBytes = length * byteSize;

        // 2. Buffer 1 contains the physical data in imported Arrow structures
        MemorySegment.copy(source.buffer(1), srcByteOffset, this.dataBuffer, destByteOffset, copyBytes);

        // 3. Sync nulls
        for (long i = 0; i < length; i++) {
            if (source.isNull(srcIndex + i)) {
                markCurrentNull();
            }
            logicalLength++;
        }

        return this;
    }

    /**
     * Seals the builder into a read-only, immutable Int32Buffer.
     */
    public Int32Buffer build() {
        // Slice the memory exactly to the written boundaries.
        // This hides any extra allocated padding from downstream consumers.
        MemorySegment sealedValidity = validityBitmap.asSlice(0, (logicalLength + 7) / 8);
        MemorySegment sealedData = dataBuffer.asSlice(0, logicalLength * ValueLayout.JAVA_INT.byteSize());

        return new Int32Buffer(name, arena, logicalLength, 0, sealedValidity, sealedData);
    }
}