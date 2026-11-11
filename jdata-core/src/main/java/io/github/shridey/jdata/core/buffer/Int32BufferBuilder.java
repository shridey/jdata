package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * Dynamically builds a 32-bit integer column.
 * Use this class when you are reading data (like a CSV) and do not know
 * how many rows you will have upfront. It grows automatically.
 */
public final class Int32BufferBuilder extends AbstractBufferBuilder {

    private MemorySegment dataBuffer;

    public Int32BufferBuilder(Arena arena, String name, long initialLogicalCapacity) {
        super(arena, name, initialLogicalCapacity);

        long dataBytes = Math.multiplyExact(logicalCapacity, ValueLayout.JAVA_INT.byteSize());
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        long newDataBytes = Math.multiplyExact(newLogicalCapacity, ValueLayout.JAVA_INT.byteSize());
        MemorySegment newData = ArrowMemoryAllocator.allocateAligned(arena, newDataBytes);

        long currentBytes = logicalLength * ValueLayout.JAVA_INT.byteSize();
        MemorySegment.copy(dataBuffer, 0, newData, 0, currentBytes);

        this.dataBuffer = newData;
    }

    /**
     * Appends a single integer to the end of the column.
     */
    public Int32BufferBuilder append(int value) {
        ensureLogicalCapacity(1);

        long byteOffset = logicalLength * ValueLayout.JAVA_INT.byteSize();
        dataBuffer.set(ValueLayout.JAVA_INT, byteOffset, value);

        logicalLength++;
        return this;
    }

    /**
     * Appends a null row to the column.
     */
    public Int32BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull();
        logicalLength++;
        return this;
    }

    /**
     * Bulk appends an entire array of integers at hardware speed.
     */
    public Int32BufferBuilder appendAll(int[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (values.length == 0) return this;

        ensureLogicalCapacity(values.length);

        long startByteOffset = logicalLength * ValueLayout.JAVA_INT.byteSize();
        long copyBytes = (long) values.length * ValueLayout.JAVA_INT.byteSize();

        MemorySegment heapSegment = MemorySegment.ofArray(values);
        MemorySegment.copy(heapSegment, 0, dataBuffer, startByteOffset, copyBytes);

        logicalLength += values.length;
        return this;
    }

    /**
     * Bulk appends data directly from an existing sealed Int32Buffer.
     */
    public Int32BufferBuilder appendBlock(Int32Buffer source, long srcIndex, long length) {
        return appendImportedBlock(source, srcIndex, length);
    }

    /**
     * Safely copies data from a DataBuffer.
     */
    public Int32BufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        if (source.type() != DataType.INT32) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into an Int32 builder.");
        }
        Objects.checkFromIndexSize(srcIndex, length, source.length());
        if (length == 0) return this;

        ensureLogicalCapacity(length);

        long byteSize = ValueLayout.JAVA_INT.byteSize();
        long srcPhysical = srcIndex + source.offset();
        long srcByteOffset = srcPhysical * byteSize;
        long destByteOffset = logicalLength * byteSize;
        long copyBytes = length * byteSize;

        MemorySegment.copy(source.buffer(1), srcByteOffset, this.dataBuffer, destByteOffset, copyBytes);

        if (source.nullCount() == 0) {
            logicalLength += length;
        } else {
            for (long i = 0; i < length; i++) {
                if (source.isNull(srcIndex + i)) {
                    markCurrentNull();
                }
                logicalLength++;
            }
        }

        return this;
    }

    /**
     * Seals the builder into a read-only, immutable Int32Buffer.
     */
    @Override
    public Int32Buffer build() {
        MemorySegment sealedValidity = validityBitmap.asSlice(0, (logicalLength + 7L) >>> 3);
        MemorySegment sealedData = dataBuffer.asSlice(0, logicalLength * ValueLayout.JAVA_INT.byteSize());

        return new Int32Buffer(name, arena, logicalLength, 0, sealedValidity, sealedData);
    }
}