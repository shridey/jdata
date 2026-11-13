package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * Dynamically builds an off-heap 32-bit signed integer {@link Int32Buffer}.
 */
public final class Int32BufferBuilder extends AbstractBufferBuilder {

    private MemorySegment dataBuffer;

    /**
     * Creates a new {@link Int32BufferBuilder} with the specified initial row capacity.
     *
     * @param arena                  the memory arena governing allocations
     * @param name                   the column name
     * @param initialLogicalCapacity the initial logical row capacity
     */
    public Int32BufferBuilder(Arena arena, String name, long initialLogicalCapacity) {
        super(arena, name, initialLogicalCapacity);

        long dataBytes = Math.multiplyExact(logicalCapacity, ValueLayout.JAVA_INT.byteSize());
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        long newDataBytes = Math.multiplyExact(newLogicalCapacity, ValueLayout.JAVA_INT.byteSize());
        long currentBytes = logicalLength * ValueLayout.JAVA_INT.byteSize();
        this.dataBuffer = ArrowMemoryAllocator.reallocateAligned(arena, dataBuffer, currentBytes, newDataBytes);
    }

    /**
     * Appends a single non-null {@code int} value.
     *
     * @param value the {@code int} value to append
     * @return this builder
     */
    public Int32BufferBuilder append(int value) {
        ensureLogicalCapacity(1);

        long byteOffset = logicalLength * ValueLayout.JAVA_INT.byteSize();
        dataBuffer.set(ValueLayout.JAVA_INT, byteOffset, value);

        logicalLength++;
        return this;
    }

    /**
     * Appends a single null entry.
     *
     * @return this builder
     */
    public Int32BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull();
        logicalLength++;
        return this;
    }

    /**
     * Bulk-appends a Java {@code int[]} via vectorized off-heap memory copy.
     *
     * @param values the array of {@code int} values to append
     * @return this builder
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
     * Appends a contiguous range of elements from an existing {@link Int32Buffer}.
     *
     * @param source   the source buffer
     * @param srcIndex the starting logical index in {@code source}
     * @param length   the number of elements to append
     * @return this builder
     */
    public Int32BufferBuilder appendBlock(Int32Buffer source, long srcIndex, long length) {
        return appendImportedBlock(source, srcIndex, length);
    }

    /**
     * Appends a range of elements from any {@link DataBuffer} of type {@link DataType#INT32}.
     *
     * @param source   the source buffer
     * @param srcIndex the starting logical index in {@code source}
     * @param length   the number of elements to append
     * @return this builder
     */
    public Int32BufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        if (source.type() != DataType.INT32) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into an Int32 builder.");
        }
        Objects.checkFromIndexSize(srcIndex, length, source.length());
        if (length == 0) return this;

        if (source instanceof ChunkedBuffer cb) {
            for (DataBuffer chunk : cb.slice(srcIndex, length).chunks()) {
                appendImportedBlock(chunk, 0, chunk.length());
            }
            return this;
        }

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

    @Override
    public Int32Buffer build() {
        long paddedValidityBytes = ArrowMemoryAllocator.padTo64((logicalLength + 7L) >>> 3);
        long paddedDataBytes = ArrowMemoryAllocator.padTo64(logicalLength * ValueLayout.JAVA_INT.byteSize());

        MemorySegment sealedValidity = validityBitmap.asSlice(0, paddedValidityBytes);
        MemorySegment sealedData = dataBuffer.asSlice(0, paddedDataBytes);

        return new Int32Buffer(name, arena, logicalLength, 0, sealedValidity, sealedData);
    }
}