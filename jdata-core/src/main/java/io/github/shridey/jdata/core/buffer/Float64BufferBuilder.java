package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * Dynamically builds an off-heap 64-bit floating-point {@link Float64Buffer}.
 */
public final class Float64BufferBuilder extends AbstractBufferBuilder {

    private MemorySegment dataBuffer;

    /**
     * Creates a new {@link Float64BufferBuilder} with the specified initial row capacity.
     *
     * @param arena           the memory arena governing allocations
     * @param name            the column name
     * @param initialCapacity the initial logical row capacity
     */
    public Float64BufferBuilder(Arena arena, String name, long initialCapacity) {
        super(arena, name, initialCapacity);

        long dataBytes = Math.multiplyExact(logicalCapacity, ValueLayout.JAVA_DOUBLE.byteSize());
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        long newDataBytes = Math.multiplyExact(newLogicalCapacity, ValueLayout.JAVA_DOUBLE.byteSize());
        long currentBytes = logicalLength * ValueLayout.JAVA_DOUBLE.byteSize();
        this.dataBuffer = ArrowMemoryAllocator.reallocateAligned(arena, dataBuffer, currentBytes, newDataBytes);
    }

    /**
     * Appends a single non-null {@code double} value.
     *
     * @param value the {@code double} value to append
     * @return this builder
     */
    public Float64BufferBuilder append(double value) {
        ensureLogicalCapacity(1);

        long byteOffset = logicalLength * ValueLayout.JAVA_DOUBLE.byteSize();
        dataBuffer.set(ValueLayout.JAVA_DOUBLE, byteOffset, value);

        logicalLength++;
        return this;
    }

    /**
     * Appends a single null entry.
     *
     * @return this builder
     */
    public Float64BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull();
        logicalLength++;
        return this;
    }

    /**
     * Bulk-appends a Java {@code double[]} via vectorized off-heap memory copy.
     *
     * @param values the array of {@code double} values to append
     * @return this builder
     */
    public Float64BufferBuilder appendAll(double[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (values.length == 0) return this;

        ensureLogicalCapacity(values.length);

        long startByteOffset = logicalLength * ValueLayout.JAVA_DOUBLE.byteSize();
        long copyBytes = (long) values.length * ValueLayout.JAVA_DOUBLE.byteSize();

        MemorySegment heapSegment = MemorySegment.ofArray(values);
        MemorySegment.copy(heapSegment, 0, dataBuffer, startByteOffset, copyBytes);

        logicalLength += values.length;
        return this;
    }

    /**
     * Appends a contiguous range of elements from an existing {@link Float64Buffer}.
     *
     * @param source   the source buffer
     * @param srcIndex the starting logical index in {@code source}
     * @param length   the number of elements to append
     * @return this builder
     */
    public Float64BufferBuilder appendBlock(Float64Buffer source, long srcIndex, long length) {
        return appendImportedBlock(source, srcIndex, length);
    }

    /**
     * Appends a range of elements from any {@link DataBuffer} of type {@link DataType#FLOAT64}.
     *
     * @param source   the source buffer
     * @param srcIndex the starting logical index in {@code source}
     * @param length   the number of elements to append
     * @return this builder
     */
    public Float64BufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        if (source.type() != DataType.FLOAT64) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into a Float64 builder.");
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

        long byteSize = ValueLayout.JAVA_DOUBLE.byteSize();
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
    public Float64Buffer build() {
        long paddedValidityBytes = ArrowMemoryAllocator.padTo64((logicalLength + 7L) >>> 3);
        long paddedDataBytes = ArrowMemoryAllocator.padTo64(logicalLength * ValueLayout.JAVA_DOUBLE.byteSize());

        MemorySegment sealedValidity = validityBitmap.asSlice(0, paddedValidityBytes);
        MemorySegment sealedData = dataBuffer.asSlice(0, paddedDataBytes);

        return new Float64Buffer(name, arena, logicalLength, 0, sealedValidity, sealedData, nullCount);
    }
}