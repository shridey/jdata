package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * Dynamically builds an off-heap bit-packed {@link BooleanBuffer}.
 */
public final class BooleanBufferBuilder extends AbstractBufferBuilder {

    private MemorySegment dataBuffer;

    /**
     * Creates a new {@link BooleanBufferBuilder} with the specified initial row capacity.
     *
     * @param arena                  the memory arena governing allocations
     * @param name                   the column name
     * @param initialLogicalCapacity the initial logical row capacity
     */
    public BooleanBufferBuilder(Arena arena, String name, long initialLogicalCapacity) {
        super(arena, name, initialLogicalCapacity);

        long dataBytes = (logicalCapacity + 7L) >>> 3;
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        long newDataBytes = (newLogicalCapacity + 7L) >>> 3;
        if (newDataBytes > dataBuffer.byteSize()) {
            long currentBytes = (logicalLength + 7L) >>> 3;
            this.dataBuffer = ArrowMemoryAllocator.reallocateAligned(arena, dataBuffer, currentBytes, newDataBytes);
        }
    }

    /**
     * Appends a single non-null boolean value.
     *
     * @param value the boolean value to append
     * @return this builder
     */
    public BooleanBufferBuilder append(boolean value) {
        ensureLogicalCapacity(1);

        long byteIndex = logicalLength >>> 3;
        int bitIndex = (int) (logicalLength & 7L);
        byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);

        if (value) {
            b = (byte) (b | (1 << bitIndex));
        } else {
            b = (byte) (b & ~(1 << bitIndex));
        }

        dataBuffer.set(ValueLayout.JAVA_BYTE, byteIndex, b);
        logicalLength++;
        return this;
    }

    /**
     * Bulk-appends an array of boolean values using 8-bit byte packing.
     *
     * @param values the array of boolean values to append
     * @return this builder
     */
    public BooleanBufferBuilder appendAll(boolean[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (values.length == 0) return this;

        ensureLogicalCapacity(values.length);

        int idx = 0;
        int len = values.length;

        while (idx < len && (logicalLength & 7L) != 0) {
            writeBitAtCurrentLength(values[idx++]);
            logicalLength++;
        }

        long byteCursor = logicalLength >>> 3;
        int fullBytes = (len - idx) >>> 3;
        for (int bIdx = 0; bIdx < fullBytes; bIdx++) {
            int packed = (values[idx] ? 1 : 0)
                    | (values[idx + 1] ? 2 : 0)
                    | (values[idx + 2] ? 4 : 0)
                    | (values[idx + 3] ? 8 : 0)
                    | (values[idx + 4] ? 16 : 0)
                    | (values[idx + 5] ? 32 : 0)
                    | (values[idx + 6] ? 64 : 0)
                    | (values[idx + 7] ? 128 : 0);
            dataBuffer.set(ValueLayout.JAVA_BYTE, byteCursor + bIdx, (byte) packed);
            idx += 8;
        }
        logicalLength += (long) fullBytes << 3;

        while (idx < len) {
            writeBitAtCurrentLength(values[idx++]);
            logicalLength++;
        }

        return this;
    }

    private void writeBitAtCurrentLength(boolean value) {
        long byteIndex = logicalLength >>> 3;
        int bitIndex = (int) (logicalLength & 7L);
        byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);
        if (value) {
            b = (byte) (b | (1 << bitIndex));
        } else {
            b = (byte) (b & ~(1 << bitIndex));
        }
        dataBuffer.set(ValueLayout.JAVA_BYTE, byteIndex, b);
    }

    /**
     * Appends a single null entry.
     *
     * @return this builder
     */
    public BooleanBufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull();
        logicalLength++;
        return this;
    }

    /**
     * Appends a range of elements from an existing {@link BooleanBuffer}.
     *
     * @param source   the source buffer
     * @param srcIndex the starting logical index in {@code source}
     * @param length   the number of elements to append
     * @return this builder
     */
    public BooleanBufferBuilder appendBlock(BooleanBuffer source, long srcIndex, long length) {
        return appendImportedBlock(source, srcIndex, length);
    }

    /**
     * Appends a range of elements from any {@link DataBuffer} of type {@link DataType#BOOLEAN}.
     *
     * @param source   the source buffer
     * @param srcIndex the starting logical index in {@code source}
     * @param length   the number of elements to append
     * @return this builder
     */
    public BooleanBufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        if (source.type() != DataType.BOOLEAN) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into a Boolean builder.");
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

        MemorySegment foreignDataBuffer = source.buffer(1);
        long srcPhysicalStart = srcIndex + source.offset();
        boolean hasNulls = source.nullCount() > 0;

        if (!hasNulls && (srcPhysicalStart & 7L) == 0 && (logicalLength & 7L) == 0) {
            long fullBytes = length >>> 3;
            if (fullBytes > 0) {
                MemorySegment.copy(foreignDataBuffer, srcPhysicalStart >>> 3, dataBuffer, logicalLength >>> 3, fullBytes);
            }
            long copiedBits = fullBytes << 3;
            logicalLength += copiedBits;
            for (long i = copiedBits; i < length; i++) {
                long physicalIdx = srcPhysicalStart + i;
                byte b = foreignDataBuffer.get(ValueLayout.JAVA_BYTE, physicalIdx >>> 3);
                writeBitAtCurrentLength(((b >>> (int) (physicalIdx & 7L)) & 1) != 0);
                logicalLength++;
            }
            return this;
        }

        for (long i = 0; i < length; i++) {
            long currentLogicalIdx = srcIndex + i;
            if (hasNulls && source.isNull(currentLogicalIdx)) {
                markCurrentNull();
            } else {
                long physicalIdx = srcPhysicalStart + i;
                byte b = foreignDataBuffer.get(ValueLayout.JAVA_BYTE, physicalIdx >>> 3);
                writeBitAtCurrentLength(((b >>> (int) (physicalIdx & 7L)) & 1) != 0);
            }
            logicalLength++;
        }
        return this;
    }

    @Override
    public BooleanBuffer build() {
        long paddedBytes = ArrowMemoryAllocator.padTo64((logicalLength + 7L) >>> 3);

        MemorySegment sealedValidity = validityBitmap.asSlice(0, paddedBytes);
        MemorySegment sealedData = dataBuffer.asSlice(0, paddedBytes);

        return new BooleanBuffer(name, arena, logicalLength, 0, sealedValidity, sealedData);
    }
}