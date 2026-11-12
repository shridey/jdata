package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

public final class Float32BufferBuilder extends AbstractBufferBuilder {

    private MemorySegment dataBuffer;

    public Float32BufferBuilder(Arena arena, String name, long initialCapacity) {
        super(arena, name, initialCapacity);

        long dataBytes = Math.multiplyExact(logicalCapacity, ValueLayout.JAVA_FLOAT.byteSize());
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        long newDataBytes = Math.multiplyExact(newLogicalCapacity, ValueLayout.JAVA_FLOAT.byteSize());
        long currentBytes = logicalLength * ValueLayout.JAVA_FLOAT.byteSize();
        this.dataBuffer = ArrowMemoryAllocator.reallocateAligned(arena, dataBuffer, currentBytes, newDataBytes);
    }

    public Float32BufferBuilder append(float value) {
        ensureLogicalCapacity(1);

        long byteOffset = logicalLength * ValueLayout.JAVA_FLOAT.byteSize();
        dataBuffer.set(ValueLayout.JAVA_FLOAT, byteOffset, value);

        logicalLength++;
        return this;
    }

    public Float32BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull();
        logicalLength++;
        return this;
    }

    public Float32BufferBuilder appendAll(float[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (values.length == 0) return this;

        ensureLogicalCapacity(values.length);

        long startByteOffset = logicalLength * ValueLayout.JAVA_FLOAT.byteSize();
        long copyBytes = (long) values.length * ValueLayout.JAVA_FLOAT.byteSize();

        MemorySegment heapSegment = MemorySegment.ofArray(values);
        MemorySegment.copy(heapSegment, 0, dataBuffer, startByteOffset, copyBytes);

        logicalLength += values.length;
        return this;
    }

    public Float32BufferBuilder appendBlock(Float32Buffer source, long srcIndex, long length) {
        return appendImportedBlock(source, srcIndex, length);
    }

    public Float32BufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        if (source.type() != DataType.FLOAT32) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into a Float32 builder.");
        }
        Objects.checkFromIndexSize(srcIndex, length, source.length());
        if (length == 0) return this;

        ensureLogicalCapacity(length);

        long byteSize = ValueLayout.JAVA_FLOAT.byteSize();
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
    public Float32Buffer build() {
        long paddedValidityBytes = ArrowMemoryAllocator.padTo64((logicalLength + 7L) >>> 3);
        long paddedDataBytes = ArrowMemoryAllocator.padTo64(logicalLength * ValueLayout.JAVA_FLOAT.byteSize());

        MemorySegment sealedValidity = validityBitmap.asSlice(0, paddedValidityBytes);
        MemorySegment sealedData = dataBuffer.asSlice(0, paddedDataBytes);

        return new Float32Buffer(name, arena, logicalLength, 0, sealedValidity, sealedData);
    }
}