package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

public final class Int64BufferBuilder extends AbstractBufferBuilder {

    private MemorySegment dataBuffer;

    public Int64BufferBuilder(Arena arena, String name, long initialCapacity) {
        super(arena, name, initialCapacity);

        long dataBytes = Math.multiplyExact(logicalCapacity, ValueLayout.JAVA_LONG.byteSize());
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        long newDataBytes = Math.multiplyExact(newLogicalCapacity, ValueLayout.JAVA_LONG.byteSize());
        long currentBytes = logicalLength * ValueLayout.JAVA_LONG.byteSize();
        this.dataBuffer = ArrowMemoryAllocator.reallocateAligned(arena, dataBuffer, currentBytes, newDataBytes);
    }

    public Int64BufferBuilder append(long value) {
        ensureLogicalCapacity(1);

        long byteOffset = logicalLength * ValueLayout.JAVA_LONG.byteSize();
        dataBuffer.set(ValueLayout.JAVA_LONG, byteOffset, value);

        logicalLength++;
        return this;
    }

    public Int64BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull();
        logicalLength++;
        return this;
    }

    public Int64BufferBuilder appendAll(long[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (values.length == 0) return this;

        ensureLogicalCapacity(values.length);

        long startByteOffset = logicalLength * ValueLayout.JAVA_LONG.byteSize();
        long copyBytes = (long) values.length * ValueLayout.JAVA_LONG.byteSize();

        MemorySegment heapSegment = MemorySegment.ofArray(values);
        MemorySegment.copy(heapSegment, 0, dataBuffer, startByteOffset, copyBytes);

        logicalLength += values.length;
        return this;
    }

    public Int64BufferBuilder appendBlock(Int64Buffer source, long srcIndex, long length) {
        return appendImportedBlock(source, srcIndex, length);
    }

    public Int64BufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        if (source.type() != DataType.INT64) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into an Int64 builder.");
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

        long byteSize = ValueLayout.JAVA_LONG.byteSize();
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
    public Int64Buffer build() {
        long paddedValidityBytes = ArrowMemoryAllocator.padTo64((logicalLength + 7L) >>> 3);
        long paddedDataBytes = ArrowMemoryAllocator.padTo64(logicalLength * ValueLayout.JAVA_LONG.byteSize());

        MemorySegment sealedValidity = validityBitmap.asSlice(0, paddedValidityBytes);
        MemorySegment sealedData = dataBuffer.asSlice(0, paddedDataBytes);

        return new Int64Buffer(name, arena, logicalLength, 0, sealedValidity, sealedData);
    }
}