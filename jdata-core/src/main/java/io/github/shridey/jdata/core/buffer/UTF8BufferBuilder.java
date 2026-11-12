package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

public final class UTF8BufferBuilder extends AbstractBufferBuilder {

    private MemorySegment offsetsBuffer;
    private MemorySegment dataBuffer;

    private long dataCapacity;
    private int currentDataOffset = 0;

    public UTF8BufferBuilder(Arena arena, String name, long initialLogicalCapacity, long initialDataCapacity) {
        super(arena, name, initialLogicalCapacity);
        if (initialDataCapacity < 0 || initialDataCapacity > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid initialDataCapacity: " + initialDataCapacity);
        }

        this.dataCapacity = Math.max(initialDataCapacity, 64L);

        long offsetsBytes = Math.multiplyExact(logicalCapacity + 1L, ValueLayout.JAVA_INT.byteSize());
        this.offsetsBuffer = ArrowMemoryAllocator.allocateAligned(arena, offsetsBytes);

        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataCapacity);
        this.dataCapacity = this.dataBuffer.byteSize();
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        long newOffsetsBytes = Math.multiplyExact(newLogicalCapacity + 1L, ValueLayout.JAVA_INT.byteSize());
        long currentOffsetsBytes = (logicalLength + 1L) * ValueLayout.JAVA_INT.byteSize();
        this.offsetsBuffer = ArrowMemoryAllocator.reallocateAligned(
                arena, offsetsBuffer, currentOffsetsBytes, newOffsetsBytes
        );
    }

    private void ensureDataCapacity(long additionalBytes) {
        if (additionalBytes < 0) {
            throw new IllegalArgumentException("additionalBytes cannot be negative: " + additionalBytes);
        }
        long targetData = Math.addExact(currentDataOffset, additionalBytes);
        if (targetData > Integer.MAX_VALUE) {
            throw new IllegalStateException("UTF8 buffer cannot exceed Integer.MAX_VALUE bytes; use LARGE_UTF8.");
        }

        if (targetData > dataCapacity) {
            long doubled = Math.min(dataCapacity * 2L, Integer.MAX_VALUE);
            long newDataCapacity = Math.max(doubled, targetData);
            this.dataBuffer = ArrowMemoryAllocator.reallocateAligned(
                    arena, dataBuffer, currentDataOffset, newDataCapacity
            );
            this.dataCapacity = this.dataBuffer.byteSize();
        }
    }

    public UTF8BufferBuilder append(String value) {
        if (value == null) return appendNull();

        int encodedLength = UTF8Buffer.utf8ByteLength(value);
        ensureLogicalCapacity(1);
        ensureDataCapacity(encodedLength);

        currentDataOffset = UTF8Buffer.encodeUtf8(value, dataBuffer, currentDataOffset);
        logicalLength++;

        offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), currentDataOffset);
        return this;
    }

    public UTF8BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull();
        logicalLength++;

        offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), currentDataOffset);
        return this;
    }

    public UTF8BufferBuilder appendAll(String[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (values.length == 0) return this;

        long additionalBytes = UTF8Buffer.computeTotalUtf8Bytes(values);
        ensureLogicalCapacity(values.length);
        ensureDataCapacity(additionalBytes);

        for (String val : values) {
            if (val == null) {
                markCurrentNull();
            } else {
                currentDataOffset = UTF8Buffer.encodeUtf8(val, dataBuffer, currentDataOffset);
            }
            logicalLength++;
            offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), currentDataOffset);
        }
        return this;
    }

    public UTF8BufferBuilder appendBlock(UTF8Buffer source, long srcIndex, long length) {
        return appendImportedBlock(source, srcIndex, length);
    }

    public UTF8BufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        if (source.type() != DataType.UTF8) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into a UTF8 builder.");
        }
        Objects.checkFromIndexSize(srcIndex, length, source.length());
        if (length == 0) return this;

        long offsetByteSize = ValueLayout.JAVA_INT.byteSize();
        long srcPhysical = srcIndex + source.offset();
        MemorySegment srcOffsets = source.buffer(1);

        int srcStartByte = srcOffsets.get(ValueLayout.JAVA_INT, srcPhysical * offsetByteSize);
        int srcEndByte = srcOffsets.get(ValueLayout.JAVA_INT, (srcPhysical + length) * offsetByteSize);
        long totalBytesToCopy = (long) srcEndByte - srcStartByte;

        ensureLogicalCapacity(length);
        ensureDataCapacity(totalBytesToCopy);

        if (totalBytesToCopy > 0) {
            MemorySegment.copy(source.buffer(2), srcStartByte, dataBuffer, currentDataOffset, totalBytesToCopy);
        }

        int offsetDelta = currentDataOffset - srcStartByte;
        boolean hasNulls = source.nullCount() > 0;

        if (offsetDelta == 0 && !hasNulls) {
            MemorySegment.copy(
                    srcOffsets, (srcPhysical + 1L) * offsetByteSize,
                    offsetsBuffer, (logicalLength + 1L) * offsetByteSize,
                    length * offsetByteSize
            );
            logicalLength += length;
            currentDataOffset = srcEndByte;
            return this;
        }

        for (long i = 0; i < length; i++) {
            if (hasNulls && source.isNull(srcIndex + i)) {
                markCurrentNull();
            }

            int sEnd = srcOffsets.get(ValueLayout.JAVA_INT, (srcPhysical + i + 1L) * offsetByteSize);
            logicalLength++;
            offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * offsetByteSize, sEnd + offsetDelta);
        }
        currentDataOffset += (int) totalBytesToCopy;
        return this;
    }

    @Override
    public UTF8Buffer build() {
        long paddedValidityBytes = ArrowMemoryAllocator.padTo64((logicalLength + 7L) >>> 3);
        long paddedOffsetsBytes = ArrowMemoryAllocator.padTo64((logicalLength + 1L) * ValueLayout.JAVA_INT.byteSize());
        long paddedDataBytes = ArrowMemoryAllocator.padTo64(currentDataOffset == 0 ? 64L : currentDataOffset);

        MemorySegment sealedValidity = validityBitmap.asSlice(0, paddedValidityBytes);
        MemorySegment sealedOffsets = offsetsBuffer.asSlice(0, paddedOffsetsBytes);
        MemorySegment sealedData = dataBuffer.asSlice(0, paddedDataBytes);

        return new UTF8Buffer(name, arena, logicalLength, 0, sealedValidity, sealedOffsets, sealedData);
    }
}