package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * A sealed, read-only Arrow column for 64-bit floating-point numbers (doubles).
 * Utilizes hardware-aligned contiguous memory to enable SIMD vectorization.
 */
public final class Float64Buffer extends AbstractDataBuffer {

    private final MemorySegment dataBuffer;

    Float64Buffer(String name, Arena arena, long length, long offset,
                  MemorySegment validityBitmap, MemorySegment dataBuffer) {
        super(name, arena, length, offset, validityBitmap);
        this.dataBuffer = dataBuffer;
    }

    /**
     * Allocates a fixed-size buffer instantly.
     * Used when the exact number of rows is known prior to allocation.
     */
    public static Float64Buffer allocate(Arena arena, String name, long length) {
        if (length < 0) {
            throw new IllegalArgumentException("Buffer length cannot be negative: " + length);
        }
        long bitmapBytes = (length + 7L) >>> 3;
        long dataBytes = Math.multiplyExact(length, ValueLayout.JAVA_DOUBLE.byteSize());

        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);

        Float64Buffer buffer = new Float64Buffer(name, arena, length, 0, validityBitmap, dataBuffer);
        buffer.setAllValid();

        return buffer;
    }

    public void set(long index, double value) {
        checkBounds(index);

        long physicalIndex = index + this.offset;
        long byteOffset = physicalIndex * ValueLayout.JAVA_DOUBLE.byteSize();
        dataBuffer.set(ValueLayout.JAVA_DOUBLE, byteOffset, value);

        markValid(index);
    }

    public double get(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;
        long byteOffset = physicalIndex * ValueLayout.JAVA_DOUBLE.byteSize();
        return dataBuffer.get(ValueLayout.JAVA_DOUBLE, byteOffset);
    }

    public void copyBlock(Float64Buffer source, long srcIndex, long destIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        Objects.checkFromIndexSize(srcIndex, length, source.length);
        Objects.checkFromIndexSize(destIndex, length, this.length);

        if (length == 0) return;

        long byteSize = ValueLayout.JAVA_DOUBLE.byteSize();
        long srcPhysical = srcIndex + source.offset;
        long destPhysical = destIndex + this.offset;

        long srcByteOffset = srcPhysical * byteSize;
        long destByteOffset = destPhysical * byteSize;
        long copyBytes = length * byteSize;

        MemorySegment.copy(source.dataBuffer, srcByteOffset, this.dataBuffer, destByteOffset, copyBytes);

        if (source.nullCount() == 0 && destIndex == 0 && length == this.length) {
            setAllValid();
        } else {
            for (long i = 0; i < length; i++) {
                if (source.isNull(srcIndex + i)) {
                    this.setNull(destIndex + i);
                } else {
                    this.markValid(destIndex + i);
                }
            }
        }
    }

    /**
     * Ingestion: Dumps a Java double[] array into this buffer instantly.
     */
    public void copyFrom(double[] source) {
        Objects.requireNonNull(source, "Source array cannot be null");
        if (source.length != this.length) {
            throw new IllegalArgumentException("Array length must exactly match buffer length.");
        }

        long byteSize = ValueLayout.JAVA_DOUBLE.byteSize();
        MemorySegment heapSegment = MemorySegment.ofArray(source);
        MemorySegment.copy(heapSegment, 0, this.dataBuffer, this.offset * byteSize, this.length * byteSize);
        setAllValid();
    }

    /**
     * Exfiltration: Dumps this off-heap buffer back into a standard Java double[] array.
     */
    public double[] toArray() {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }

        double[] target = new double[(int) this.length];
        long byteSize = ValueLayout.JAVA_DOUBLE.byteSize();
        MemorySegment heapSegment = MemorySegment.ofArray(target);
        MemorySegment.copy(this.dataBuffer, this.offset * byteSize, heapSegment, 0, this.length * byteSize);

        return target;
    }

    @Override
    public Float64BufferBuilder toBuilder(Arena arena) {
        Float64BufferBuilder builder = new Float64BufferBuilder(arena, this.name, this.length);
        return builder.appendBlock(this, 0, this.length);
    }

    @Override
    public Float64Buffer slice(long sliceOffset, long sliceLength) {
        Objects.checkFromIndexSize(sliceOffset, sliceLength, this.length);
        long absoluteOffset = this.offset + sliceOffset;

        return new Float64Buffer(this.name, this.arena, sliceLength, absoluteOffset,
                this.validityBitmap, this.dataBuffer);
    }

    @Override public DataType type() { return DataType.FLOAT64; }
    @Override public int bufferCount() { return 2; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0);
            case 1 -> dataBuffer;
            default -> throw new IndexOutOfBoundsException("Float64Buffer has exactly 2 buffers.");
        };
    }
}