package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * A sealed, read-only Arrow column for 32-bit floating-point numbers (floats).
 * Utilizes hardware-aligned contiguous memory to enable SIMD vectorization.
 */
public final class Float32Buffer extends AbstractDataBuffer {

    private final MemorySegment dataBuffer;

    Float32Buffer(String name, Arena arena, long length, long offset,
                  MemorySegment validityBitmap, MemorySegment dataBuffer) {
        super(name, arena, length, offset, validityBitmap);
        this.dataBuffer = dataBuffer;
    }

    /**
     * Allocates a fixed-size buffer instantly.
     * Used when the exact number of rows is known prior to allocation.
     */
    public static Float32Buffer allocate(Arena arena, String name, long length) {
        if (length < 0) {
            throw new IllegalArgumentException("Buffer length cannot be negative: " + length);
        }
        long bitmapBytes = (length + 7L) >>> 3;
        long dataBytes = Math.multiplyExact(length, ValueLayout.JAVA_FLOAT.byteSize());

        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);

        Float32Buffer buffer = new Float32Buffer(name, arena, length, 0, validityBitmap, dataBuffer);
        buffer.setAllValid();

        return buffer;
    }

    public void set(long index, float value) {
        checkBounds(index);

        long physicalIndex = index + this.offset;
        long byteOffset = physicalIndex * ValueLayout.JAVA_FLOAT.byteSize();
        dataBuffer.set(ValueLayout.JAVA_FLOAT, byteOffset, value);

        markValid(index);
    }

    public float get(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;
        long byteOffset = physicalIndex * ValueLayout.JAVA_FLOAT.byteSize();
        return dataBuffer.get(ValueLayout.JAVA_FLOAT, byteOffset);
    }

    public void copyBlock(Float32Buffer source, long srcIndex, long destIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        Objects.checkFromIndexSize(srcIndex, length, source.length);
        Objects.checkFromIndexSize(destIndex, length, this.length);

        if (length == 0) return;

        long byteSize = ValueLayout.JAVA_FLOAT.byteSize();
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
     * Ingestion: Dumps a Java float[] array into this buffer instantly.
     */
    public void copyFrom(float[] source) {
        Objects.requireNonNull(source, "Source array cannot be null");
        if (source.length != this.length) {
            throw new IllegalArgumentException("Array length must exactly match buffer length.");
        }

        long byteSize = ValueLayout.JAVA_FLOAT.byteSize();
        MemorySegment heapSegment = MemorySegment.ofArray(source);
        MemorySegment.copy(heapSegment, 0, this.dataBuffer, this.offset * byteSize, this.length * byteSize);
        setAllValid();
    }

    /**
     * Exfiltration: Dumps this off-heap buffer back into a standard Java float[] array.
     */
    public float[] toArray() {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }

        float[] target = new float[(int) this.length];
        long byteSize = ValueLayout.JAVA_FLOAT.byteSize();
        MemorySegment heapSegment = MemorySegment.ofArray(target);
        MemorySegment.copy(this.dataBuffer, this.offset * byteSize, heapSegment, 0, this.length * byteSize);

        return target;
    }

    @Override
    public Float32BufferBuilder toBuilder(Arena arena) {
        Float32BufferBuilder builder = new Float32BufferBuilder(arena, this.name, this.length);
        return builder.appendBlock(this, 0, this.length);
    }

    @Override
    public Float32Buffer slice(long sliceOffset, long sliceLength) {
        Objects.checkFromIndexSize(sliceOffset, sliceLength, this.length);
        long absoluteOffset = this.offset + sliceOffset;

        return new Float32Buffer(this.name, this.arena, sliceLength, absoluteOffset,
                this.validityBitmap, this.dataBuffer);
    }

    @Override public DataType type() { return DataType.FLOAT32; }
    @Override public int bufferCount() { return 2; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0);
            case 1 -> dataBuffer;
            default -> throw new IndexOutOfBoundsException("Float32Buffer has exactly 2 buffers.");
        };
    }
}