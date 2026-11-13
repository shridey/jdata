package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * A contiguous off-heap Apache Arrow {@link DataType#FLOAT32} column storing 32-bit IEEE 754 floats[cite: 5, 16].
 */
public final class Float32Buffer extends AbstractDataBuffer {

    private final MemorySegment dataBuffer;

    Float32Buffer(String name, Arena arena, long length, long offset, MemorySegment validityBitmap, MemorySegment dataBuffer) {
        super(name, arena, length, offset, validityBitmap);
        this.dataBuffer = dataBuffer;
    }

    /**
     * Allocates a new 64-byte-aligned and 64-byte tail-padded {@link Float32Buffer} initialized to {@code 0.0f} and valid.
     *
     * @param arena  the arena governing the off-heap allocation
     * @param name   the column name
     * @param length the number of elements to allocate
     * @return a newly allocated {@link Float32Buffer}
     */
    public static Float32Buffer allocate(Arena arena, String name, long length) {
        Objects.requireNonNull(arena, "Arena cannot be null");
        Objects.requireNonNull(name, "Column name cannot be null");
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

    /**
     * Zero-copy wraps pre-allocated or memory-mapped validity and 32-bit float data segments.
     *
     * @param name           the column name
     * @param arena          the governing arena
     * @param length         the logical element count
     * @param offset         the starting physical element offset
     * @param validityBitmap the validity bitmap segment (or {@link MemorySegment#NULL})
     * @param dataBuffer     the 32-bit float values segment
     * @return a wrapped {@link Float32Buffer}
     */
    public static Float32Buffer wrap(String name, Arena arena, long length, long offset,
                                     MemorySegment validityBitmap, MemorySegment dataBuffer) {
        long totalElements = Math.addExact(length, offset);
        long minDataBytes = Math.multiplyExact(totalElements, ValueLayout.JAVA_FLOAT.byteSize());
        ArrowMemoryAllocator.validateWrapBounds(name, arena, length, offset, validityBitmap, dataBuffer, minDataBytes);
        return new Float32Buffer(name, arena, length, offset, validityBitmap, dataBuffer);
    }

    /**
     * Reads the {@code float} value at the specified logical index.
     *
     * @param index the zero-based logical index
     * @return the {@code float} value at {@code index}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public float get(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;
        return dataBuffer.get(ValueLayout.JAVA_FLOAT, physicalIndex * ValueLayout.JAVA_FLOAT.byteSize());
    }

    /**
     * Writes a {@code float} value at the specified logical index and marks the slot valid.
     *
     * @param index the zero-based logical index
     * @param value the {@code float} value to write
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public void set(long index, float value) {
        checkBounds(index);
        long physicalIndex = index + this.offset;
        dataBuffer.set(ValueLayout.JAVA_FLOAT, physicalIndex * ValueLayout.JAVA_FLOAT.byteSize(), value);
        setValid(index);
    }

    /**
     * Bulk-copies a Java {@code float[]} into off-heap memory starting at logical index {@code 0}.
     *
     * @param source the source array to copy from
     * @throws IndexOutOfBoundsException if {@code source.length > length()}
     */
    public void copyFrom(float[] source) {
        Objects.requireNonNull(source, "Source array cannot be null");
        if (source.length > this.length) {
            throw new IndexOutOfBoundsException("Source array is larger than the buffer capacity.");
        }

        long destByteOffset = this.offset * ValueLayout.JAVA_FLOAT.byteSize();
        long copyBytes = (long) source.length * ValueLayout.JAVA_FLOAT.byteSize();

        MemorySegment heapSegment = MemorySegment.ofArray(source);
        MemorySegment.copy(heapSegment, 0, this.dataBuffer, destByteOffset, copyBytes);

        if (source.length == this.length) {
            setAllValid();
        } else {
            for (int i = 0; i < source.length; i++) {
                setValid(i);
            }
        }
    }

    /**
     * Copies a contiguous block of elements and validity bits from {@code source} into this buffer.
     *
     * @param source    the source {@link Float32Buffer}
     * @param srcIndex  the starting logical index in {@code source}
     * @param destIndex the starting logical index in this buffer
     * @param length    the number of elements to copy
     */
    public void copyBlock(Float32Buffer source, long srcIndex, long destIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        Objects.checkFromIndexSize(srcIndex, length, source.length);
        Objects.checkFromIndexSize(destIndex, length, this.length);
        if (length == 0) return;

        long byteSize = ValueLayout.JAVA_FLOAT.byteSize();
        long srcByteOffset = (srcIndex + source.offset) * byteSize;
        long destByteOffset = (destIndex + this.offset) * byteSize;
        long copyBytes = length * byteSize;

        MemorySegment.copy(source.dataBuffer, srcByteOffset, this.dataBuffer, destByteOffset, copyBytes);

        if (source.nullCount() == 0) {
            if (destIndex == 0 && length == this.length) {
                this.setAllValid();
            } else {
                for (long i = 0; i < length; i++) {
                    this.setValid(destIndex + i);
                }
            }
        } else {
            for (long i = 0; i < length; i++) {
                if (source.isNull(srcIndex + i)) {
                    this.setNull(destIndex + i);
                } else {
                    this.setValid(destIndex + i);
                }
            }
        }
    }

    /**
     * Materializes the elements of this buffer into a newly allocated Java {@code float[]}.
     *
     * @return a Java {@code float[]} containing this buffer's values
     */
    public float[] toArray() {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }

        int len = (int) this.length;
        float[] target = new float[len];

        long srcByteOffset = this.offset * ValueLayout.JAVA_FLOAT.byteSize();
        long copyBytes = this.length * ValueLayout.JAVA_FLOAT.byteSize();

        MemorySegment heapSegment = MemorySegment.ofArray(target);
        MemorySegment.copy(this.dataBuffer, srcByteOffset, heapSegment, 0, copyBytes);

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
        return new Float32Buffer(this.name, this.arena, sliceLength, absoluteOffset, this.validityBitmap, this.dataBuffer);
    }

    @Override
    public Float32Buffer withName(String newName) {
        Objects.requireNonNull(newName, "newName cannot be null");
        return new Float32Buffer(newName, this.arena, this.length, this.offset, this.validityBitmap, this.dataBuffer);
    }

    @Override
    public Float32Buffer take(Int32Buffer indices, Arena targetArena) {
        Objects.requireNonNull(indices, "indices cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        long outLen = indices.length();
        Float32Buffer out = Float32Buffer.allocate(targetArena, this.name, outLen);
        if (outLen == 0) return out;

        MemorySegment outData = out.dataBuffer;
        MemorySegment idxData = indices.buffer(1);
        long idxOffset = indices.offset();
        long intBytes = ValueLayout.JAVA_INT.byteSize();
        long floatBytes = ValueLayout.JAVA_FLOAT.byteSize();

        if (this.nullCount() == 0 && indices.nullCount() == 0) {
            for (long i = 0; i < outLen; i++) {
                int rowIdx = idxData.get(ValueLayout.JAVA_INT, (idxOffset + i) * intBytes);
                checkBounds(rowIdx);
                float val = this.dataBuffer.get(ValueLayout.JAVA_FLOAT, (this.offset + rowIdx) * floatBytes);
                outData.set(ValueLayout.JAVA_FLOAT, i * floatBytes, val);
            }
        } else {
            boolean idxHasNulls = indices.nullCount() > 0;
            boolean srcHasNulls = this.nullCount() > 0;
            for (long i = 0; i < outLen; i++) {
                if (idxHasNulls && indices.isNull(i)) {
                    out.setNull(i);
                } else {
                    int rowIdx = idxData.get(ValueLayout.JAVA_INT, (idxOffset + i) * intBytes);
                    if (srcHasNulls && this.isNull(rowIdx)) {
                        out.setNull(i);
                    } else {
                        outData.set(ValueLayout.JAVA_FLOAT, i * floatBytes, this.get(rowIdx));
                    }
                }
            }
        }
        return out;
    }

    @Override public DataType type() { return DataType.FLOAT32; }
    @Override public int bufferCount() { return 2; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0);
            case 1 -> dataBuffer;
            default -> throw new IndexOutOfBoundsException("Float32Buffer only has 2 buffers.");
        };
    }
}