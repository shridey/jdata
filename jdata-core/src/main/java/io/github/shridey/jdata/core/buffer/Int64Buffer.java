package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * A contiguous off-heap Apache Arrow {@link DataType#INT64} column storing 64-bit signed integers.
 */
public final class Int64Buffer extends AbstractDataBuffer {

    private final MemorySegment dataBuffer;

    Int64Buffer(String name, Arena arena, long length, long offset, MemorySegment validityBitmap, MemorySegment dataBuffer) {
        super(name, arena, length, offset, validityBitmap);
        this.dataBuffer = dataBuffer;
    }

    /**
     * Allocates a new 64-byte-aligned and 64-byte tail-padded {@link Int64Buffer} initialized to {@code 0L} and valid.
     *
     * @param arena  the arena governing the off-heap allocation
     * @param name   the column name
     * @param length the number of elements to allocate
     * @return a newly allocated {@link Int64Buffer}
     */
    public static Int64Buffer allocate(Arena arena, String name, long length) {
        Objects.requireNonNull(arena, "Arena cannot be null");
        Objects.requireNonNull(name, "Column name cannot be null");
        if (length < 0) {
            throw new IllegalArgumentException("Buffer length cannot be negative: " + length);
        }

        long bitmapBytes = (length + 7L) >>> 3;
        long dataBytes = Math.multiplyExact(length, ValueLayout.JAVA_LONG.byteSize());

        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);

        Int64Buffer buffer = new Int64Buffer(name, arena, length, 0, validityBitmap, dataBuffer);
        buffer.setAllValid();
        return buffer;
    }

    /**
     * Zero-copy wraps pre-allocated or memory-mapped validity and 64-bit integer data segments.
     *
     * @param name           the column name
     * @param arena          the governing arena
     * @param length         the logical element count
     * @param offset         the starting physical element offset
     * @param validityBitmap the validity bitmap segment (or {@link MemorySegment#NULL})
     * @param dataBuffer     the 64-bit integer values segment
     * @return a wrapped {@link Int64Buffer}
     */
    public static Int64Buffer wrap(String name, Arena arena, long length, long offset,
                                   MemorySegment validityBitmap, MemorySegment dataBuffer) {
        long totalElements = Math.addExact(length, offset);
        long minDataBytes = Math.multiplyExact(totalElements, ValueLayout.JAVA_LONG.byteSize());
        ArrowMemoryAllocator.validateWrapBounds(name, arena, length, offset, validityBitmap, dataBuffer, minDataBytes);
        return new Int64Buffer(name, arena, length, offset, validityBitmap, dataBuffer);
    }

    /**
     * Reads the {@code long} value at the specified logical index.
     *
     * @param index the zero-based logical index
     * @return the {@code long} value at {@code index}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public long get(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;
        return dataBuffer.get(ValueLayout.JAVA_LONG, physicalIndex * ValueLayout.JAVA_LONG.byteSize());
    }

    /**
     * Writes a {@code long} value at the specified logical index and marks the slot valid.
     *
     * @param index the zero-based logical index
     * @param value the {@code long} value to write
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public void set(long index, long value) {
        checkBounds(index);
        long physicalIndex = index + this.offset;
        dataBuffer.set(ValueLayout.JAVA_LONG, physicalIndex * ValueLayout.JAVA_LONG.byteSize(), value);
        setValid(index);
    }

    /**
     * Bulk-copies a Java {@code long[]} into off-heap memory starting at logical index {@code 0}.
     *
     * @param source the source array to copy from
     * @throws IndexOutOfBoundsException if {@code source.length > length()}
     */
    public void copyFrom(long[] source) {
        Objects.requireNonNull(source, "Source array cannot be null");
        if (source.length > this.length) {
            throw new IndexOutOfBoundsException("Source array is larger than the buffer capacity.");
        }

        long destByteOffset = this.offset * ValueLayout.JAVA_LONG.byteSize();
        long copyBytes = (long) source.length * ValueLayout.JAVA_LONG.byteSize();

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
     * @param source    the source {@link Int64Buffer}
     * @param srcIndex  the starting logical index in {@code source}
     * @param destIndex the starting logical index in this buffer
     * @param length    the number of elements to copy
     */
    public void copyBlock(Int64Buffer source, long srcIndex, long destIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        Objects.checkFromIndexSize(srcIndex, length, source.length);
        Objects.checkFromIndexSize(destIndex, length, this.length);
        if (length == 0) return;

        long byteSize = ValueLayout.JAVA_LONG.byteSize();
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
     * Materializes the elements of this buffer into a newly allocated Java {@code long[]}.
     *
     * @return a Java {@code long[]} containing this buffer's values
     */
    public long[] toArray() {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }

        int len = (int) this.length;
        long[] target = new long[len];

        long srcByteOffset = this.offset * ValueLayout.JAVA_LONG.byteSize();
        long copyBytes = this.length * ValueLayout.JAVA_LONG.byteSize();

        MemorySegment heapSegment = MemorySegment.ofArray(target);
        MemorySegment.copy(this.dataBuffer, srcByteOffset, heapSegment, 0, copyBytes);

        return target;
    }

    @Override
    public Int64BufferBuilder toBuilder(Arena arena) {
        Int64BufferBuilder builder = new Int64BufferBuilder(arena, this.name, this.length);
        return builder.appendBlock(this, 0, this.length);
    }

    @Override
    public Int64Buffer slice(long sliceOffset, long sliceLength) {
        Objects.checkFromIndexSize(sliceOffset, sliceLength, this.length);
        long absoluteOffset = this.offset + sliceOffset;
        return new Int64Buffer(this.name, this.arena, sliceLength, absoluteOffset, this.validityBitmap, this.dataBuffer);
    }

    @Override
    public Int64Buffer withName(String newName) {
        Objects.requireNonNull(newName, "newName cannot be null");
        return new Int64Buffer(newName, this.arena, this.length, this.offset, this.validityBitmap, this.dataBuffer);
    }

    @Override
    public Int64Buffer take(Int32Buffer indices, Arena targetArena) {
        Objects.requireNonNull(indices, "indices cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        long outLen = indices.length();
        Int64Buffer out = Int64Buffer.allocate(targetArena, this.name, outLen);
        if (outLen == 0) return out;

        MemorySegment outData = out.dataBuffer;
        MemorySegment idxData = indices.buffer(1);
        long idxOffset = indices.offset();
        long intBytes = ValueLayout.JAVA_INT.byteSize();
        long longBytes = ValueLayout.JAVA_LONG.byteSize();

        if (this.nullCount() == 0 && indices.nullCount() == 0) {
            for (long i = 0; i < outLen; i++) {
                int rowIdx = idxData.get(ValueLayout.JAVA_INT, (idxOffset + i) * intBytes);
                checkBounds(rowIdx);
                long val = this.dataBuffer.get(ValueLayout.JAVA_LONG, (this.offset + rowIdx) * longBytes);
                outData.set(ValueLayout.JAVA_LONG, i * longBytes, val);
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
                        outData.set(ValueLayout.JAVA_LONG, i * longBytes, this.get(rowIdx));
                    }
                }
            }
        }
        return out;
    }

    @Override public DataType type() { return DataType.INT64; }
    @Override public int bufferCount() { return 2; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0);
            case 1 -> dataBuffer;
            default -> throw new IndexOutOfBoundsException("Int64Buffer only has 2 buffers.");
        };
    }
}