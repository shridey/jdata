package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * A contiguous off-heap Apache Arrow {@link DataType#BOOLEAN} column backed by LSB-0 bit-packed
 * validity and data bitmaps (8 boolean values per byte).
 */
public final class BooleanBuffer extends AbstractDataBuffer {

    private final MemorySegment dataBuffer;

    BooleanBuffer(String name, Arena arena, long length, long offset, MemorySegment validityBitmap, MemorySegment dataBuffer) {
        this(name, arena, length, offset, validityBitmap, dataBuffer, -1L);
    }

    BooleanBuffer(String name, Arena arena, long length, long offset,
                  MemorySegment validityBitmap, MemorySegment dataBuffer, long knownNullCount) {
        super(name, arena, length, offset, validityBitmap, knownNullCount);
        this.dataBuffer = dataBuffer;
    }

    /**
     * Allocates a new 64-byte-aligned and 64-byte tail-padded {@link BooleanBuffer} initialized to {@code false} and valid.
     *
     * @param arena  the arena governing the off-heap allocation
     * @param name   the column name
     * @param length the number of boolean elements to allocate
     * @return a newly allocated {@link BooleanBuffer}
     */
    public static BooleanBuffer allocate(Arena arena, String name, long length) {
        Objects.requireNonNull(arena, "Arena cannot be null");
        Objects.requireNonNull(name, "Column name cannot be null");
        if (length < 0) {
            throw new IllegalArgumentException("Buffer length cannot be negative: " + length);
        }

        long requiredBytes = (length + 7L) >>> 3;
        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, requiredBytes);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, requiredBytes);

        BooleanBuffer buffer = new BooleanBuffer(name, arena, length, 0, validityBitmap, dataBuffer);
        buffer.setAllValid();
        return buffer;
    }

    /**
     * Zero-copy wraps pre-allocated or memory-mapped validity and bit-packed data segments.
     *
     * @param name           the column name
     * @param arena          the governing arena
     * @param length         the logical element count
     * @param offset         the starting physical bit offset
     * @param validityBitmap the validity bitmap segment (or {@link MemorySegment#NULL})
     * @param dataBuffer     the bit-packed boolean data segment
     * @return a wrapped {@link BooleanBuffer}
     */
    public static BooleanBuffer wrap(String name, Arena arena, long length, long offset,
                                     MemorySegment validityBitmap, MemorySegment dataBuffer) {
        long totalElements = Math.addExact(length, offset);
        long minDataBytes = (totalElements + 7L) >>> 3;
        ArrowMemoryAllocator.validateWrapBounds(name, arena, length, offset, validityBitmap, dataBuffer, minDataBytes);
        return new BooleanBuffer(name, arena, length, offset, validityBitmap, dataBuffer);
    }

    /**
     * Reads the boolean value at the specified logical index.
     *
     * @param index the zero-based logical index
     * @return the boolean value at {@code index}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public boolean get(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;
        long byteIndex = physicalIndex >>> 3;
        int bitIndex = (int) (physicalIndex & 7L);

        byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);
        return ((b >>> bitIndex) & 1) != 0;
    }

    /**
     * Writes a boolean value at the specified logical index and marks the slot valid.
     *
     * @param index the zero-based logical index
     * @param value the boolean value to write
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public void set(long index, boolean value) {
        checkBounds(index);
        long physicalIndex = index + this.offset;
        long byteIndex = physicalIndex >>> 3;
        int bitIndex = (int) (physicalIndex & 7L);

        byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);
        if (value) {
            b = (byte) (b | (1 << bitIndex));
        } else {
            b = (byte) (b & ~(1 << bitIndex));
        }
        dataBuffer.set(ValueLayout.JAVA_BYTE, byteIndex, b);
        setValid(index);
    }

    /**
     * Bulk-packs a Java {@code boolean[]} into this buffer starting at logical index {@code 0}.
     *
     * @param source the source array to copy from
     * @throws IndexOutOfBoundsException if {@code source.length > length()}
     */
    public void copyFrom(boolean[] source) {
        Objects.requireNonNull(source, "Source array cannot be null");
        if (source.length > this.length) {
            throw new IndexOutOfBoundsException("Source array is larger than the buffer capacity.");
        }

        int len = source.length;
        if ((this.offset & 7L) == 0) {
            long byteOffset = this.offset >>> 3;
            int fullBytes = len >>> 3;
            for (int bIdx = 0; bIdx < fullBytes; bIdx++) {
                int base = bIdx << 3;
                int packed = (source[base] ? 1 : 0)
                        | (source[base + 1] ? 2 : 0)
                        | (source[base + 2] ? 4 : 0)
                        | (source[base + 3] ? 8 : 0)
                        | (source[base + 4] ? 16 : 0)
                        | (source[base + 5] ? 32 : 0)
                        | (source[base + 6] ? 64 : 0)
                        | (source[base + 7] ? 128 : 0);
                dataBuffer.set(ValueLayout.JAVA_BYTE, byteOffset + bIdx, (byte) packed);
            }

            int tailStart = fullBytes << 3;
            if (tailStart < len) {
                long tailByteIndex = byteOffset + fullBytes;
                int tailBits = len - tailStart;
                int tailMask = (1 << tailBits) - 1;
                int tailPacked = 0;
                for (int b = 0; b < tailBits; b++) {
                    if (source[tailStart + b]) {
                        tailPacked |= (1 << b);
                    }
                }
                byte existing = dataBuffer.get(ValueLayout.JAVA_BYTE, tailByteIndex);
                byte merged = (byte) ((existing & ~tailMask) | tailPacked);
                dataBuffer.set(ValueLayout.JAVA_BYTE, tailByteIndex, merged);
            }

            if (len == this.length) {
                setAllValid();
            } else {
                for (int i = 0; i < len; i++) {
                    setValid(i);
                }
            }
        } else {
            for (int i = 0; i < len; i++) {
                set(i, source[i]);
            }
        }
    }

    /**
     * Copies a range of elements and their validity bits from {@code source} into this buffer.
     *
     * @param source    the source {@link BooleanBuffer}
     * @param srcIndex  the starting logical index in {@code source}
     * @param destIndex the starting logical index in this buffer
     * @param length    the number of elements to copy
     */
    public void copyBlock(BooleanBuffer source, long srcIndex, long destIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        Objects.checkFromIndexSize(srcIndex, length, source.length);
        Objects.checkFromIndexSize(destIndex, length, this.length);
        if (length == 0) return;

        long actualSrcIndex = srcIndex + source.offset;
        long actualDestIndex = destIndex + this.offset;

        if ((actualSrcIndex & 7L) == 0 && (actualDestIndex & 7L) == 0 && (length & 7L) == 0) {
            long byteLength = length >>> 3;
            long srcByteOffset = actualSrcIndex >>> 3;
            long destByteOffset = actualDestIndex >>> 3;

            MemorySegment.copy(source.dataBuffer, srcByteOffset, this.dataBuffer, destByteOffset, byteLength);
            if (!this.lacksValidityBitmap()) {
                if (source.lacksValidityBitmap()) {
                    this.validityBitmap.asSlice(destByteOffset, byteLength).fill((byte) 0xFF);
                } else {
                    MemorySegment.copy(source.validityBitmap, srcByteOffset, this.validityBitmap, destByteOffset, byteLength);
                }
            }
            this.nullCount = -1;
        } else {
            boolean srcHasNulls = source.nullCount() > 0;
            for (long i = 0; i < length; i++) {
                if (srcHasNulls && source.isNull(srcIndex + i)) {
                    this.setNull(destIndex + i);
                } else {
                    this.set(destIndex + i, source.get(srcIndex + i));
                }
            }
        }
    }

    /**
     * Materializes the boolean values of this buffer into a newly allocated Java {@code boolean[]}.
     *
     * @return a Java {@code boolean[]} containing this buffer's values
     */
    public boolean[] toArray() {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }

        int len = (int) this.length;
        boolean[] target = new boolean[len];

        for (int i = 0; i < len; i++) {
            target[i] = get(i);
        }
        return target;
    }

    @Override
    public BooleanBufferBuilder toBuilder(Arena arena) {
        BooleanBufferBuilder builder = new BooleanBufferBuilder(arena, this.name, this.length);
        return builder.appendBlock(this, 0, this.length);
    }

    @Override
    public BooleanBuffer slice(long sliceOffset, long sliceLength) {
        Objects.checkFromIndexSize(sliceOffset, sliceLength, this.length);
        long absoluteOffset = this.offset + sliceOffset;
        return new BooleanBuffer(this.name, this.arena, sliceLength, absoluteOffset, this.validityBitmap, this.dataBuffer);
    }

    @Override
    public BooleanBuffer withName(String newName) {
        Objects.requireNonNull(newName, "newName cannot be null");
        return new BooleanBuffer(newName, this.arena, this.length, this.offset, this.validityBitmap, this.dataBuffer);
    }

    @Override
    public BooleanBuffer take(Int32Buffer indices, Arena targetArena) {
        Objects.requireNonNull(indices, "indices cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        long outLen = indices.length();
        BooleanBuffer out = BooleanBuffer.allocate(targetArena, this.name, outLen);
        boolean idxHasNulls = indices.nullCount() > 0;
        boolean srcHasNulls = this.nullCount() > 0;

        for (long i = 0; i < outLen; i++) {
            if (idxHasNulls && indices.isNull(i)) {
                out.setNull(i);
            } else {
                int rowIdx = indices.get(i);
                if (srcHasNulls && this.isNull(rowIdx)) {
                    out.setNull(i);
                } else {
                    out.set(i, this.get(rowIdx));
                }
            }
        }
        return out;
    }

    @Override public DataType type() { return DataType.BOOLEAN; }
    @Override public int bufferCount() { return 2; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0);
            case 1 -> dataBuffer;
            default -> throw new IndexOutOfBoundsException("BooleanBuffer only has 2 buffers (0 and 1).");
        };
    }
}