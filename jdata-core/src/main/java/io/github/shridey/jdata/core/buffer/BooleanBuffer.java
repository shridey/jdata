package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * A sealed, read-only Arrow column for Booleans.
 * Unlike integers which take 4 bytes each, these booleans are bit-packed
 * (8 true/false values fit inside a single byte) to save massive amounts of memory.
 */
public final class BooleanBuffer extends AbstractDataBuffer {

    private final MemorySegment dataBuffer;

    BooleanBuffer(String name, Arena arena, long length, long offset, MemorySegment validityBitmap, MemorySegment dataBuffer) {
        super(name, arena, length, offset, validityBitmap);
        this.dataBuffer = dataBuffer;
    }

    public static BooleanBuffer allocate(Arena arena, String name, long length) {
        if (length < 0) {
            throw new IllegalArgumentException("Buffer length cannot be negative: " + length);
        }
        long byteSize = (length + 7L) >>> 3;

        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, byteSize);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, byteSize);

        BooleanBuffer buffer = new BooleanBuffer(name, arena, length, 0, validityBitmap, dataBuffer);
        buffer.setAllValid();
        return buffer;
    }

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
        markValid(index);
    }

    public boolean get(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;

        long byteIndex = physicalIndex >>> 3;
        int bitIndex = (int) (physicalIndex & 7L);
        byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);

        return (b & (1 << bitIndex)) != 0;
    }

    public void copyBlock(BooleanBuffer source, long srcIndex, long destIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        Objects.checkFromIndexSize(srcIndex, length, source.length);
        Objects.checkFromIndexSize(destIndex, length, this.length);

        if (length == 0) return;

        long srcPhysical = srcIndex + source.offset;
        long destPhysical = destIndex + this.offset;

        // FAST PATH: Both slice offsets and length align on byte boundaries
        if ((srcPhysical & 7L) == 0 && (destPhysical & 7L) == 0 && (length & 7L) == 0) {
            long byteOffsetSrc = srcPhysical >>> 3;
            long byteOffsetDest = destPhysical >>> 3;
            long byteLength = length >>> 3;

            MemorySegment.copy(source.dataBuffer, byteOffsetSrc, this.dataBuffer, byteOffsetDest, byteLength);
            MemorySegment.copy(source.validityBitmap, byteOffsetSrc, this.validityBitmap, byteOffsetDest, byteLength);
            this.nullCount = -1;
            return;
        }

        // SLOW PATH: Bit-by-bit copy for unaligned bit windows
        for (long i = 0; i < length; i++) {
            long currentSrc = srcIndex + i;
            long currentDest = destIndex + i;

            // set() automatically calls markValid(currentDest)
            this.set(currentDest, source.get(currentSrc));
            if (source.isNull(currentSrc)) {
                this.setNull(currentDest);
            }
        }
    }

    public void copyFrom(boolean[] source) {
        Objects.requireNonNull(source, "Source array cannot be null");
        if (source.length != this.length) {
            throw new IllegalArgumentException("Array length must exactly match buffer length.");
        }

        // If sliced at a non-byte boundary, use bit-level writes to avoid clobbering neighboring bits
        if ((this.offset & 7L) != 0) {
            for (int i = 0; i < source.length; i++) {
                this.set(i, source[i]);
            }
            return;
        }

        // Fast path: Pack 8 booleans at a time into a single byte write
        long byteOffset = this.offset >>> 3;
        int fullBytes = source.length >>> 3;
        int idx = 0;

        for (int bIdx = 0; bIdx < fullBytes; bIdx++) {
            int packed = (source[idx] ? 1 : 0)
                    | (source[idx + 1] ? 2 : 0)
                    | (source[idx + 2] ? 4 : 0)
                    | (source[idx + 3] ? 8 : 0)
                    | (source[idx + 4] ? 16 : 0)
                    | (source[idx + 5] ? 32 : 0)
                    | (source[idx + 6] ? 64 : 0)
                    | (source[idx + 7] ? 128 : 0);
            dataBuffer.set(ValueLayout.JAVA_BYTE, byteOffset + bIdx, (byte) packed);
            idx += 8;
        }

        int remainder = source.length & 7;
        if (remainder > 0) {
            int packed = 0;
            for (int bit = 0; bit < remainder; bit++) {
                if (source[idx + bit]) {
                    packed |= (1 << bit);
                }
            }
            int mask = (1 << remainder) - 1;
            long tailByteIndex = byteOffset + fullBytes;
            byte existing = dataBuffer.get(ValueLayout.JAVA_BYTE, tailByteIndex);
            dataBuffer.set(ValueLayout.JAVA_BYTE, tailByteIndex, (byte) ((existing & ~mask) | packed));
        }

        setAllValid();
    }

    public boolean[] toArray() {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }
        int len = (int) this.length;
        boolean[] target = new boolean[len];
        for (int i = 0; i < len; i++) {
            target[i] = this.get(i);
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

    @Override public DataType type() { return DataType.BOOLEAN; }
    @Override public int bufferCount() { return 2; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0);
            case 1 -> dataBuffer;
            default -> throw new IndexOutOfBoundsException("BooleanBuffer has exactly 2 buffers.");
        };
    }
}