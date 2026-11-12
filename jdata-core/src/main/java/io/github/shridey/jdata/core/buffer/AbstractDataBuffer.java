package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.ffi.ImportedDataBuffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * The foundational skeletal implementation for all contiguous physical DataBuffers.
 * Manages common metadata, bounds checking, and 64-bit word-accelerated validity bitmap operations.
 */
public abstract sealed class AbstractDataBuffer implements DataBuffer permits
        BooleanBuffer, Int32Buffer, Int64Buffer, Float32Buffer, Float64Buffer,
        UTF8Buffer, DictionaryBuffer, ImportedDataBuffer {

    protected final String name;
    protected final Arena arena;
    protected final long length;
    protected final long offset;
    protected final MemorySegment validityBitmap;
    protected long nullCount = -1;

    protected AbstractDataBuffer(String name, Arena arena, long length, long offset, MemorySegment validityBitmap) {
        this.name = Objects.requireNonNull(name, "Column name cannot be null");
        this.arena = Objects.requireNonNull(arena, "Arena cannot be null");
        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + length);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        this.length = length;
        this.offset = offset;
        this.validityBitmap = Objects.requireNonNull(validityBitmap, "validityBitmap cannot be null");
        if (lacksValidityBitmap()) {
            this.nullCount = 0L;
        }
    }

    protected final boolean lacksValidityBitmap() {
        return validityBitmap.equals(MemorySegment.NULL) || validityBitmap.byteSize() == 0;
    }

    protected final void checkBounds(long index) {
        Objects.checkIndex(index, this.length);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public Arena arena() {
        return arena;
    }

    @Override
    public boolean isNull(long index) {
        checkBounds(index);
        if (this.nullCount == 0 || lacksValidityBitmap()) {
            return false;
        }

        long physicalIndex = index + this.offset;
        long byteIndex = physicalIndex >>> 3;
        int bitIndex = (int) (physicalIndex & 7L);

        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);
        return ((b >>> bitIndex) & 1) == 0;
    }

    @Override
    public void setNull(long index) {
        checkBounds(index);
        if (lacksValidityBitmap()) {
            throw new UnsupportedOperationException(
                    "Cannot mark null on a buffer imported without a validity bitmap."
            );
        }

        long physicalIndex = index + this.offset;
        long byteIndex = physicalIndex >>> 3;
        int bitIndex = (int) (physicalIndex & 7L);
        int mask = 1 << bitIndex;

        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);
        if ((b & mask) != 0) {
            validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (b & ~mask));
            if (this.nullCount >= 0) {
                this.nullCount++;
            }
        }
    }

    /**
     * Marks the logical index as valid (non-null) in O(1) time.
     * Skips the off-heap memory write if the bit is already set to 1.
     */
    public void setValid(long index) {
        checkBounds(index);
        if (lacksValidityBitmap()) {
            return;
        }

        long physicalIndex = index + this.offset;
        long byteIndex = physicalIndex >>> 3;
        int bitIndex = (int) (physicalIndex & 7L);
        int mask = 1 << bitIndex;

        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);
        if ((b & mask) == 0) {
            validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (b | mask));
            if (this.nullCount > 0) {
                this.nullCount--;
            }
        }
    }

    @Override
    public void setAllValid() {
        if (length == 0 || lacksValidityBitmap()) {
            this.nullCount = 0;
            return;
        }

        long startBit = this.offset;
        long endBit = this.offset + this.length;
        long startByte = startBit >>> 3;
        long endByte = endBit >>> 3;
        int startBitOffset = (int) (startBit & 7L);
        int endBitOffset = (int) (endBit & 7L);

        if (startByte == endByte) {
            int mask = ((1 << (endBitOffset - startBitOffset)) - 1) << startBitOffset;
            byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, startByte);
            validityBitmap.set(ValueLayout.JAVA_BYTE, startByte, (byte) (b | mask));
        } else {
            if (startBitOffset != 0) {
                int mask = -(1 << startBitOffset);
                byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, startByte);
                validityBitmap.set(ValueLayout.JAVA_BYTE, startByte, (byte) (b | mask));
                startByte++;
            }
            if (startByte < endByte) {
                validityBitmap.asSlice(startByte, endByte - startByte).fill((byte) 0xFF);
            }
            if (endBitOffset != 0) {
                int mask = (1 << endBitOffset) - 1;
                byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, endByte);
                validityBitmap.set(ValueLayout.JAVA_BYTE, endByte, (byte) (b | mask));
            }
        }
        this.nullCount = 0;
    }

    @Override
    public long nullCount() {
        if (this.nullCount >= 0) {
            return this.nullCount;
        }
        if (this.length == 0 || lacksValidityBitmap()) {
            this.nullCount = 0;
            return 0;
        }

        long startBit = this.offset;
        long endBit = this.offset + this.length;
        long startByte = startBit >>> 3;
        long endByte = endBit >>> 3;
        int startBitOffset = (int) (startBit & 7L);
        int endBitOffset = (int) (endBit & 7L);

        long validBits = 0;

        if (startByte == endByte) {
            int mask = ((1 << (endBitOffset - startBitOffset)) - 1) << startBitOffset;
            int b = validityBitmap.get(ValueLayout.JAVA_BYTE, startByte) & 0xFF;
            validBits += Integer.bitCount(b & mask);
        } else {
            if (startBitOffset != 0) {
                int mask = -(1 << startBitOffset) & 0xFF;
                int b = validityBitmap.get(ValueLayout.JAVA_BYTE, startByte) & 0xFF;
                validBits += Integer.bitCount(b & mask);
                startByte++;
            }

            while (startByte + Long.BYTES <= endByte) {
                long word = validityBitmap.get(ValueLayout.JAVA_LONG_UNALIGNED, startByte);
                validBits += Long.bitCount(word);
                startByte += Long.BYTES;
            }

            while (startByte < endByte) {
                int b = validityBitmap.get(ValueLayout.JAVA_BYTE, startByte) & 0xFF;
                validBits += Integer.bitCount(b);
                startByte++;
            }

            if (endBitOffset != 0) {
                int mask = (1 << endBitOffset) - 1;
                int b = validityBitmap.get(ValueLayout.JAVA_BYTE, endByte) & 0xFF;
                validBits += Integer.bitCount(b & mask);
            }
        }

        this.nullCount = this.length - validBits;
        return this.nullCount;
    }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        if (index == 0) {
            return validityBitmap;
        }
        throw new IndexOutOfBoundsException("Invalid buffer index: " + index);
    }
}