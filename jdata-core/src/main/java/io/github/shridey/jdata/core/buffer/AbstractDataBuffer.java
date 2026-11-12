package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.ffi.ImportedDataBuffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public abstract sealed class AbstractDataBuffer implements DataBuffer
        permits BooleanBuffer, Float32Buffer, Float64Buffer, Int32Buffer, Int64Buffer, UTF8Buffer, ImportedDataBuffer {

    protected final String name;
    protected final Arena arena;
    protected final long length;
    protected final long offset;
    protected final MemorySegment validityBitmap;

    protected long nullCount = -1;

    protected AbstractDataBuffer(String name, Arena arena, long length, long offset, MemorySegment validityBitmap) {
        this.name = name;
        this.arena = arena;
        this.length = length;
        this.offset = offset;
        this.validityBitmap = validityBitmap;
    }

    protected boolean lacksValidityBitmap() {
        return validityBitmap == null
                || validityBitmap.equals(MemorySegment.NULL)
                || validityBitmap.byteSize() == 0;
    }

    protected void checkBounds(long index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for slice length " + length);
        }
    }

    protected void markValid(long index) {
        checkBounds(index);
        if (lacksValidityBitmap()) {
            return;
        }

        long physicalIndex = index + this.offset;
        long byteIndex = physicalIndex >>> 3;
        int bitMask = 1 << (int) (physicalIndex & 7L);
        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);

        if ((b & bitMask) == 0) {
            validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (b | bitMask));
            if (this.nullCount > 0) {
                this.nullCount--;
            }
        }
    }

    private long recalculateNullCount() {
        if (length == 0 || lacksValidityBitmap()) {
            return 0L;
        }

        long validCount = 0;

        // FAST PATH: Slice starts on a byte boundary (0, 8, 16, ...)
        if ((this.offset & 7L) == 0) {
            long startByte = this.offset >>> 3;
            long fullLongs = length >>> 6; // 64 bits per long
            long byteCursor = startByte;

            for (long i = 0; i < fullLongs; i++) {
                long word = validityBitmap.get(ValueLayout.JAVA_LONG_UNALIGNED, byteCursor);
                validCount += Long.bitCount(word);
                byteCursor += Long.BYTES;
            }

            long remainingBits = length & 63L;
            long fullBytes = remainingBits >>> 3;
            for (long i = 0; i < fullBytes; i++) {
                byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteCursor++);
                validCount += Integer.bitCount(Byte.toUnsignedInt(b));
            }

            int tailBits = (int) (remainingBits & 7L);
            if (tailBits > 0) {
                byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteCursor);
                int mask = (1 << tailBits) - 1;
                validCount += Integer.bitCount(Byte.toUnsignedInt(b) & mask);
            }
        } else {
            // Unaligned bit offset
            for (long i = 0; i < length; i++) {
                long physicalIndex = this.offset + i;
                byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, physicalIndex >>> 3);
                validCount += (b >>> (int) (physicalIndex & 7L)) & 1;
            }
        }

        return length - validCount;
    }

    @Override public String name() { return name; }
    @Override public long length() { return length; }
    @Override public Arena arena() { return arena; }
    @Override public long offset() { return offset; }

    @Override
    public void setNull(long index) {
        checkBounds(index);
        if (lacksValidityBitmap()) {
            throw new UnsupportedOperationException("Cannot set null on a buffer without an allocated validity bitmap.");
        }

        long physicalIndex = index + this.offset;
        long byteIndex = physicalIndex >>> 3;
        int bitMask = 1 << (int) (physicalIndex & 7L);
        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);

        if ((b & bitMask) != 0) {
            validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (b & ~bitMask));
            if (this.nullCount != -1) {
                this.nullCount++;
            }
        }
    }

    @Override
    public boolean isNull(long index) {
        checkBounds(index);
        if (lacksValidityBitmap()) {
            return false;
        }

        long physicalIndex = index + this.offset;
        long byteIndex = physicalIndex >>> 3;
        int bitIndex = (int) (physicalIndex & 7L);
        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);

        return (b & (1 << bitIndex)) == 0;
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
        if (this.nullCount == -1) {
            this.nullCount = recalculateNullCount();
        }
        return this.nullCount;
    }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        if (index == 0) return validityBitmap;
        throw new IndexOutOfBoundsException("Buffer index out of bounds: " + index);
    }
}