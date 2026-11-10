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

    protected void checkBounds(long index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds for slice length " + length);
        }
    }

    protected void markValid(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;

        long byteIndex = physicalIndex / 8;
        int bitIndex = (int) (physicalIndex % 8);
        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);

        if ((b & (1 << bitIndex)) == 0) {
            b = (byte) (b | (1 << bitIndex));
            validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, b);
            this.nullCount = -1;
        }
    }

    private long recalculateNullCount() {
        long validCount = 0;

        // FAST PATH: If the slice starts perfectly on a byte boundary (0, 8, 16)
        if (this.offset % 8 == 0) {
            long startByte = this.offset / 8;
            long fullBytes = length / 8;

            for (long i = 0; i < fullBytes; i++) {
                byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, startByte + i);
                validCount += Integer.bitCount(Byte.toUnsignedInt(b));
            }

            int remainderBits = (int) (length % 8);
            if (remainderBits > 0) {
                byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, startByte + fullBytes);
                int mask = (1 << remainderBits) - 1;
                validCount += Integer.bitCount(Byte.toUnsignedInt(b) & mask);
            }
        } else {
            // SLOW PATH: Unaligned slice (bits cross byte boundaries)
            for (long i = 0; i < length; i++) {
                if (!isNull(i)) validCount++;
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
        long physicalIndex = index + this.offset;

        long byteIndex = physicalIndex / 8;
        int bitIndex = (int) (physicalIndex % 8);
        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);

        b = (byte) (b & ~(1 << bitIndex));
        validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, b);
        this.nullCount = -1;
    }

    @Override
    public boolean isNull(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;

        long byteIndex = physicalIndex / 8;
        int bitIndex = (int) (physicalIndex % 8);
        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);

        return (b & (1 << bitIndex)) == 0;
    }

    @Override
    public void setAllValid() {
        if (this.offset == 0 && this.length * 8 == validityBitmap.byteSize()) {
            validityBitmap.fill((byte) 0xFF);
        } else {
            // If sliced, only flip bits belonging to our specific window
            for(long i = 0; i < length; i++) markValid(i);
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
        throw new IndexOutOfBoundsException("Buffer index out of bounds.");
    }
}