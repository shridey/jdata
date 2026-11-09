package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.ffi.ImportedDataBuffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * The base class for all finalized, read-only Arrow columns.
 * It provides the shared validity bitmap logic so child classes only worry about their specific data.
 */
public abstract sealed class AbstractDataBuffer implements DataBuffer
        permits BooleanBuffer, Float32Buffer, Float64Buffer, Int32Buffer, Int64Buffer, UTF8Buffer, ImportedDataBuffer {

    protected final String name;
    protected final Arena arena;
    protected final long length;
    protected final MemorySegment validityBitmap;

    // Cache the null count. -1 means "we don't know right now, recalculate it if asked".
    protected long nullCount = -1;

    protected AbstractDataBuffer(String name, Arena arena, long length, MemorySegment validityBitmap) {
        this.name = name;
        this.arena = arena;
        this.length = length;
        this.validityBitmap = validityBitmap;
    }

    /**
     * CRITICAL SECURITY BOUNDARY: Ensures the user isn't trying to read/write
     * past the declared logical length of the column.
     */
    protected void checkBounds(long index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("Index " + index + " is out of logical bounds for length " + length);
        }
    }

    protected void markValid(long index) {
        checkBounds(index); // Prevent corrupting padding bytes

        long byteIndex = index / 8;
        int bitIndex = (int) (index % 8);
        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);

        // If the bit is currently 0 (null), flip it to 1 using a bitwise OR mask
        if ((b & (1 << bitIndex)) == 0) {
            b = (byte) (b | (1 << bitIndex));
            validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, b);
            this.nullCount = -1; // Invalidate the cache since we changed the data
        }
    }

    private long recalculateNullCount() {
        long validCount = 0;
        long fullBytes = length / 8;

        // Count 1s in all full bytes using hardware POPCNT (very fast)
        for (long i = 0; i < fullBytes; i++) {
            byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, i);
            validCount += Integer.bitCount(Byte.toUnsignedInt(b));
        }

        // Count 1s in the final partial byte, masking away the padding bits
        int remainderBits = (int) (length % 8);
        if (remainderBits > 0) {
            byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, fullBytes);
            int mask = (1 << remainderBits) - 1;
            validCount += Integer.bitCount(Byte.toUnsignedInt(b) & mask);
        }

        // Nulls = Total Rows - Valid Rows
        return length - validCount;
    }

    @Override public String name() { return name; }
    @Override public long length() { return length; }
    @Override public Arena arena() { return arena; }

    @Override
    public void setNull(long index) {
        checkBounds(index); // Prevent corrupting padding bytes

        long byteIndex = index / 8;
        int bitIndex = (int) (index % 8);
        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);

        // Force the specific bit to 0
        b = (byte) (b & ~(1 << bitIndex));
        validityBitmap.set(ValueLayout.JAVA_BYTE, byteIndex, b);
        this.nullCount = -1;
    }

    @Override
    public boolean isNull(long index) {
        checkBounds(index); // Prevent reading garbage data

        long byteIndex = index / 8;
        int bitIndex = (int) (index % 8);
        byte b = validityBitmap.get(ValueLayout.JAVA_BYTE, byteIndex);

        // It is null if the bit at this position is a 0
        return (b & (1 << bitIndex)) == 0;
    }

    @Override
    public void setAllValid() {
        validityBitmap.fill((byte) 0xFF);
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
        throw new IndexOutOfBoundsException("Buffer index out of bounds. Subclasses must handle index > 0.");
    }
}