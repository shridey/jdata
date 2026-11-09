package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A sealed, read-only Arrow column for 64-bit signed integers (longs).
 * Utilizes hardware-aligned contiguous memory to enable SIMD vectorization.
 */
public final class Int64Buffer extends AbstractDataBuffer {

    // The physical 8-byte-per-element data block (Buffer 1 in Arrow spec)
    private final MemorySegment dataBuffer;

    // Package-private constructor: Only Int64BufferBuilder or allocate() can create this.
    Int64Buffer(String name, Arena arena, long length,
                MemorySegment validityBitmap, MemorySegment dataBuffer) {
        super(name, arena, length, validityBitmap);
        this.dataBuffer = dataBuffer;
    }

    /**
     * Allocates a fixed-size buffer instantly.
     * Used when the exact number of rows is known prior to allocation.
     */
    public static Int64Buffer allocate(Arena arena, String name, long length) {
        long bitmapBytes = (length + 7) / 8;
        long dataBytes = length * ValueLayout.JAVA_LONG.byteSize();

        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);

        Int64Buffer buffer = new Int64Buffer(name, arena, length, validityBitmap, dataBuffer);
        buffer.setAllValid(); // Fill bitmap with 1s (all valid)

        return buffer;
    }

    public void set(long index, long value) {
        checkBounds(index); // Security check to prevent memory corruption

        // Convert row index to physical byte offset (index * 8 bytes)
        long byteOffset = index * ValueLayout.JAVA_LONG.byteSize();
        dataBuffer.set(ValueLayout.JAVA_LONG, byteOffset, value);

        markValid(index); // Instruct the parent class to update the null tracker
    }

    public long get(long index) {
        checkBounds(index);
        long byteOffset = index * ValueLayout.JAVA_LONG.byteSize();
        return dataBuffer.get(ValueLayout.JAVA_LONG, byteOffset);
    }

    public void copyBlock(Int64Buffer source, long srcIndex, long destIndex, long length) {
        if (length <= 0) return;

        // Ensure we don't accidentally read/write outside logical boundaries
        if (destIndex + length > this.length) throw new IndexOutOfBoundsException("Destination bounds exceeded");
        if (srcIndex + length > source.length) throw new IndexOutOfBoundsException("Source bounds exceeded");

        long byteSize = ValueLayout.JAVA_LONG.byteSize();
        long srcByteOffset = srcIndex * byteSize;
        long destByteOffset = destIndex * byteSize;
        long copyBytes = length * byteSize;

        // 1. Instantly copy the raw 64-bit integer bytes using OS-level intrinsic speed
        MemorySegment.copy(source.dataBuffer, srcByteOffset, this.dataBuffer, destByteOffset, copyBytes);

        // 2. Synchronize the validity bitmap bit-by-bit
        for (long i = 0; i < length; i++) {
            if (source.isNull(srcIndex + i)) {
                this.setNull(destIndex + i);
            } else {
                this.markValid(destIndex + i);
            }
        }
    }

    /**
     * Ingestion: Dumps a Java long[] array into this buffer instantly.
     */
    public void copyFrom(long[] source) throws IllegalArgumentException {
        if (source.length != this.length) {
            throw new IllegalArgumentException("Array length must exactly match buffer length.");
        }

        MemorySegment heapSegment = MemorySegment.ofArray(source);
        MemorySegment.copy(heapSegment, 0, this.dataBuffer, 0, (long) source.length * ValueLayout.JAVA_LONG.byteSize());
        setAllValid(); // Primitives cannot be null, mark all as valid
    }

    /**
     * Exfiltration: Dumps this off-heap buffer back into a standard Java long[] array.
     */
    public long[] toArray() throws IllegalStateException {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }

        long[] target = new long[(int) this.length];
        MemorySegment heapSegment = MemorySegment.ofArray(target);
        MemorySegment.copy(this.dataBuffer, 0, heapSegment, 0, this.length * ValueLayout.JAVA_LONG.byteSize());

        return target;
    }

    @Override public DataType type() { return DataType.INT64; }
    @Override public int bufferCount() { return 2; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0); // Safely returns the validity bitmap
            case 1 -> dataBuffer;      // Returns the physical 64-bit integers
            default -> throw new IndexOutOfBoundsException("Int64Buffer has exactly 2 buffers.");
        };
    }
}