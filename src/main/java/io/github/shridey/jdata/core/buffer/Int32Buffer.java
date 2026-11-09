package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A sealed, read-only Arrow column for 32-bit signed integers.
 * Uses contiguous off-heap memory to bypass JVM Garbage Collection.
 */
public final class Int32Buffer extends AbstractDataBuffer {

    // The physical 4-byte-per-element data block (Buffer 1 in Arrow spec)
    private final MemorySegment dataBuffer;

    // Package-private constructor: Only Int32BufferBuilder or allocate() can create this.
    Int32Buffer(String name, Arena arena, long length, MemorySegment validityBitmap, MemorySegment dataBuffer) {
        super(name, arena, length, validityBitmap);
        this.dataBuffer = dataBuffer;
    }

    /**
     * Allocates a fixed-size buffer instantly.
     * Use this if you know EXACTLY how many rows you need upfront.
     */
    public static Int32Buffer allocate(Arena arena, String name, long length) {
        long bitmapBytes = (length + 7) / 8;
        long dataBytes = length * ValueLayout.JAVA_INT.byteSize();

        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);

        Int32Buffer buffer = new Int32Buffer(name, arena, length, validityBitmap, dataBuffer);
        buffer.setAllValid(); // Fill bitmap with 1s (all valid)

        return buffer;
    }

    public void set(long index, int value) {
        checkBounds(index); // Security check to prevent writing into memory padding

        // Convert the logical row index to a physical memory byte offset.
        // E.g., Index 2 = Byte Offset 8.
        long byteOffset = index * ValueLayout.JAVA_INT.byteSize();
        dataBuffer.set(ValueLayout.JAVA_INT, byteOffset, value);
        markValid(index); // Ensure the parent class updates the null tracker
    }

    public int get(long index) {
        checkBounds(index);
        long byteOffset = index * ValueLayout.JAVA_INT.byteSize();
        return dataBuffer.get(ValueLayout.JAVA_INT, byteOffset);
    }

    public void copyBlock(Int32Buffer source, long srcIndex, long destIndex, long length) {
        if (length <= 0) return;

        // Ensure we don't accidentally overwrite memory outside our logical boundaries
        if (destIndex + length > this.length) throw new IndexOutOfBoundsException("Destination bounds exceeded");
        if (srcIndex + length > source.length) throw new IndexOutOfBoundsException("Source bounds exceeded");

        long byteSize = ValueLayout.JAVA_INT.byteSize();
        long srcByteOffset = srcIndex * byteSize;
        long destByteOffset = destIndex * byteSize;
        long copyBytes = length * byteSize;

        // 1. Instantly copy the raw integer bytes using OS-level intrinsic speed
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
     * Ingestion: Dumps an entire Java array into this buffer instantly.
     */
    public void copyFrom(int[] source) throws IllegalArgumentException {
        if (source.length != this.length) {
            throw new IllegalArgumentException("Array length must exactly match buffer length.");
        }

        MemorySegment heapSegment = MemorySegment.ofArray(source);
        MemorySegment.copy(heapSegment, 0, this.dataBuffer, 0, (long) source.length * ValueLayout.JAVA_INT.byteSize());
        setAllValid();
    }

    /**
     * Exfiltration: Dumps this off-heap buffer back into a standard Java array.
     */
    public int[] toArray() throws IllegalStateException {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }

        int[] target = new int[(int) this.length];
        MemorySegment heapSegment = MemorySegment.ofArray(target);
        MemorySegment.copy(this.dataBuffer, 0, heapSegment, 0, this.length * ValueLayout.JAVA_INT.byteSize());

        return target;
    }

    @Override public DataType type() { return DataType.INT32; }
    @Override public int bufferCount() { return 2; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0); // Safely returns the validity bitmap
            case 1 -> dataBuffer;      // Returns the physical integers
            default -> throw new IndexOutOfBoundsException("Int32Buffer has exactly 2 buffers.");
        };
    }
}