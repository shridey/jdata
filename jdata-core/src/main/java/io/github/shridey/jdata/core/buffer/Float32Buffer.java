package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A sealed, read-only Arrow column for 32-bit floating-point numbers (floats).
 * Utilizes hardware-aligned contiguous memory to enable SIMD vectorization.
 */
public final class Float32Buffer extends AbstractDataBuffer {

    // The physical 4-byte-per-element data block (Buffer 1 in Arrow spec)
    private final MemorySegment dataBuffer;

    // Package-private constructor: Only Float32BufferBuilder or allocate() can create this.
    Float32Buffer(String name, Arena arena, long length, long offset,
                  MemorySegment validityBitmap, MemorySegment dataBuffer) {
        super(name, arena, length, offset, validityBitmap);
        this.dataBuffer = dataBuffer;
    }

    /**
     * Allocates a fixed-size buffer instantly.
     * Used when the exact number of rows is known prior to allocation.
     */
    public static Float32Buffer allocate(Arena arena, String name, long length) {
        long bitmapBytes = (length + 7) / 8;
        long dataBytes = length * ValueLayout.JAVA_FLOAT.byteSize();

        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);

        // Pass 0 as the absolute physical offset for new allocations
        Float32Buffer buffer = new Float32Buffer(name, arena, length, 0, validityBitmap, dataBuffer);
        buffer.setAllValid(); // Fill bitmap with 1s (all valid)

        return buffer;
    }

    public void set(long index, float value) {
        checkBounds(index); // Security check to prevent memory corruption

        // Convert logical row index to a physical memory byte offset
        long physicalIndex = index + this.offset;
        long byteOffset = physicalIndex * ValueLayout.JAVA_FLOAT.byteSize();
        dataBuffer.set(ValueLayout.JAVA_FLOAT, byteOffset, value);

        markValid(index); // Instruct the parent class to update the null tracker
    }

    public float get(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;
        long byteOffset = physicalIndex * ValueLayout.JAVA_FLOAT.byteSize();
        return dataBuffer.get(ValueLayout.JAVA_FLOAT, byteOffset);
    }

    public void copyBlock(Float32Buffer source, long srcIndex, long destIndex, long length) {
        if (length <= 0) return;

        // Ensure we don't accidentally read/write outside logical boundaries
        if (destIndex + length > this.length) throw new IndexOutOfBoundsException("Destination bounds exceeded");
        if (srcIndex + length > source.length) throw new IndexOutOfBoundsException("Source bounds exceeded");

        long byteSize = ValueLayout.JAVA_FLOAT.byteSize();

        // Account for logical offsets in both source and destination buffers
        long srcPhysical = srcIndex + source.offset;
        long destPhysical = destIndex + this.offset;

        long srcByteOffset = srcPhysical * byteSize;
        long destByteOffset = destPhysical * byteSize;
        long copyBytes = length * byteSize;

        // 1. Instantly copy the raw 32-bit float bytes using OS-level intrinsic speed
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
     * Ingestion: Dumps a Java float[] array into this buffer instantly.
     */
    public void copyFrom(float[] source) throws IllegalArgumentException {
        if (source.length != this.length) {
            throw new IllegalArgumentException("Array length must exactly match buffer length.");
        }

        // If this is a sliced buffer, bulk copying would overwrite memory outside logical boundaries.
        if (this.offset != 0) {
            for (int i = 0; i < source.length; i++) {
                this.set(i, source[i]);
            }
            return;
        }

        MemorySegment heapSegment = MemorySegment.ofArray(source);
        MemorySegment.copy(heapSegment, 0, this.dataBuffer, 0, (long) source.length * ValueLayout.JAVA_FLOAT.byteSize());
        setAllValid();
    }

    /**
     * Exfiltration: Dumps this off-heap buffer back into a standard Java float[] array.
     */
    public float[] toArray() throws IllegalStateException {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }

        float[] target = new float[(int) this.length];

        if (this.offset == 0) {
            // Fast path for standard, unsliced buffers
            MemorySegment heapSegment = MemorySegment.ofArray(target);
            MemorySegment.copy(this.dataBuffer, 0, heapSegment, 0, this.length * ValueLayout.JAVA_FLOAT.byteSize());
        } else {
            // Slow path for sliced buffers to prevent reading out-of-bounds bytes
            for (int i = 0; i < this.length; i++) {
                target[i] = this.get(i);
            }
        }

        return target;
    }

    @Override
    public Float32BufferBuilder toBuilder(Arena arena) {
        Float32BufferBuilder builder = new Float32BufferBuilder(arena, this.name, this.length);
        return builder.appendBlock(this, 0, this.length);
    }

    @Override
    public Float32Buffer slice(long sliceOffset, long sliceLength) {
        if (sliceOffset < 0 || sliceLength < 0 || sliceOffset + sliceLength > this.length) {
            throw new IndexOutOfBoundsException("Invalid slice parameters");
        }
        // Calculate the new absolute hardware offset
        long absoluteOffset = this.offset + sliceOffset;

        // Return a zero-copy view wrapping the EXACT SAME MemorySegments
        return new Float32Buffer(this.name, this.arena, sliceLength, absoluteOffset,
                this.validityBitmap, this.dataBuffer);
    }

    @Override public DataType type() { return DataType.FLOAT32; }
    @Override public int bufferCount() { return 2; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0); // Safely returns the validity bitmap
            case 1 -> dataBuffer;      // Returns the physical 32-bit floats
            default -> throw new IndexOutOfBoundsException("Float32Buffer has exactly 2 buffers.");
        };
    }
}