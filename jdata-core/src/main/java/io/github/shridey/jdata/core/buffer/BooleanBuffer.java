package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A sealed, read-only Arrow column for Booleans.
 * Unlike integers which take 4 bytes each, these booleans are bit-packed
 * (8 true/false values fit inside a single byte) to save massive amounts of memory.
 */
public final class BooleanBuffer extends AbstractDataBuffer {

    private final MemorySegment dataBuffer;

    // Package-private constructor so only the Builder or static allocate() can create it
    BooleanBuffer(String name, Arena arena, long length, long offset, MemorySegment validityBitmap, MemorySegment dataBuffer) {
        super(name, arena, length, offset, validityBitmap);
        this.dataBuffer = dataBuffer;
    }

    public static BooleanBuffer allocate(Arena arena, String name, long length) {
        long byteSize = (length + 7) / 8;

        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, byteSize);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, byteSize);
        dataBuffer.fill((byte) 0x00);

        BooleanBuffer buffer = new BooleanBuffer(name, arena, length, 0, validityBitmap, dataBuffer);
        buffer.setAllValid();
        return buffer;
    }

    public void set(long index, boolean value) {
        checkBounds(index);
        long physicalIndex = index + this.offset;

        long byteIndex = physicalIndex / 8;
        int bitIndex = (int) (physicalIndex % 8);
        byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);

        if (value) b = (byte) (b | (1 << bitIndex));
        else       b = (byte) (b & ~(1 << bitIndex));

        dataBuffer.set(ValueLayout.JAVA_BYTE, byteIndex, b);
        markValid(index);
    }

    public boolean get(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;

        long byteIndex = physicalIndex / 8;
        int bitIndex = (int) (physicalIndex % 8);
        byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);

        return (b & (1 << bitIndex)) != 0;
    }

    public void copyBlock(BooleanBuffer source, long srcIndex, long destIndex, long length) {
        if (length <= 0) return;

        if (destIndex + length > this.length) throw new IndexOutOfBoundsException();
        if (srcIndex + length > source.length) throw new IndexOutOfBoundsException();

        // FAST PATH: Only applies if both the slice offsets AND the local indices align perfectly on byte boundaries
        long srcPhysical = srcIndex + source.offset;
        long destPhysical = destIndex + this.offset;

        if (srcPhysical % 8 == 0 && destPhysical % 8 == 0 && length % 8 == 0) {
            long byteOffsetSrc = srcPhysical / 8;
            long byteOffsetDest = destPhysical / 8;
            long byteLength = length / 8;

            MemorySegment.copy(source.dataBuffer, byteOffsetSrc, this.dataBuffer, byteOffsetDest, byteLength);
            MemorySegment.copy(source.buffer(0), byteOffsetSrc, this.buffer(0), byteOffsetDest, byteLength);
            return;
        }

        // SLOW PATH: Manual bit-by-bit copy
        for (long i = 0; i < length; i++) {
            long currentSrc = srcIndex + i;
            long currentDest = destIndex + i;

            this.set(currentDest, source.get(currentSrc));
            if (source.isNull(currentSrc)) this.setNull(currentDest);
            else this.markValid(currentDest);
        }
    }

    public void copyFrom(boolean[] source) {
        if (source.length != this.length) throw new IllegalArgumentException();

        // If sliced, we must use the slow path to avoid overwriting neighbors
        if (this.offset != 0) {
            for (int i = 0; i < source.length; i++) this.set(i, source[i]);
            return;
        }

        // Fast path for unsliced arrays
        for (int i = 0; i < source.length; i++) {
            long byteIndex = i / 8;
            int bitIndex = i % 8;
            byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);

            if (source[i]) b = (byte) (b | (1 << bitIndex));
            else b = (byte) (b & ~(1 << bitIndex));

            dataBuffer.set(ValueLayout.JAVA_BYTE, byteIndex, b);
        }
        setAllValid();
    }

    public boolean[] toArray() {
        if (this.length > Integer.MAX_VALUE) throw new IllegalStateException();
        boolean[] target = new boolean[(int) this.length];
        for (int i = 0; i < this.length; i++) {
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
        if (sliceOffset < 0 || sliceLength < 0 || sliceOffset + sliceLength > this.length) {
            throw new IndexOutOfBoundsException("Invalid slice parameters");
        }
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
            default -> throw new IndexOutOfBoundsException();
        };
    }
}