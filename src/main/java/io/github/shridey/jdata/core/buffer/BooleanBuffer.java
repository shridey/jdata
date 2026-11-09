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
    BooleanBuffer(String name, Arena arena, long length, MemorySegment validityBitmap, MemorySegment dataBuffer) {
        super(name, arena, length, validityBitmap);
        this.dataBuffer = dataBuffer;
    }

    public static BooleanBuffer allocate(Arena arena, String name, long length) {
        long byteSize = (length + 7) / 8;

        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, byteSize);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, byteSize);
        dataBuffer.fill((byte) 0x00);

        BooleanBuffer buffer = new BooleanBuffer(name, arena, length, validityBitmap, dataBuffer);
        buffer.setAllValid();
        return buffer;
    }

    public void set(long index, boolean value) {
        checkBounds(index); // Security check from parent

        long byteIndex = index / 8;
        int bitIndex = (int) (index % 8);
        byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);

        if (value) {
            b = (byte) (b | (1 << bitIndex));
        } else {
            b = (byte) (b & ~(1 << bitIndex));
        }

        dataBuffer.set(ValueLayout.JAVA_BYTE, byteIndex, b);
        markValid(index); // Tell parent to update null tracker
    }

    public boolean get(long index) {
        checkBounds(index); // Security check from parent

        long byteIndex = index / 8;
        int bitIndex = (int) (index % 8);
        byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);

        return (b & (1 << bitIndex)) != 0;
    }

    public void copyBlock(BooleanBuffer source, long srcIndex, long destIndex, long length) {
        if (length <= 0) return;

        // Prevent copying out of physical memory boundaries
        if (destIndex + length > this.length) throw new IndexOutOfBoundsException();
        if (srcIndex + length > source.length) throw new IndexOutOfBoundsException();

        // FAST PATH: If both chunks start perfectly on a byte boundary (index 0, 8, 16...)
        // we skip bitwise math and use the CPU to blast the raw bytes across instantly.
        if (srcIndex % 8 == 0 && destIndex % 8 == 0 && length % 8 == 0) {
            long byteOffsetSrc = srcIndex / 8;
            long byteOffsetDest = destIndex / 8;
            long byteLength = length / 8;

            MemorySegment.copy(source.dataBuffer, byteOffsetSrc, this.dataBuffer, byteOffsetDest, byteLength);
            MemorySegment.copy(source.buffer(0), byteOffsetSrc, this.buffer(0), byteOffsetDest, byteLength);
            return;
        }

        // SLOW PATH: If bits cross byte boundaries awkwardly, we must copy one-by-one.
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

        // Pack the standard Java boolean[] into our 1-bit Arrow format
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
            long byteIndex = i / 8;
            int bitIndex = i % 8;
            byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);
            target[i] = (b & (1 << bitIndex)) != 0;
        }
        return target;
    }

    @Override public DataType type() { return DataType.BOOLEAN; }
    @Override public int bufferCount() { return 2; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0); // Validity bitmap (inherited)
            case 1 -> dataBuffer;      // Bit-packed booleans
            default -> throw new IndexOutOfBoundsException();
        };
    }
}