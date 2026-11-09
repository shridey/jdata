package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * A sealed, read-only Arrow column for variable-length UTF-8 strings.
 * Relies on 3 separate memory buffers (Validity, Offsets, Data) to resolve boundaries.
 */
public final class UTF8Buffer extends AbstractDataBuffer {

    // Tracks where each string starts and ends (32-bit integers)
    private final MemorySegment offsetsBuffer;

    // The tightly packed, concatenated text bytes
    private final MemorySegment dataBuffer;

    // Package-private constructor: Only UTF8BufferBuilder or allocate() can create this.
    UTF8Buffer(String name, Arena arena, long length, MemorySegment validityBitmap,
               MemorySegment offsetsBuffer, MemorySegment dataBuffer) {
        super(name, arena, length, validityBitmap);
        this.offsetsBuffer = offsetsBuffer;
        this.dataBuffer = dataBuffer;
    }

    /**
     * Allocates a string buffer instantly from an existing Java array.
     */
    public static UTF8Buffer allocate(Arena arena, String name, String[] values) {
        long length = values.length;

        // 1. Pass 1: Calculate precise total text bytes
        long bitmapBytes = (length + 7) / 8;
        long offsetsBytes = (length + 1) * ValueLayout.JAVA_INT.byteSize();
        long totalDataBytes = getTotalDataBytes(values);

        // 2. Allocate aligned memory blocks
        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);
        MemorySegment offsetsBuffer = ArrowMemoryAllocator.allocateAligned(arena, offsetsBytes);
        long safeDataBytes = totalDataBytes == 0 ? 64 : totalDataBytes;
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, safeDataBytes);

        validityBitmap.fill((byte) 0xFF); // Mark all valid initially
        UTF8Buffer buffer = new UTF8Buffer(name, arena, length, validityBitmap, offsetsBuffer, dataBuffer);

        // 3. Pass 2: Write offsets and bytes
        int currentOffset = 0;
        for (int i = 0; i < length; i++) {
            buffer.offsetsBuffer.set(ValueLayout.JAVA_INT, (long) i * ValueLayout.JAVA_INT.byteSize(), currentOffset);

            if (values[i] == null) {
                buffer.setNull(i);
            } else {
                byte[] stringBytes = values[i].getBytes(StandardCharsets.UTF_8);
                MemorySegment.copy(stringBytes, 0, buffer.dataBuffer, ValueLayout.JAVA_BYTE, currentOffset, stringBytes.length);
                currentOffset += stringBytes.length;
            }
        }

        buffer.offsetsBuffer.set(ValueLayout.JAVA_INT, length * ValueLayout.JAVA_INT.byteSize(), currentOffset);
        return buffer;
    }

    private static long getTotalDataBytes(String[] values) {
        long totalDataBytes = 0;
        for (String value : values) {
            if (value != null) {
                int strLen = value.length();
                for (int j = 0; j < strLen; j++) {
                    char c = value.charAt(j);
                    if (c <= 0x7F) totalDataBytes++;
                    else if (c <= 0x7FF) totalDataBytes += 2;
                    else if (Character.isHighSurrogate(c)) {
                        totalDataBytes += 4;
                        j++;
                    } else totalDataBytes += 3;
                }
            }
        }
        return totalDataBytes;
    }

    public String getString(long index) {
        checkBounds(index); // Security check to prevent memory access violations

        if (isNull(index)) {
            return null;
        }

        // Read where the string begins
        long startOffsetByteIndex = index * ValueLayout.JAVA_INT.byteSize();
        int startByte = offsetsBuffer.get(ValueLayout.JAVA_INT, startOffsetByteIndex);

        // Read where the string ends (which is the beginning of the next string)
        long endOffsetByteIndex = (index + 1) * ValueLayout.JAVA_INT.byteSize();
        int endByte = offsetsBuffer.get(ValueLayout.JAVA_INT, endOffsetByteIndex);

        int stringLength = endByte - startByte;
        if (stringLength == 0) return "";

        // Slice the raw bytes and decode them into a Java String
        MemorySegment stringSegment = dataBuffer.asSlice(startByte, stringLength);
        byte[] bytes = stringSegment.toArray(ValueLayout.JAVA_BYTE);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public String[] toArray() throws IllegalStateException {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }

        int len = (int) this.length;
        String[] target = new String[len];

        for (int i = 0; i < len; i++) {
            target[i] = getString(i); // Handled safely by validity checks inside getString
        }
        return target;
    }

    @Override public DataType type() { return DataType.UTF8; }
    @Override public int bufferCount() { return 3; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0); // Validity bitmap
            case 1 -> offsetsBuffer;   // 32-bit string boundaries
            case 2 -> dataBuffer;      // Raw UTF-8 string data
            default -> throw new IndexOutOfBoundsException("UTF8Buffer has exactly 3 buffers.");
        };
    }
}