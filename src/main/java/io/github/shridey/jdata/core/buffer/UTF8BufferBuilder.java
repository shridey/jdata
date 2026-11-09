package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * Dynamically builds a variable-length UTF-8 string column.
 * Unlike primitive numbers which have fixed sizes, strings vary in length.
 * Therefore, this builder must manage TWO data capacities independently:
 * 1. Logical Capacity (How many rows/offsets we have)
 * 2. Data Capacity (How many total raw text bytes we have accumulated)
 */
public final class UTF8BufferBuilder extends AbstractBufferBuilder {

    // Tracks the starting byte of every string (Buffer 1 in Arrow spec)
    private MemorySegment offsetsBuffer;

    // Tracks the massive block of concatenated raw text (Buffer 2 in Arrow spec)
    private MemorySegment dataBuffer;

    private long dataCapacity;
    private int currentDataOffset = 0; // The write-head for the raw text

    public UTF8BufferBuilder(Arena arena, String name, long initialLogicalCapacity, long initialDataCapacity) {
        super(arena, name, initialLogicalCapacity);

        // Ensure we allocate at least 64 bytes for data to satisfy hardware alignment
        this.dataCapacity = Math.max(initialDataCapacity, 64);

        // Offsets are 32-bit integers (4 bytes each).
        // We need (logicalCapacity + 1) offsets because Arrow tracks the absolute end boundary too.
        long offsetsBytes = (logicalCapacity + 1) * ValueLayout.JAVA_INT.byteSize();
        this.offsetsBuffer = ArrowMemoryAllocator.allocateAligned(arena, offsetsBytes);
        this.offsetsBuffer.set(ValueLayout.JAVA_INT, 0, 0); // The first string always starts at byte 0

        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataCapacity);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        // The parent doubled the row count, so we must double the offsets buffer
        long newOffsetsBytes = (newLogicalCapacity + 1) * ValueLayout.JAVA_INT.byteSize();
        MemorySegment newOffsets = ArrowMemoryAllocator.allocateAligned(arena, newOffsetsBytes);

        // Native copy of the old offsets into the new segment
        long currentOffsetsBytes = (logicalLength + 1) * ValueLayout.JAVA_INT.byteSize();
        MemorySegment.copy(offsetsBuffer, 0, newOffsets, 0, currentOffsetsBytes);

        this.offsetsBuffer = newOffsets;
    }

    /**
     * Independently ensures the raw text buffer has enough physical room
     * for the specific string we are about to append.
     */
    private void ensureDataCapacity(long additionalBytes) {
        long targetData = currentDataOffset + additionalBytes;
        if (targetData > dataCapacity) {
            long newDataCapacity = Math.max(dataCapacity * 2, targetData);
            MemorySegment newData = ArrowMemoryAllocator.allocateAligned(arena, newDataCapacity);

            if (currentDataOffset > 0) {
                MemorySegment.copy(dataBuffer, 0, newData, 0, currentDataOffset);
            }
            this.dataBuffer = newData;
            this.dataCapacity = newDataCapacity;
        }
    }

    /**
     * Appends a single string to the column.
     */
    public UTF8BufferBuilder append(String value) {
        if (value == null) {
            return appendNull();
        }

        // 1. Calculate bytes and ensure we have enough memory
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        ensureLogicalCapacity(1); // Check row capacity
        ensureDataCapacity(bytes.length); // Check raw text capacity

        // 2. Write the raw UTF-8 bytes instantly
        MemorySegment.copy(bytes, 0, dataBuffer, ValueLayout.JAVA_BYTE, currentDataOffset, bytes.length);
        currentDataOffset += bytes.length;
        logicalLength++;

        // 3. Write the boundary offset so the engine knows where this string ends
        offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), currentDataOffset);
        return this;
    }

    /**
     * Appends a null row to the column.
     */
    public UTF8BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull(); // Parent flips the validity bit to 0
        logicalLength++;

        // For a null string, the start and end offsets are identical (length = 0)
        offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), currentDataOffset);
        return this;
    }

    /**
     * Zero-Allocation Bulk String Ingestion.
     * Capable of ingesting multi-million element arrays without triggering JVM Garbage Collection.
     */
    public UTF8BufferBuilder appendAll(String[] values) {
        if (values.length == 0) return this;

        long additionalBytes = getAdditionalBytes(values);

        // Safely resize off-heap memory exactly once for the entire array
        ensureLogicalCapacity(values.length);
        ensureDataCapacity(additionalBytes);

        // The Direct Encoding Pass (Zero Allocations on Java Heap)
        for (String val : values) {
            if (val == null) {
                markCurrentNull();
            } else {
                int strLen = val.length();
                for (int j = 0; j < strLen; j++) {
                    char c = val.charAt(j);

                    // Direct bitwise encoding from Java UTF-16 to Arrow UTF-8
                    if (c <= 0x7F) {
                        dataBuffer.set(ValueLayout.JAVA_BYTE, currentDataOffset++, (byte) c);
                    } else if (c <= 0x7FF) {
                        dataBuffer.set(ValueLayout.JAVA_BYTE, currentDataOffset++, (byte) (0xC0 | ((c >> 6) & 0x1F)));
                        dataBuffer.set(ValueLayout.JAVA_BYTE, currentDataOffset++, (byte) (0x80 | (c & 0x3F)));
                    } else if (Character.isHighSurrogate(c)) {
                        char low = val.charAt(++j);
                        int codePoint = Character.toCodePoint(c, low);
                        dataBuffer.set(ValueLayout.JAVA_BYTE, currentDataOffset++, (byte) (0xF0 | ((codePoint >> 18) & 0x07)));
                        dataBuffer.set(ValueLayout.JAVA_BYTE, currentDataOffset++, (byte) (0x80 | ((codePoint >> 12) & 0x3F)));
                        dataBuffer.set(ValueLayout.JAVA_BYTE, currentDataOffset++, (byte) (0x80 | ((codePoint >> 6) & 0x3F)));
                        dataBuffer.set(ValueLayout.JAVA_BYTE, currentDataOffset++, (byte) (0x80 | (codePoint & 0x3F)));
                    } else {
                        dataBuffer.set(ValueLayout.JAVA_BYTE, currentDataOffset++, (byte) (0xE0 | ((c >> 12) & 0x0F)));
                        dataBuffer.set(ValueLayout.JAVA_BYTE, currentDataOffset++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                        dataBuffer.set(ValueLayout.JAVA_BYTE, currentDataOffset++, (byte) (0x80 | (c & 0x3F)));
                    }
                }
            }

            // Advance the row count and write the boundary offset
            logicalLength++;
            offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), currentDataOffset);
        }

        return this;
    }

    private static long getAdditionalBytes(String[] values) {
        long additionalBytes = 0;
        // The Sizing Pass: Calculates exact physical UTF-8 footprint without allocating objects
        for (String val : values) {
            if (val != null) {
                int strLen = val.length();
                for (int j = 0; j < strLen; j++) {
                    char c = val.charAt(j);
                    if (c <= 0x7F) additionalBytes++;
                    else if (c <= 0x7FF) additionalBytes += 2;
                    else if (Character.isHighSurrogate(c)) {
                        additionalBytes += 4;
                        j++; // Skip the paired low surrogate
                    } else additionalBytes += 3;
                }
            }
        }
        return additionalBytes;
    }

    /**
     * Safely copies a block of strings from an existing sealed UTF8Buffer into this builder.
     * Uses a single bulk OS-level memory copy for maximum throughput.
     */
    public UTF8BufferBuilder copyBlock(UTF8Buffer source, long srcIndex, long length) {
        if (length <= 0) return this;

        long offsetByteSize = ValueLayout.JAVA_INT.byteSize();

        // 1. Calculate the exact physical byte slice of raw text to copy from the source
        int srcStartByte = source.buffer(1).get(ValueLayout.JAVA_INT, srcIndex * offsetByteSize);
        int srcEndByte = source.buffer(1).get(ValueLayout.JAVA_INT, (srcIndex + length) * offsetByteSize);
        long totalBytesToCopy = srcEndByte - srcStartByte;

        // 2. Ensure the builder has enough capacity to handle the incoming block
        ensureLogicalCapacity(length);
        ensureDataCapacity(totalBytesToCopy);

        // 3. FAST PATH: Bulk native copy of the raw string bytes instantly
        if (totalBytesToCopy > 0) {
            MemorySegment.copy(source.buffer(2), srcStartByte, dataBuffer, currentDataOffset, totalBytesToCopy);
        }

        // 4. Map the offsets and validity bitmap to the builder's write-head
        for (long i = 0; i < length; i++) {
            int sStart = source.buffer(1).get(ValueLayout.JAVA_INT, (srcIndex + i) * offsetByteSize);
            int sEnd = source.buffer(1).get(ValueLayout.JAVA_INT, (srcIndex + i + 1) * offsetByteSize);
            int stringLength = sEnd - sStart;

            currentDataOffset += stringLength;
            logicalLength++;
            offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * offsetByteSize, currentDataOffset);

            if (source.isNull(srcIndex + i)) {
                markCurrentNull();
            }
        }
        return this;
    }

    /**
     * Seals the builder into a read-only, immutable UTF8Buffer.
     */
    public UTF8Buffer build() {
        // Slice memory strictly to the written boundaries to hide padded bytes
        MemorySegment sealedValidity = validityBitmap.asSlice(0, (logicalLength + 7) / 8);
        MemorySegment sealedOffsets = offsetsBuffer.asSlice(0, (logicalLength + 1) * ValueLayout.JAVA_INT.byteSize());
        MemorySegment sealedData = dataBuffer.asSlice(0, currentDataOffset == 0 ? 64 : currentDataOffset);

        return new UTF8Buffer(name, arena, logicalLength, sealedValidity, sealedOffsets, sealedData);
    }
}