package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * Dynamically builds a variable-length UTF-8 string column.
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

        long offsetsBytes = (logicalCapacity + 1) * ValueLayout.JAVA_INT.byteSize();
        this.offsetsBuffer = ArrowMemoryAllocator.allocateAligned(arena, offsetsBytes);
        this.offsetsBuffer.set(ValueLayout.JAVA_INT, 0, 0);

        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataCapacity);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        long newOffsetsBytes = (newLogicalCapacity + 1) * ValueLayout.JAVA_INT.byteSize();
        MemorySegment newOffsets = ArrowMemoryAllocator.allocateAligned(arena, newOffsetsBytes);

        long currentOffsetsBytes = (logicalLength + 1) * ValueLayout.JAVA_INT.byteSize();
        MemorySegment.copy(offsetsBuffer, 0, newOffsets, 0, currentOffsetsBytes);

        this.offsetsBuffer = newOffsets;
    }

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

    public UTF8BufferBuilder append(String value) {
        if (value == null) return appendNull();

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        ensureLogicalCapacity(1);
        ensureDataCapacity(bytes.length);

        MemorySegment.copy(bytes, 0, dataBuffer, ValueLayout.JAVA_BYTE, currentDataOffset, bytes.length);
        currentDataOffset += bytes.length;
        logicalLength++;

        offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), currentDataOffset);
        return this;
    }

    public UTF8BufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull();
        logicalLength++;

        offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), currentDataOffset);
        return this;
    }

    public UTF8BufferBuilder appendAll(String[] values) {
        if (values.length == 0) return this;

        long additionalBytes = getAdditionalBytes(values);
        ensureLogicalCapacity(values.length);
        ensureDataCapacity(additionalBytes);

        for (String val : values) {
            if (val == null) {
                markCurrentNull();
            } else {
                int strLen = val.length();
                for (int j = 0; j < strLen; j++) {
                    char c = val.charAt(j);
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
            logicalLength++;
            offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), currentDataOffset);
        }
        return this;
    }

    private static long getAdditionalBytes(String[] values) {
        long additionalBytes = 0;
        for (String val : values) {
            if (val != null) {
                int strLen = val.length();
                for (int j = 0; j < strLen; j++) {
                    char c = val.charAt(j);
                    if (c <= 0x7F) additionalBytes++;
                    else if (c <= 0x7FF) additionalBytes += 2;
                    else if (Character.isHighSurrogate(c)) {
                        additionalBytes += 4;
                        j++;
                    } else additionalBytes += 3;
                }
            }
        }
        return additionalBytes;
    }

    /**
     * Safely copies a block of strings from an existing sealed UTF8Buffer.
     * Respects the slice offset of the source buffer.
     */
    public UTF8BufferBuilder appendBlock(UTF8Buffer source, long srcIndex, long length) {
        if (length <= 0) return this;

        long offsetByteSize = ValueLayout.JAVA_INT.byteSize();
        long srcPhysical = srcIndex + source.offset();

        int srcStartByte = source.buffer(1).get(ValueLayout.JAVA_INT, srcPhysical * offsetByteSize);
        int srcEndByte = source.buffer(1).get(ValueLayout.JAVA_INT, (srcPhysical + length) * offsetByteSize);
        long totalBytesToCopy = srcEndByte - srcStartByte;

        ensureLogicalCapacity(length);
        ensureDataCapacity(totalBytesToCopy);

        if (totalBytesToCopy > 0) {
            MemorySegment.copy(source.buffer(2), srcStartByte, dataBuffer, currentDataOffset, totalBytesToCopy);
        }

        for (long i = 0; i < length; i++) {
            if (source.isNull(srcIndex + i)) {
                markCurrentNull();
            }

            int sStart = source.buffer(1).get(ValueLayout.JAVA_INT, (srcPhysical + i) * offsetByteSize);
            int sEnd = source.buffer(1).get(ValueLayout.JAVA_INT, (srcPhysical + i + 1) * offsetByteSize);
            int stringLength = sEnd - sStart;

            currentDataOffset += stringLength;
            logicalLength++;
            offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * offsetByteSize, currentDataOffset);
        }
        return this;
    }

    /**
     * Safely copies strings from a generic DataBuffer imported via FFI.
     */
    public UTF8BufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        if (source.type() != DataType.UTF8) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into a UTF8 builder.");
        }

        if (length <= 0) return this;

        long offsetByteSize = ValueLayout.JAVA_INT.byteSize();
        long srcPhysical = srcIndex + source.offset();
        MemorySegment srcOffsets = source.buffer(1);

        int srcStartByte = srcOffsets.get(ValueLayout.JAVA_INT, srcPhysical * offsetByteSize);
        int srcEndByte = srcOffsets.get(ValueLayout.JAVA_INT, (srcPhysical + length) * offsetByteSize);
        long totalBytesToCopy = srcEndByte - srcStartByte;

        ensureLogicalCapacity(length);
        ensureDataCapacity(totalBytesToCopy);

        if (totalBytesToCopy > 0) {
            MemorySegment.copy(source.buffer(2), srcStartByte, dataBuffer, currentDataOffset, totalBytesToCopy);
        }

        for (long i = 0; i < length; i++) {
            if (source.isNull(srcIndex + i)) {
                markCurrentNull();
            }

            int sStart = srcOffsets.get(ValueLayout.JAVA_INT, (srcPhysical + i) * offsetByteSize);
            int sEnd = srcOffsets.get(ValueLayout.JAVA_INT, (srcPhysical + i + 1) * offsetByteSize);
            int stringLength = sEnd - sStart;

            currentDataOffset += stringLength;
            logicalLength++;
            offsetsBuffer.set(ValueLayout.JAVA_INT, logicalLength * offsetByteSize, currentDataOffset);
        }
        return this;
    }

    public UTF8Buffer build() {
        MemorySegment sealedValidity = validityBitmap.asSlice(0, (logicalLength + 7) / 8);
        MemorySegment sealedOffsets = offsetsBuffer.asSlice(0, (logicalLength + 1) * ValueLayout.JAVA_INT.byteSize());
        MemorySegment sealedData = dataBuffer.asSlice(0, currentDataOffset == 0 ? 64 : currentDataOffset);

        // Pass 0 as the absolute physical offset for newly built buffers
        return new UTF8Buffer(name, arena, logicalLength, 0, sealedValidity, sealedOffsets, sealedData);
    }
}