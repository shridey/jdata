package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class UTF8Buffer extends AbstractDataBuffer {

    private final MemorySegment offsetsBuffer;
    private final MemorySegment dataBuffer;

    UTF8Buffer(String name, Arena arena, long length, long offset, MemorySegment validityBitmap,
               MemorySegment offsetsBuffer, MemorySegment dataBuffer) {
        super(name, arena, length, offset, validityBitmap);
        this.offsetsBuffer = offsetsBuffer;
        this.dataBuffer = dataBuffer;
    }

    public static UTF8Buffer allocate(Arena arena, String name, String[] values) {
        Objects.requireNonNull(arena, "Arena cannot be null");
        Objects.requireNonNull(name, "Column name cannot be null");
        Objects.requireNonNull(values, "Values array cannot be null");

        long length = values.length;
        long bitmapBytes = (length + 7L) >>> 3;
        long offsetsBytes = Math.multiplyExact(length + 1L, ValueLayout.JAVA_INT.byteSize());
        long totalDataBytes = computeTotalUtf8Bytes(values);

        if (totalDataBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Total UTF-8 data exceeds Integer.MAX_VALUE bytes; use LARGE_UTF8.");
        }

        MemorySegment validityBitmap = ArrowMemoryAllocator.allocateAligned(arena, bitmapBytes);
        MemorySegment offsetsBuffer = ArrowMemoryAllocator.allocateAligned(arena, offsetsBytes);
        MemorySegment dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, totalDataBytes);

        UTF8Buffer buffer = new UTF8Buffer(name, arena, length, 0, validityBitmap, offsetsBuffer, dataBuffer);
        buffer.setAllValid();

        int currentOffset = 0;
        for (int i = 0; i < values.length; i++) {
            buffer.offsetsBuffer.set(ValueLayout.JAVA_INT, (long) i * ValueLayout.JAVA_INT.byteSize(), currentOffset);

            String val = values[i];
            if (val == null) {
                buffer.setNull(i);
            } else {
                currentOffset = encodeUtf8(val, buffer.dataBuffer, currentOffset);
            }
        }

        buffer.offsetsBuffer.set(ValueLayout.JAVA_INT, length * ValueLayout.JAVA_INT.byteSize(), currentOffset);
        return buffer;
    }

    /**
     * Zero-copy wraps pre-allocated or memory-mapped validity, 32-bit offsets, and UTF-8 byte segments.
     */
    public static UTF8Buffer wrap(String name, Arena arena, long length, long offset,
                                  MemorySegment validityBitmap, MemorySegment offsetsBuffer, MemorySegment dataBuffer) {
        Objects.requireNonNull(offsetsBuffer, "offsetsBuffer cannot be null");
        long totalElements = Math.addExact(length, offset);
        long minOffsetsBytes = Math.multiplyExact(totalElements + 1L, ValueLayout.JAVA_INT.byteSize());
        if (offsetsBuffer.byteSize() < minOffsetsBytes) {
            throw new IllegalArgumentException(
                    "offsetsBuffer byteSize (" + offsetsBuffer.byteSize() + ") is smaller than required (" + minOffsetsBytes + ")"
            );
        }
        int lastOffset = offsetsBuffer.get(ValueLayout.JAVA_INT, totalElements * ValueLayout.JAVA_INT.byteSize());
        if (lastOffset < 0) {
            throw new IllegalArgumentException("Invalid negative terminal UTF-8 byte offset: " + lastOffset);
        }
        ArrowMemoryAllocator.validateWrapBounds(name, arena, length, offset, validityBitmap, dataBuffer, lastOffset);
        return new UTF8Buffer(name, arena, length, offset, validityBitmap, offsetsBuffer, dataBuffer);
    }

    static long computeTotalUtf8Bytes(String[] values) {
        long totalDataBytes = 0;
        for (String value : values) {
            if (value != null) {
                totalDataBytes += utf8ByteLength(value);
            }
        }
        return totalDataBytes;
    }

    static int utf8ByteLength(String value) {
        int byteLen = 0;
        int strLen = value.length();
        for (int j = 0; j < strLen; j++) {
            char c = value.charAt(j);
            if (c <= 0x7F) {
                byteLen++;
            } else if (c <= 0x7FF) {
                byteLen += 2;
            } else if (Character.isSurrogate(c)) {
                if (Character.isHighSurrogate(c) && j + 1 < strLen && Character.isLowSurrogate(value.charAt(j + 1))) {
                    byteLen += 4;
                    j++;
                } else {
                    byteLen++;
                }
            } else {
                byteLen += 3;
            }
        }
        return byteLen;
    }

    static int encodeUtf8(String val, MemorySegment target, int offset) {
        int cursor = offset;
        int strLen = val.length();
        for (int j = 0; j < strLen; j++) {
            char c = val.charAt(j);
            if (c <= 0x7F) {
                target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) c);
            } else if (c <= 0x7FF) {
                target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) (0xC0 | (c >>> 6)));
                target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) (0x80 | (c & 0x3F)));
            } else if (Character.isSurrogate(c)) {
                if (Character.isHighSurrogate(c) && j + 1 < strLen && Character.isLowSurrogate(val.charAt(j + 1))) {
                    char low = val.charAt(++j);
                    int codePoint = Character.toCodePoint(c, low);
                    target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) (0xF0 | (codePoint >>> 18)));
                    target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) (0x80 | ((codePoint >>> 12) & 0x3F)));
                    target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) (0x80 | ((codePoint >>> 6) & 0x3F)));
                    target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) (0x80 | (codePoint & 0x3F)));
                } else {
                    target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) 0x3F);
                }
            } else {
                target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) (0xE0 | (c >>> 12)));
                target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) (0x80 | ((c >>> 6) & 0x3F)));
                target.set(ValueLayout.JAVA_BYTE, cursor++, (byte) (0x80 | (c & 0x3F)));
            }
        }
        return cursor;
    }

    public String getString(long index) {
        checkBounds(index);

        if (isNull(index)) {
            return null;
        }

        long physicalIndex = index + this.offset;
        long startOffsetByteIndex = physicalIndex * ValueLayout.JAVA_INT.byteSize();
        int startByte = offsetsBuffer.get(ValueLayout.JAVA_INT, startOffsetByteIndex);
        int endByte = offsetsBuffer.get(ValueLayout.JAVA_INT, startOffsetByteIndex + ValueLayout.JAVA_INT.byteSize());

        int stringLength = endByte - startByte;
        if (stringLength == 0) return "";

        byte[] bytes = new byte[stringLength];
        MemorySegment.copy(dataBuffer, ValueLayout.JAVA_BYTE, startByte, bytes, 0, stringLength);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public String[] toArray() {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }

        int len = (int) this.length;
        String[] target = new String[len];

        for (int i = 0; i < len; i++) {
            target[i] = getString(i);
        }
        return target;
    }

    @Override
    public UTF8BufferBuilder toBuilder(Arena arena) {
        long absoluteEndIndex = this.offset + this.length;
        long startOffsetByteIndex = this.offset * ValueLayout.JAVA_INT.byteSize();
        long endOffsetByteIndex = absoluteEndIndex * ValueLayout.JAVA_INT.byteSize();

        int startByte = this.offsetsBuffer.get(ValueLayout.JAVA_INT, startOffsetByteIndex);
        int endByte = this.offsetsBuffer.get(ValueLayout.JAVA_INT, endOffsetByteIndex);
        int totalSliceDataBytes = endByte - startByte;

        UTF8BufferBuilder builder = new UTF8BufferBuilder(arena, this.name, this.length, totalSliceDataBytes);
        return builder.appendBlock(this, 0, this.length);
    }

    @Override
    public UTF8Buffer slice(long sliceOffset, long sliceLength) {
        Objects.checkFromIndexSize(sliceOffset, sliceLength, this.length);
        long absoluteOffset = this.offset + sliceOffset;
        return new UTF8Buffer(this.name, this.arena, sliceLength, absoluteOffset,
                this.validityBitmap, this.offsetsBuffer, this.dataBuffer);
    }

    @Override
    public UTF8Buffer withName(String newName) {
        Objects.requireNonNull(newName, "newName cannot be null");
        return new UTF8Buffer(newName, this.arena, this.length, this.offset,
                this.validityBitmap, this.offsetsBuffer, this.dataBuffer);
    }

    @Override
    public UTF8Buffer take(Int32Buffer indices, Arena targetArena) {
        Objects.requireNonNull(indices, "indices cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        long outLen = indices.length();
        boolean idxHasNulls = indices.nullCount() > 0;
        boolean srcHasNulls = this.nullCount() > 0;
        long intBytes = ValueLayout.JAVA_INT.byteSize();

        // Pass 1: Sum exact byte length of selected UTF-8 slices
        long totalBytes = 0;
        for (long i = 0; i < outLen; i++) {
            if (!idxHasNulls || !indices.isNull(i)) {
                int rowIdx = indices.get(i);
                checkBounds(rowIdx);
                if (!srcHasNulls || !this.isNull(rowIdx)) {
                    long phys = this.offset + rowIdx;
                    int s = offsetsBuffer.get(ValueLayout.JAVA_INT, phys * intBytes);
                    int e = offsetsBuffer.get(ValueLayout.JAVA_INT, (phys + 1L) * intBytes);
                    totalBytes += (e - s);
                }
            }
        }

        if (totalBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException("Gathered UTF8Buffer exceeds Integer.MAX_VALUE bytes.");
        }

        MemorySegment outValidity = ArrowMemoryAllocator.allocateAligned(targetArena, (outLen + 7L) >>> 3);
        MemorySegment outOffsets = ArrowMemoryAllocator.allocateAligned(targetArena, Math.multiplyExact(outLen + 1L, intBytes));
        MemorySegment outData = ArrowMemoryAllocator.allocateAligned(targetArena, totalBytes);

        UTF8Buffer out = new UTF8Buffer(this.name, targetArena, outLen, 0, outValidity, outOffsets, outData);
        out.setAllValid();

        // Pass 2: Copy UTF-8 byte ranges directly off-heap
        int cursor = 0;
        for (long i = 0; i < outLen; i++) {
            outOffsets.set(ValueLayout.JAVA_INT, i * intBytes, cursor);
            if (idxHasNulls && indices.isNull(i)) {
                out.setNull(i);
            } else {
                int rowIdx = indices.get(i);
                if (srcHasNulls && this.isNull(rowIdx)) {
                    out.setNull(i);
                } else {
                    long phys = this.offset + rowIdx;
                    int s = offsetsBuffer.get(ValueLayout.JAVA_INT, phys * intBytes);
                    int e = offsetsBuffer.get(ValueLayout.JAVA_INT, (phys + 1L) * intBytes);
                    int len = e - s;
                    if (len > 0) {
                        MemorySegment.copy(this.dataBuffer, s, outData, cursor, len);
                        cursor += len;
                    }
                }
            }
        }
        outOffsets.set(ValueLayout.JAVA_INT, outLen * intBytes, cursor);
        return out;
    }

    @Override public DataType type() { return DataType.UTF8; }
    @Override public int bufferCount() { return 3; }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0);
            case 1 -> offsetsBuffer;
            case 2 -> dataBuffer;
            default -> throw new IndexOutOfBoundsException("UTF8Buffer has exactly 3 buffers.");
        };
    }
}