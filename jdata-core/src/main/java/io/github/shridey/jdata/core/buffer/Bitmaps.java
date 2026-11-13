package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * 64-bit word-level bitmap algorithms for validity intersection, boolean vector logic,
 * and hardware-accelerated ({@code POPCNT} / {@code TZCNT}) SelectionVector extraction.
 */
public final class Bitmaps {

    private Bitmaps() {}

    /**
     * Extracts up to 64 bits from a bitmap starting at an arbitrary bit offset.
     * Safely handles unaligned bit offsets (from zero-copy slices) and tail boundaries.
     *
     * @param bitmap   the source off-heap bitmap segment (or {@link MemorySegment#NULL})
     * @param startBit the zero-based starting bit index
     * @param bitCount the number of bits to extract (must be in {@code [1, 64]})
     * @return a {@code long} containing the extracted bits in its lowest {@code bitCount} positions
     * @throws NullPointerException     if {@code bitmap} is {@code null}
     * @throws IllegalArgumentException if {@code startBit < 0} or {@code bitCount} is outside {@code [1, 64]}
     */
    public static long extractWord(MemorySegment bitmap, long startBit, int bitCount) {
        Objects.requireNonNull(bitmap, "bitmap cannot be null");
        if (startBit < 0) {
            throw new IllegalArgumentException("startBit cannot be negative: " + startBit);
        }
        if (bitCount <= 0 || bitCount > 64) {
            throw new IllegalArgumentException("bitCount must be in [1..64], got: " + bitCount);
        }
        long mask = bitCount == 64 ? -1L : (1L << bitCount) - 1L;
        if (bitmap.equals(MemorySegment.NULL) || bitmap.byteSize() == 0) {
            return mask;
        }

        long byteIndex = startBit >>> 3;
        int bitShift = (int) (startBit & 7L);
        long segSize = bitmap.byteSize();

        long word;
        if (bitShift == 0 && byteIndex + Long.BYTES <= segSize) {
            word = bitmap.get(ValueLayout.JAVA_LONG_UNALIGNED, byteIndex);
        } else if (byteIndex + Long.BYTES + 1L <= segSize) {
            long lo = bitmap.get(ValueLayout.JAVA_LONG_UNALIGNED, byteIndex);
            long hi = Byte.toUnsignedLong(bitmap.get(ValueLayout.JAVA_BYTE, byteIndex + Long.BYTES));
            word = (lo >>> bitShift) | (hi << (64 - bitShift));
        } else {
            word = 0L;
            int totalBitsNeeded = bitShift + bitCount;
            int bytesToRead = (totalBitsNeeded + 7) >>> 3;
            for (int b = 0; b < bytesToRead && (byteIndex + b) < segSize; b++) {
                long byteVal = Byte.toUnsignedLong(bitmap.get(ValueLayout.JAVA_BYTE, byteIndex + b));
                if (b < 8) {
                    word |= (byteVal << (b << 3));
                } else {
                    word = (word >>> bitShift) | (byteVal << (64 - bitShift));
                    return word & mask;
                }
            }
            word >>>= bitShift;
        }
        return word & mask;
    }

    /**
     * Computes the bitwise-AND validity bitmap of two equal-length buffers 64 bits at a time.
     * Used by binary compute kernels (e.g., {@code colA + colB}, {@code colA == colB}) in {@code jdata-num}.
     *
     * @param left        the left operand buffer
     * @param right       the right operand buffer
     * @param targetArena the arena in which to allocate the resulting validity bitmap
     * @return a 64-byte-aligned and padded {@link MemorySegment} representing the intersected validity bitmap
     * @throws IllegalArgumentException if {@code left.length() != right.length()}
     */
    public static MemorySegment intersectValidity(DataBuffer left, DataBuffer right, Arena targetArena) {
        Objects.requireNonNull(left, "left buffer cannot be null");
        Objects.requireNonNull(right, "right buffer cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        if (left.length() != right.length()) {
            throw new IllegalArgumentException(
                    "Buffer lengths must match for validity intersection: " + left.length() + " vs " + right.length()
            );
        }

        long length = left.length();
        long bitmapBytes = (length + 7L) >>> 3;
        MemorySegment out = ArrowMemoryAllocator.allocateAligned(targetArena, bitmapBytes);
        if (length == 0) {
            return out;
        }

        boolean leftHasNulls = left.nullCount() > 0;
        boolean rightHasNulls = right.nullCount() > 0;

        // Fast path: neither buffer has nulls
        if (!leftHasNulls && !rightHasNulls) {
            long fullBytes = length >>> 3;
            if (fullBytes > 0) {
                out.asSlice(0, fullBytes).fill((byte) 0xFF);
            }
            int tailBits = (int) (length & 7L);
            if (tailBits != 0) {
                out.set(ValueLayout.JAVA_BYTE, fullBytes, (byte) ((1 << tailBits) - 1));
            }
            return out;
        }

        MemorySegment leftSeg = leftHasNulls ? left.buffer(0) : MemorySegment.NULL;
        MemorySegment rightSeg = rightHasNulls ? right.buffer(0) : MemorySegment.NULL;
        long leftOffset = left.offset();
        long rightOffset = right.offset();

        long bitCursor = 0;
        long outByteCursor = 0;
        while (bitCursor < length) {
            int bits = (int) Math.min(64L, length - bitCursor);
            long wLeft = leftHasNulls ? extractWord(leftSeg, leftOffset + bitCursor, bits) : -1L;
            long wRight = rightHasNulls ? extractWord(rightSeg, rightOffset + bitCursor, bits) : -1L;
            long mask = bits == 64 ? -1L : (1L << bits) - 1L;
            out.set(ValueLayout.JAVA_LONG_UNALIGNED, outByteCursor, (wLeft & wRight) & mask);
            bitCursor += 64L;
            outByteCursor += Long.BYTES;
        }
        return out;
    }

    /**
     * Computes the element-wise logical AND of two {@link BooleanBuffer}s 64 bits at a time.
     *
     * @param left        the left boolean buffer
     * @param right       the right boolean buffer
     * @param resultName  the column name for the resulting buffer
     * @param targetArena the arena in which to allocate the result
     * @return a new {@link BooleanBuffer} containing {@code left AND right}
     */
    public static BooleanBuffer and(BooleanBuffer left, BooleanBuffer right, String resultName, Arena targetArena) {
        return applyBinaryOp(left, right, resultName, targetArena, BinaryBitOp.AND);
    }

    /**
     * Computes the element-wise logical OR of two {@link BooleanBuffer}s 64 bits at a time.
     *
     * @param left        the left boolean buffer
     * @param right       the right boolean buffer
     * @param resultName  the column name for the resulting buffer
     * @param targetArena the arena in which to allocate the result
     * @return a new {@link BooleanBuffer} containing {@code left OR right}
     */
    public static BooleanBuffer or(BooleanBuffer left, BooleanBuffer right, String resultName, Arena targetArena) {
        return applyBinaryOp(left, right, resultName, targetArena, BinaryBitOp.OR);
    }

    /**
     * Computes the element-wise logical XOR of two {@link BooleanBuffer}s 64 bits at a time.
     *
     * @param left        the left boolean buffer
     * @param right       the right boolean buffer
     * @param resultName  the column name for the resulting buffer
     * @param targetArena the arena in which to allocate the result
     * @return a new {@link BooleanBuffer} containing {@code left XOR right}
     */
    public static BooleanBuffer xor(BooleanBuffer left, BooleanBuffer right, String resultName, Arena targetArena) {
        return applyBinaryOp(left, right, resultName, targetArena, BinaryBitOp.XOR);
    }

    /**
     * Computes the element-wise logical NOT of a {@link BooleanBuffer} 64 bits at a time, preserving null slots.
     *
     * @param source      the source boolean buffer
     * @param resultName  the column name for the resulting buffer
     * @param targetArena the arena in which to allocate the result
     * @return a new {@link BooleanBuffer} containing {@code NOT source}
     */
    public static BooleanBuffer not(BooleanBuffer source, String resultName, Arena targetArena) {
        Objects.requireNonNull(source, "source cannot be null");
        Objects.requireNonNull(resultName, "resultName cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        long length = source.length();
        long bitmapBytes = (length + 7L) >>> 3;
        MemorySegment outValidity = ArrowMemoryAllocator.allocateAligned(targetArena, bitmapBytes);
        MemorySegment outData = ArrowMemoryAllocator.allocateAligned(targetArena, bitmapBytes);

        boolean hasNulls = source.nullCount() > 0;
        MemorySegment srcVal = source.buffer(0);
        MemorySegment srcData = source.buffer(1);
        long srcOffset = source.offset();

        long bitCursor = 0;
        long outByteCursor = 0;
        while (bitCursor < length) {
            int bits = (int) Math.min(64L, length - bitCursor);
            long mask = bits == 64 ? -1L : (1L << bits) - 1L;
            long valWord = hasNulls ? extractWord(srcVal, srcOffset + bitCursor, bits) : mask;
            long dataWord = (~extractWord(srcData, srcOffset + bitCursor, bits)) & mask;

            outValidity.set(ValueLayout.JAVA_LONG_UNALIGNED, outByteCursor, valWord);
            outData.set(ValueLayout.JAVA_LONG_UNALIGNED, outByteCursor, dataWord);
            bitCursor += 64L;
            outByteCursor += Long.BYTES;
        }

        return BooleanBuffer.wrap(resultName, targetArena, length, 0, outValidity, outData);
    }

    /**
     * Converts a {@link BooleanBuffer} filter mask into an {@link Int32Buffer} of matching row indices
     * (SelectionVector) using 64-bit word scans and hardware {@code TZCNT} ({@link Long#numberOfTrailingZeros}).
     * Null slots evaluate to {@code false}.
     *
     * @param mask        the boolean filter predicate mask
     * @param targetArena the arena in which to allocate the selection indices buffer
     * @return an {@link Int32Buffer} containing the zero-based indices where {@code mask} is valid and {@code true}
     */
    public static Int32Buffer toSelectionIndices(BooleanBuffer mask, Arena targetArena) {
        Objects.requireNonNull(mask, "mask cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        long length = mask.length();
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Mask length exceeds Integer.MAX_VALUE for 32-bit selection indices.");
        }

        boolean hasNulls = mask.nullCount() > 0;
        MemorySegment valSeg = mask.buffer(0);
        MemorySegment dataSeg = mask.buffer(1);
        long offset = mask.offset();

        // Pass 1: Count active (valid && true) bits using 64-bit POPCNT
        long matchCount = 0;
        for (long bitCursor = 0; bitCursor < length; bitCursor += 64L) {
            int bits = (int) Math.min(64L, length - bitCursor);
            long dataWord = extractWord(dataSeg, offset + bitCursor, bits);
            long activeWord = hasNulls ? (dataWord & extractWord(valSeg, offset + bitCursor, bits)) : dataWord;
            matchCount += Long.bitCount(activeWord);
        }

        Int32Buffer indices = Int32Buffer.allocate(targetArena, mask.name() + "_sel", matchCount);
        if (matchCount == 0) {
            return indices;
        }

        // Pass 2: Extract matching row indices via TZCNT bit-clearing loop
        MemorySegment outData = indices.buffer(1);
        long outByteOffset = 0;
        for (long bitCursor = 0; bitCursor < length; bitCursor += 64L) {
            int bits = (int) Math.min(64L, length - bitCursor);
            long dataWord = extractWord(dataSeg, offset + bitCursor, bits);
            long activeWord = hasNulls ? (dataWord & extractWord(valSeg, offset + bitCursor, bits)) : dataWord;

            while (activeWord != 0L) {
                int bitIdx = Long.numberOfTrailingZeros(activeWord);
                outData.set(ValueLayout.JAVA_INT, outByteOffset, (int) (bitCursor + bitIdx));
                outByteOffset += Integer.BYTES;
                activeWord &= (activeWord - 1L);
            }
        }
        return indices;
    }

    private enum BinaryBitOp { AND, OR, XOR }

    private static BooleanBuffer applyBinaryOp(BooleanBuffer left, BooleanBuffer right,
                                               String resultName, Arena targetArena, BinaryBitOp op) {
        Objects.requireNonNull(left, "left cannot be null");
        Objects.requireNonNull(right, "right cannot be null");
        Objects.requireNonNull(resultName, "resultName cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        if (left.length() != right.length()) {
            throw new IllegalArgumentException(
                    "BooleanBuffer lengths must match: " + left.length() + " vs " + right.length()
            );
        }

        long length = left.length();
        long bitmapBytes = (length + 7L) >>> 3;
        MemorySegment outValidity = intersectValidity(left, right, targetArena);
        MemorySegment outData = ArrowMemoryAllocator.allocateAligned(targetArena, bitmapBytes);

        MemorySegment leftData = left.buffer(1);
        MemorySegment rightData = right.buffer(1);
        long leftOffset = left.offset();
        long rightOffset = right.offset();

        long bitCursor = 0;
        long outByteCursor = 0;
        while (bitCursor < length) {
            int bits = (int) Math.min(64L, length - bitCursor);
            long mask = bits == 64 ? -1L : (1L << bits) - 1L;
            long wL = extractWord(leftData, leftOffset + bitCursor, bits);
            long wR = extractWord(rightData, rightOffset + bitCursor, bits);
            long result = switch (op) {
                case AND -> (wL & wR) & mask;
                case OR -> (wL | wR) & mask;
                case XOR -> (wL ^ wR) & mask;
            };
            outData.set(ValueLayout.JAVA_LONG_UNALIGNED, outByteCursor, result);
            bitCursor += 64L;
            outByteCursor += Long.BYTES;
        }

        return BooleanBuffer.wrap(resultName, targetArena, length, 0, outValidity, outData);
    }
}