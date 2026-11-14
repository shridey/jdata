package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * A sealed, read-only Apache Arrow Dictionary-Encoded column ({@code INT32 indices -> UTF8 dictionary}).
 * Compresses repeated categorical strings into 32-bit integer codes for O(1) equality comparisons,
 * compact memory footprint, and SIMD-accelerated group-by and filter execution.
 */
public final class DictionaryBuffer extends AbstractDataBuffer {

    private final MemorySegment indicesBuffer;
    private final UTF8Buffer dictionary;

    DictionaryBuffer(String name, Arena arena, long length, long offset,
                     MemorySegment validityBitmap, MemorySegment indicesBuffer, UTF8Buffer dictionary) {
        this(name, arena, length, offset, validityBitmap, indicesBuffer, dictionary, -1L);
    }

    DictionaryBuffer(String name, Arena arena, long length, long offset,
                     MemorySegment validityBitmap, MemorySegment indicesBuffer, UTF8Buffer dictionary, long knownNullCount) {
        super(name, arena, length, offset, validityBitmap, knownNullCount);
        this.indicesBuffer = Objects.requireNonNull(indicesBuffer, "indicesBuffer cannot be null");
        this.dictionary = Objects.requireNonNull(dictionary, "dictionary cannot be null");
    }

    /**
     * Encodes a Java {@code String[]} into a compact {@link DictionaryBuffer}.
     *
     * @param arena  the arena governing off-heap allocations
     * @param name   the column name
     * @param values the array of strings to dictionary-encode
     * @return a newly allocated {@link DictionaryBuffer}
     */
    public static DictionaryBuffer allocate(Arena arena, String name, String[] values) {
        Objects.requireNonNull(arena, "Arena cannot be null");
        Objects.requireNonNull(name, "Column name cannot be null");
        Objects.requireNonNull(values, "Values array cannot be null");

        return new DictionaryBufferBuilder(arena, name, values.length)
                .appendAll(values)
                .build();
    }

    /**
     * Encodes an existing off-heap {@link UTF8Buffer} into a {@link DictionaryBuffer}
     * completely off-heap without allocating intermediate Java Strings.
     *
     * @param arena  the arena governing off-heap allocations
     * @param source the source {@link UTF8Buffer} to encode
     * @return a newly encoded {@link DictionaryBuffer}
     */
    public static DictionaryBuffer fromUTF8(Arena arena, UTF8Buffer source) {
        Objects.requireNonNull(arena, "Arena cannot be null");
        Objects.requireNonNull(source, "Source UTF8Buffer cannot be null");

        return new DictionaryBufferBuilder(arena, source.name(), source.length())
                .appendUTF8Block(source, 0, source.length())
                .build();
    }

    /**
     * Wraps an existing {@link Int32Buffer} of dictionary codes and a {@link UTF8Buffer} dictionary
     * in O(1) with zero memory copying, validating that all non-null codes lie within dictionary bounds.
     *
     * @param name       the column name
     * @param indices    the 32-bit integer dictionary codes
     * @param dictionary the deduplicated {@link UTF8Buffer} dictionary
     * @return a wrapped {@link DictionaryBuffer}
     * @throws IndexOutOfBoundsException if any non-null code in {@code indices} is out of bounds for {@code dictionary}
     */
    public static DictionaryBuffer ofIndices(String name, Int32Buffer indices, UTF8Buffer dictionary) {
        Objects.requireNonNull(name, "Column name cannot be null");
        Objects.requireNonNull(indices, "Indices buffer cannot be null");
        Objects.requireNonNull(dictionary, "Dictionary buffer cannot be null");

        long dictLen = dictionary.length();
        long rowCount = indices.length();
        boolean hasNulls = indices.nullCount() > 0;

        for (long i = 0; i < rowCount; i++) {
            if (!hasNulls || !indices.isNull(i)) {
                int code = indices.get(i);
                if (code < 0 || code >= dictLen) {
                    throw new IndexOutOfBoundsException(
                            "Dictionary index " + code + " at row " + i + " is out of bounds for dictionary of size " + dictLen
                    );
                }
            }
        }

        return new DictionaryBuffer(
                name, indices.arena(), rowCount, indices.offset(),
                indices.buffer(0), indices.buffer(1), dictionary
        );
    }

    /**
     * Zero-copy wraps pre-allocated or memory-mapped validity and 32-bit index segments with a {@link UTF8Buffer} dictionary.
     *
     * @param name           the column name
     * @param arena          the governing arena
     * @param length         the logical element count
     * @param offset         the starting physical element offset
     * @param validityBitmap the validity bitmap segment (or {@link MemorySegment#NULL})
     * @param indicesBuffer  the 32-bit integer codes segment
     * @param dictionary     the deduplicated {@link UTF8Buffer} dictionary
     * @return a wrapped {@link DictionaryBuffer}
     */
    public static DictionaryBuffer wrap(String name, Arena arena, long length, long offset,
                                        MemorySegment validityBitmap, MemorySegment indicesBuffer, UTF8Buffer dictionary) {
        Objects.requireNonNull(dictionary, "dictionary cannot be null");
        long totalElements = Math.addExact(length, offset);
        long minIndicesBytes = Math.multiplyExact(totalElements, ValueLayout.JAVA_INT.byteSize());
        ArrowMemoryAllocator.validateWrapBounds(name, arena, length, offset, validityBitmap, indicesBuffer, minIndicesBytes);
        return new DictionaryBuffer(name, arena, length, offset, validityBitmap, indicesBuffer, dictionary);
    }

    /**
     * Returns the 32-bit dictionary index (code) at the given logical row index.
     *
     * @param index the zero-based logical row index
     * @return the 32-bit dictionary code at {@code index}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public int getIndex(long index) {
        checkBounds(index);
        long physicalIndex = index + this.offset;
        return indicesBuffer.get(ValueLayout.JAVA_INT, physicalIndex * ValueLayout.JAVA_INT.byteSize());
    }

    /**
     * Resolves and returns the decoded UTF-8 {@link String} at the given logical row index, or {@code null} if null.
     *
     * @param index the zero-based logical row index
     * @return the decoded {@link String}, or {@code null} if the slot is null
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public String getString(long index) {
        checkBounds(index);
        if (isNull(index)) {
            return null;
        }
        int code = getIndex(index);
        return dictionary.getString(code);
    }

    /**
     * Returns the underlying deduplicated {@link UTF8Buffer} dictionary.
     *
     * @return the dictionary {@link UTF8Buffer}
     */
    public UTF8Buffer dictionary() {
        return dictionary;
    }

    /**
     * Returns the number of unique entries in the dictionary.
     *
     * @return the dictionary cardinality
     */
    public long cardinality() {
        return dictionary.length();
    }

    /**
     * Returns a zero-copy {@link Int32Buffer} view over this column's dictionary codes and validity bitmap.
     * Enables vectorized SIMD kernels to execute directly on the 32-bit integer codes.
     *
     * @return a zero-copy {@link Int32Buffer} view of the dictionary codes
     */
    public Int32Buffer indices() {
        return new Int32Buffer(this.name, this.arena, this.length, this.offset, this.validityBitmap, this.indicesBuffer, this.nullCount);
    }

    /**
     * Decodes this {@link DictionaryBuffer} into a flat off-heap {@link UTF8Buffer} without allocating
     * intermediate Java Strings on the heap.
     *
     * @param targetArena the arena in which to allocate the decoded {@link UTF8Buffer}
     * @return a flat decoded {@link UTF8Buffer}
     */
    public UTF8Buffer decode(Arena targetArena) {
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        MemorySegment dictOffsets = dictionary.buffer(1);
        MemorySegment dictData = dictionary.buffer(2);
        long dictPhysicalOffset = dictionary.offset();
        long intBytes = ValueLayout.JAVA_INT.byteSize();
        boolean hasNulls = this.nullCount() > 0;

        // Pass 1: Compute exact total UTF-8 byte size directly from dictionary offsets
        long totalDataBytes = 0;
        for (long i = 0; i < this.length; i++) {
            if (!hasNulls || !isNull(i)) {
                long dictIdx = dictPhysicalOffset + getIndex(i);
                int start = dictOffsets.get(ValueLayout.JAVA_INT, dictIdx * intBytes);
                int end = dictOffsets.get(ValueLayout.JAVA_INT, (dictIdx + 1L) * intBytes);
                totalDataBytes += (end - start);
            }
        }

        if (totalDataBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException("Decoded UTF8 buffer exceeds Integer.MAX_VALUE bytes.");
        }

        long bitmapBytes = (this.length + 7L) >>> 3;
        long offsetsBytes = Math.multiplyExact(this.length + 1L, intBytes);

        MemorySegment outValidity = ArrowMemoryAllocator.allocateAligned(targetArena, bitmapBytes);
        MemorySegment outOffsets = ArrowMemoryAllocator.allocateAligned(targetArena, offsetsBytes);
        MemorySegment outData = ArrowMemoryAllocator.allocateAligned(targetArena, totalDataBytes);

        UTF8Buffer decoded = new UTF8Buffer(this.name, targetArena, this.length, 0, outValidity, outOffsets, outData);
        decoded.setAllValid();

        // Pass 2: Copy UTF-8 byte slices directly from dictionary off-heap memory
        int currentByteOffset = 0;
        for (long i = 0; i < this.length; i++) {
            outOffsets.set(ValueLayout.JAVA_INT, i * intBytes, currentByteOffset);
            if (hasNulls && isNull(i)) {
                decoded.setNull(i);
            } else {
                long dictIdx = dictPhysicalOffset + getIndex(i);
                int start = dictOffsets.get(ValueLayout.JAVA_INT, dictIdx * intBytes);
                int end = dictOffsets.get(ValueLayout.JAVA_INT, (dictIdx + 1L) * intBytes);
                int len = end - start;
                if (len > 0) {
                    MemorySegment.copy(dictData, start, outData, currentByteOffset, len);
                    currentByteOffset += len;
                }
            }
        }
        outOffsets.set(ValueLayout.JAVA_INT, this.length * intBytes, currentByteOffset);
        return decoded;
    }

    /**
     * Materializes the column to a Java {@code String[]}, decoding each unique dictionary entry at most once.
     *
     * @return a Java {@code String[]} containing this column's decoded values
     */
    public String[] toArray() {
        if (this.length > Integer.MAX_VALUE) {
            throw new IllegalStateException("Buffer length is too large to fit in a Java array.");
        }
        String[] dictValues = dictionary.toArray();
        int len = (int) this.length;
        String[] result = new String[len];
        boolean hasNulls = this.nullCount() > 0;

        for (int i = 0; i < len; i++) {
            if (hasNulls && isNull(i)) {
                result[i] = null;
            } else {
                result[i] = dictValues[getIndex(i)];
            }
        }
        return result;
    }

    /**
     * Copies the 32-bit dictionary codes into a newly allocated Java {@code int[]}.
     *
     * @return a Java {@code int[]} of dictionary codes
     */
    public int[] toIndexArray() {
        return indices().toArray();
    }

    @Override
    public DictionaryBufferBuilder toBuilder(Arena targetArena) {
        return new DictionaryBufferBuilder(targetArena, this.name, this.length, (int) Math.max(dictionary.length(), 16L))
                .appendBlock(this, 0, this.length);
    }

    @Override
    public DictionaryBuffer slice(long sliceOffset, long sliceLength) {
        Objects.checkFromIndexSize(sliceOffset, sliceLength, this.length);
        long absoluteOffset = this.offset + sliceOffset;
        return new DictionaryBuffer(
                this.name, this.arena, sliceLength, absoluteOffset,
                this.validityBitmap, this.indicesBuffer, this.dictionary
        );
    }

    @Override
    public DictionaryBuffer withName(String newName) {
        Objects.requireNonNull(newName, "newName cannot be null");
        return new DictionaryBuffer(
                newName, this.arena, this.length, this.offset,
                this.validityBitmap, this.indicesBuffer, this.dictionary, this.nullCount
        );
    }

    @Override
    public DictionaryBuffer take(Int32Buffer selectionIndices, Arena targetArena) {
        Objects.requireNonNull(selectionIndices, "selectionIndices cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        Int32Buffer gatheredCodes = indices().take(selectionIndices, targetArena);
        UTF8Buffer targetDict = targetArena.equals(this.dictionary.arena())
                ? this.dictionary
                : this.dictionary.toBuilder(targetArena).build();

        return new DictionaryBuffer(
                this.name, targetArena, gatheredCodes.length(), 0,
                gatheredCodes.buffer(0), gatheredCodes.buffer(1), targetDict
        );
    }

    @Override
    public DataType type() {
        return DataType.DICTIONARY;
    }

    @Override
    public int bufferCount() {
        return 2;
    }

    @Override
    public MemorySegment buffer(int index) throws IndexOutOfBoundsException {
        return switch (index) {
            case 0 -> super.buffer(0);
            case 1 -> indicesBuffer;
            default -> throw new IndexOutOfBoundsException("DictionaryBuffer has 2 physical buffers (validity, indices).");
        };
    }
}