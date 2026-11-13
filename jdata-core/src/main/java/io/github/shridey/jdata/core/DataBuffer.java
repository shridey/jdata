package io.github.shridey.jdata.core;

import io.github.shridey.jdata.core.buffer.AbstractBufferBuilder;
import io.github.shridey.jdata.core.buffer.AbstractDataBuffer;
import io.github.shridey.jdata.core.buffer.ChunkedBuffer;
import io.github.shridey.jdata.core.buffer.Int32Buffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * The root sealed interface representing a typed columnar vector in {@code jdata-core}.
 *
 * <p>Permits either contiguous physical Apache Arrow arrays ({@link AbstractDataBuffer})
 * or logical composite multi-chunk columns ({@link ChunkedBuffer}). All off-heap memory
 * lifecycles are governed by the bound {@link #arena()}.
 */
public sealed interface DataBuffer permits AbstractDataBuffer, ChunkedBuffer {

    /**
     * Returns the column name of this buffer.
     *
     * @return the non-null column name
     */
    String name();

    /**
     * Returns the logical {@link DataType} of this column.
     *
     * @return the column {@link DataType}
     */
    DataType type();

    /**
     * Returns the number of logical elements (rows) in this buffer.
     *
     * @return the logical element count
     */
    long length();

    /**
     * Returns the starting element offset within the underlying physical storage buffers
     * (non-zero for zero-copy slices of physical buffers, or {@code 0} for root and chunked buffers).
     *
     * @return the physical element offset
     */
    long offset();

    /**
     * Returns the {@link Arena} that governs the off-heap memory lifecycle of this buffer.
     *
     * @return the bound {@link Arena}
     */
    Arena arena();

    /**
     * Returns the number of null elements in this buffer's logical range.
     *
     * @return the count of null elements
     */
    long nullCount();

    /**
     * Checks whether the element at the given logical index is null.
     *
     * @param index the zero-based logical row index
     * @return {@code true} if the element at {@code index} is null, {@code false} otherwise
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    boolean isNull(long index);

    /**
     * Marks the element at the given logical index as null in the validity bitmap.
     *
     * @param index the zero-based logical row index
     * @throws IndexOutOfBoundsException     if {@code index < 0 || index >= length()}
     * @throws UnsupportedOperationException if this buffer was imported without a validity bitmap
     */
    void setNull(long index);

    /**
     * Marks all elements within this buffer's logical range as valid (non-null)
     * and resets {@link #nullCount()} to {@code 0}.
     */
    void setAllValid();

    /**
     * Returns the number of physical off-heap {@link MemorySegment} buffers backing this column
     * (e.g., {@code 2} for fixed-width primitives,booleans, and dictionaries; {@code 3} for UTF-8 strings).
     *
     * @return the physical buffer count
     * @throws UnsupportedOperationException if this is a multi-chunk {@link ChunkedBuffer}
     */
    int bufferCount();

    /**
     * Returns the physical {@link MemorySegment} at the specified Arrow buffer slot
     * ({@code 0} for validity bitmap, {@code 1} for values/offsets, {@code 2} for variable-width data).
     *
     * @param index the physical buffer slot index
     * @return the backing {@link MemorySegment}
     * @throws IndexOutOfBoundsException     if {@code index < 0 || index >= bufferCount()}
     * @throws UnsupportedOperationException if this is a multi-chunk {@link ChunkedBuffer}
     */
    MemorySegment buffer(int index);

    /**
     * Creates an O(1) zero-copy view over a sub-range of this column.
     *
     * @param offset the starting logical index in this buffer
     * @param length the number of elements in the slice
     * @return a zero-copy sliced {@link DataBuffer} sharing the same underlying memory
     * @throws IndexOutOfBoundsException if the slice range is out of bounds
     */
    DataBuffer slice(long offset, long length);

    /**
     * Returns an O(1) zero-copy view of this buffer with a new column name.
     *
     * @param newName the new column name
     * @return a zero-copy {@link DataBuffer} view with {@code newName}
     */
    DataBuffer withName(String newName);

    /**
     * Materializes a new buffer in {@code targetArena} by gathering rows at the specified
     * 32-bit selection indices. Null entries in {@code indices} produce null entries in the output.
     *
     * @param indices     the 32-bit row indices to gather
     * @param targetArena the arena in which to allocate the gathered buffer
     * @return a newly materialized {@link DataBuffer} containing the selected rows
     */
    DataBuffer take(Int32Buffer indices, Arena targetArena);

    /**
     * Copies this buffer's logical elements into a mutable {@link AbstractBufferBuilder}
     * bound to {@code targetArena}.
     *
     * @param targetArena the arena in which the builder will allocate memory
     * @return a populated {@link AbstractBufferBuilder} containing a copy of this buffer's data
     */
    AbstractBufferBuilder toBuilder(Arena targetArena);
}