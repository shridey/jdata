package io.github.shridey.jdata.core;

import io.github.shridey.jdata.core.buffer.AbstractBufferBuilder;
import io.github.shridey.jdata.core.buffer.AbstractDataBuffer;
import io.github.shridey.jdata.core.buffer.ChunkedBuffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * The blueprint for every column in the engine.
 * We use a "sealed interface" to lock down the architecture. By only allowing
 * AbstractDataBuffer to implement this, we guarantee that no one can create a rogue
 * column that breaks the strict off-heap memory rules required by Apache Arrow.
 */
public sealed interface DataBuffer permits AbstractDataBuffer, ChunkedBuffer {

    /**
     * The logical name of the column (e.g., "user_id", "revenue").
     */
    String name();

    /**
     * The physical data type (e.g., INT64, UTF8).
     * This tells the engine exactly how many bytes each row consumes.
     */
    DataType type();

    /**
     * The total number of rows in this column, including nulls.
     */
    long length();

    /**
     * The exact number of null values.
     * SIMD (Hardware) processors hate "if-statements". By knowing the exact null count upfront,
     * the vectorized engine can completely skip null-checking if nullCount() == 0, massively boosting speed.
     */
    long nullCount();

    /**
     * The "Arena" is Java's off-heap memory manager.
     * When the Arena is closed, all physical memory tied to this column is instantly destroyed
     * without waiting for the Java Garbage Collector.
     */
    Arena arena();

    /**
     * Checks the hidden 1-bit validity bitmap to see if a row is null.
     */
    boolean isNull(long index);

    /**
     * Forces a row to be null by flipping its validity bit to 0.
     */
    void setNull(long index);

    /**
     * Resets the entire column to contain zero nulls.
     */
    void setAllValid();

    /**
     * Primitive columns (Int, Float) require 2 buffers: [Validity Bitmap, Physical Data].
     * String columns (UTF8) require 3 buffers: [Validity Bitmap, String Offsets, Raw Text Data].
     */
    int bufferCount();

    /**
     * Exposes the raw C-style memory pointers.
     * Index 0 is ALWAYS the validity bitmap.
     * This method is what allows us to share this column with Rust, C++, or Python
     * instantly without copying any data.
     */
    MemorySegment buffer(int index) throws IndexOutOfBoundsException;

    /**
     * The logical starting row of this buffer.
     * Greater than 0 if this buffer is a zero-copy slice of a larger column.
     */
    long offset();

    /**
     * Converts this sealed buffer into a mutable builder by allocating new memory
     * and performing a bulk native copy of the existing data.
     *
     * @param arena The memory arena to bind the new builder to.
     * @return A new builder pre-populated with this buffer's data.
     */
    AbstractBufferBuilder toBuilder(Arena arena);

    /**
     * Creates a zero-copy view of this buffer sharing the exact same physical memory.
     *
     * @param offset The logical start index relative to the current buffer's start.
     * @param length The total number of rows included in the slice.
     * @return A new DataBuffer scoped exactly to the requested window.
     */
    DataBuffer slice(long offset, long length);
}