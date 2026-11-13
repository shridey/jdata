package io.github.shridey.jdata.core;

import io.github.shridey.jdata.core.buffer.AbstractBufferBuilder;
import io.github.shridey.jdata.core.buffer.AbstractDataBuffer;
import io.github.shridey.jdata.core.buffer.ChunkedBuffer;
import io.github.shridey.jdata.core.buffer.Int32Buffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public sealed interface DataBuffer permits AbstractDataBuffer, ChunkedBuffer {

    String name();
    DataType type();
    long length();
    long offset();
    Arena arena();

    long nullCount();
    boolean isNull(long index);
    void setNull(long index);
    void setAllValid();

    int bufferCount();
    MemorySegment buffer(int index);

    DataBuffer slice(long offset, long length);

    /**
     * Returns an O(1) zero-copy view of this buffer with a new column name.
     */
    DataBuffer withName(String newName);

    /**
     * Materializes a new buffer in {@code targetArena} by gathering rows at the given 32-bit {@code indices}.
     */
    DataBuffer take(Int32Buffer indices, Arena targetArena);

    AbstractBufferBuilder toBuilder(Arena targetArena);
}