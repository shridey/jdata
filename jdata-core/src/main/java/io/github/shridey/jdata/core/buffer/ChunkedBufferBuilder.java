package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.memory.ArrowMemoryPool;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A streaming builder that constructs a {@link ChunkedBuffer} using fixed-capacity chunks.
 * Eliminates power-of-two resize copying, guarantees that individual chunks fit within
 * {@link ArrowMemoryPool} slabs, and automatically rolls over
 * {@link DataType#UTF8} chunks before reaching the 2 GB (32-bit offset) limit.
 */
public final class ChunkedBufferBuilder {

    /** Default maximum rows per physical chunk (65,536 rows {@code ->} 512 KB for 64-bit types). */
    public static final long DEFAULT_MAX_CHUNK_ROWS = 65_536L;

    /** Default maximum UTF-8 bytes per physical chunk ({@link Integer#MAX_VALUE} = 2 GB). */
    public static final int DEFAULT_MAX_UTF8_CHUNK_BYTES = Integer.MAX_VALUE;

    private final Arena arena;
    private final String name;
    private final DataType type;
    private final long maxChunkRows;
    private final int maxUtf8ChunkBytes;

    private final List<DataBuffer> completedChunks = new ArrayList<>();
    private AbstractBufferBuilder activeBuilder;
    private long activeRows = 0L;
    private long totalLength = 0L;

    /**
     * Creates a streaming {@link ChunkedBufferBuilder} with default chunk limits
     * ({@link #DEFAULT_MAX_CHUNK_ROWS} rows and {@link #DEFAULT_MAX_UTF8_CHUNK_BYTES} bytes).
     *
     * @param arena the governing memory arena
     * @param name  the column name
     * @param type  the logical {@link DataType} of the column
     */
    public ChunkedBufferBuilder(Arena arena, String name, DataType type) {
        this(arena, name, type, DEFAULT_MAX_CHUNK_ROWS, DEFAULT_MAX_UTF8_CHUNK_BYTES);
    }

    /**
     * Creates a streaming {@link ChunkedBufferBuilder} with a custom maximum row count per chunk.
     *
     * @param arena        the governing memory arena
     * @param name         the column name
     * @param type         the logical {@link DataType} of the column
     * @param maxChunkRows maximum rows per physical chunk (must be {@code > 0})
     */
    public ChunkedBufferBuilder(Arena arena, String name, DataType type, long maxChunkRows) {
        this(arena, name, type, maxChunkRows, DEFAULT_MAX_UTF8_CHUNK_BYTES);
    }

    /**
     * Creates a streaming {@link ChunkedBufferBuilder} with custom row and UTF-8 byte limits per chunk.
     *
     * @param arena             the governing memory arena
     * @param name              the column name
     * @param type              the logical {@link DataType} of the column
     * @param maxChunkRows      maximum rows per physical chunk (must be {@code > 0})
     * @param maxUtf8ChunkBytes maximum UTF-8 payload bytes per chunk (must be {@code > 0})
     */
    public ChunkedBufferBuilder(Arena arena, String name, DataType type, long maxChunkRows, int maxUtf8ChunkBytes) {
        this.arena = Objects.requireNonNull(arena, "Arena cannot be null");
        this.name = Objects.requireNonNull(name, "Column name cannot be null");
        this.type = Objects.requireNonNull(type, "DataType cannot be null");
        if (maxChunkRows <= 0) {
            throw new IllegalArgumentException("maxChunkRows must be positive: " + maxChunkRows);
        }
        if (maxUtf8ChunkBytes <= 0) {
            throw new IllegalArgumentException("maxUtf8ChunkBytes must be positive: " + maxUtf8ChunkBytes);
        }
        this.maxChunkRows = maxChunkRows;
        this.maxUtf8ChunkBytes = maxUtf8ChunkBytes;
        this.activeBuilder = createChunkBuilder();
    }

    private AbstractBufferBuilder createChunkBuilder() {
        long initialRows = Math.min(maxChunkRows, 4096L);
        return switch (this.type) {
            case INT32 -> new Int32BufferBuilder(arena, name, initialRows);
            case INT64 -> new Int64BufferBuilder(arena, name, initialRows);
            case FLOAT32 -> new Float32BufferBuilder(arena, name, initialRows);
            case FLOAT64 -> new Float64BufferBuilder(arena, name, initialRows);
            case BOOLEAN -> new BooleanBufferBuilder(arena, name, initialRows);
            case UTF8 -> new UTF8BufferBuilder(
                    arena, name, initialRows, Math.min(maxUtf8ChunkBytes, initialRows * 16L)
            );
            case DICTIONARY -> new DictionaryBufferBuilder(arena, name, initialRows);
            default -> throw new UnsupportedOperationException("ChunkedBufferBuilder does not support type: " + this.type);
        };
    }

    private void rotateIfRowLimitReached() {
        if (activeRows >= maxChunkRows) {
            flushChunk();
        }
    }

    /**
     * Seals the current partial chunk (if non-empty) and starts a fresh chunk builder.
     *
     * @return this builder
     */
    public ChunkedBufferBuilder flushChunk() {
        if (activeRows > 0) {
            completedChunks.add(activeBuilder.build());
            activeBuilder = createChunkBuilder();
            activeRows = 0L;
        }
        return this;
    }

    /**
     * Appends a single null entry.
     *
     * @return this builder
     */
    public ChunkedBufferBuilder appendNull() {
        rotateIfRowLimitReached();
        switch (activeBuilder) {
            case Int32BufferBuilder b -> b.appendNull();
            case Int64BufferBuilder b -> b.appendNull();
            case Float32BufferBuilder b -> b.appendNull();
            case Float64BufferBuilder b -> b.appendNull();
            case BooleanBufferBuilder b -> b.appendNull();
            case UTF8BufferBuilder b -> b.appendNull();
            case DictionaryBufferBuilder b -> b.appendNull();
        }
        activeRows++;
        totalLength++;
        return this;
    }

    /**
     * Appends a 32-bit integer value.
     *
     * @param value the {@code int} value to append
     * @return this builder
     */
    public ChunkedBufferBuilder append(int value) {
        if (type != DataType.INT32) throw new IllegalStateException("Builder is not INT32, is " + type);
        rotateIfRowLimitReached();
        ((Int32BufferBuilder) activeBuilder).append(value);
        activeRows++;
        totalLength++;
        return this;
    }

    /**
     * Bulk-appends an array of 32-bit integers, automatically splitting across chunk boundaries.
     *
     * @param values the array of {@code int} values to append
     * @return this builder
     */
    public ChunkedBufferBuilder appendAll(int[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (type != DataType.INT32) throw new IllegalStateException("Builder is not INT32, is " + type);
        int offset = 0;
        while (offset < values.length) {
            rotateIfRowLimitReached();
            int batch = (int) Math.min(values.length - offset, maxChunkRows - activeRows);
            Int32BufferBuilder b = (Int32BufferBuilder) activeBuilder;
            if (offset == 0 && batch == values.length) {
                b.appendAll(values);
            } else {
                for (int i = 0; i < batch; i++) {
                    b.append(values[offset + i]);
                }
            }
            offset += batch;
            activeRows += batch;
            totalLength += batch;
        }
        return this;
    }

    /**
     * Appends a 64-bit integer value.
     *
     * @param value the {@code long} value to append
     * @return this builder
     */
    public ChunkedBufferBuilder append(long value) {
        if (type != DataType.INT64) throw new IllegalStateException("Builder is not INT64, is " + type);
        rotateIfRowLimitReached();
        ((Int64BufferBuilder) activeBuilder).append(value);
        activeRows++;
        totalLength++;
        return this;
    }

    /**
     * Bulk-appends an array of 64-bit integers, automatically splitting across chunk boundaries.
     *
     * @param values the array of {@code long} values to append
     * @return this builder
     */
    public ChunkedBufferBuilder appendAll(long[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (type != DataType.INT64) throw new IllegalStateException("Builder is not INT64, is " + type);
        int offset = 0;
        while (offset < values.length) {
            rotateIfRowLimitReached();
            int batch = (int) Math.min(values.length - offset, maxChunkRows - activeRows);
            Int64BufferBuilder b = (Int64BufferBuilder) activeBuilder;
            if (offset == 0 && batch == values.length) {
                b.appendAll(values);
            } else {
                for (int i = 0; i < batch; i++) {
                    b.append(values[offset + i]);
                }
            }
            offset += batch;
            activeRows += batch;
            totalLength += batch;
        }
        return this;
    }

    /**
     * Appends a 32-bit float value.
     *
     * @param value the {@code float} value to append
     * @return this builder
     */
    public ChunkedBufferBuilder append(float value) {
        if (type != DataType.FLOAT32) throw new IllegalStateException("Builder is not FLOAT32, is " + type);
        rotateIfRowLimitReached();
        ((Float32BufferBuilder) activeBuilder).append(value);
        activeRows++;
        totalLength++;
        return this;
    }

    /**
     * Bulk-appends an array of 32-bit floats, automatically splitting across chunk boundaries.
     *
     * @param values the array of {@code float} values to append
     * @return this builder
     */
    public ChunkedBufferBuilder appendAll(float[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (type != DataType.FLOAT32) throw new IllegalStateException("Builder is not FLOAT32, is " + type);
        int offset = 0;
        while (offset < values.length) {
            rotateIfRowLimitReached();
            int batch = (int) Math.min(values.length - offset, maxChunkRows - activeRows);
            Float32BufferBuilder b = (Float32BufferBuilder) activeBuilder;
            if (offset == 0 && batch == values.length) {
                b.appendAll(values);
            } else {
                for (int i = 0; i < batch; i++) {
                    b.append(values[offset + i]);
                }
            }
            offset += batch;
            activeRows += batch;
            totalLength += batch;
        }
        return this;
    }

    /**
     * Appends a 64-bit double value.
     *
     * @param value the {@code double} value to append
     * @return this builder
     */
    public ChunkedBufferBuilder append(double value) {
        if (type != DataType.FLOAT64) throw new IllegalStateException("Builder is not FLOAT64, is " + type);
        rotateIfRowLimitReached();
        ((Float64BufferBuilder) activeBuilder).append(value);
        activeRows++;
        totalLength++;
        return this;
    }

    /**
     * Bulk-appends an array of 64-bit doubles, automatically splitting across chunk boundaries.
     *
     * @param values the array of {@code double} values to append
     * @return this builder
     */
    public ChunkedBufferBuilder appendAll(double[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (type != DataType.FLOAT64) throw new IllegalStateException("Builder is not FLOAT64, is " + type);
        int offset = 0;
        while (offset < values.length) {
            rotateIfRowLimitReached();
            int batch = (int) Math.min(values.length - offset, maxChunkRows - activeRows);
            Float64BufferBuilder b = (Float64BufferBuilder) activeBuilder;
            if (offset == 0 && batch == values.length) {
                b.appendAll(values);
            } else {
                for (int i = 0; i < batch; i++) {
                    b.append(values[offset + i]);
                }
            }
            offset += batch;
            activeRows += batch;
            totalLength += batch;
        }
        return this;
    }

    /**
     * Appends a boolean value.
     *
     * @param value the {@code boolean} value to append
     * @return this builder
     */
    public ChunkedBufferBuilder append(boolean value) {
        if (type != DataType.BOOLEAN) throw new IllegalStateException("Builder is not BOOLEAN, is " + type);
        rotateIfRowLimitReached();
        ((BooleanBufferBuilder) activeBuilder).append(value);
        activeRows++;
        totalLength++;
        return this;
    }

    /**
     * Bulk-appends an array of booleans, automatically splitting across chunk boundaries.
     *
     * @param values the array of {@code boolean} values to append
     * @return this builder
     */
    public ChunkedBufferBuilder appendAll(boolean[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (type != DataType.BOOLEAN) throw new IllegalStateException("Builder is not BOOLEAN, is " + type);
        int offset = 0;
        while (offset < values.length) {
            rotateIfRowLimitReached();
            int batch = (int) Math.min(values.length - offset, maxChunkRows - activeRows);
            BooleanBufferBuilder b = (BooleanBufferBuilder) activeBuilder;
            if (offset == 0 && batch == values.length) {
                b.appendAll(values);
            } else {
                for (int i = 0; i < batch; i++) {
                    b.append(values[offset + i]);
                }
            }
            offset += batch;
            activeRows += batch;
            totalLength += batch;
        }
        return this;
    }

    /**
     * Appends a UTF-8 or Dictionary string value, rolling over to a new chunk if either
     * {@link #maxChunkRows()} or {@link #maxUtf8ChunkBytes()} would be exceeded.
     *
     * @param value the string value to append (or {@code null})
     * @return this builder
     */
    public ChunkedBufferBuilder append(String value) {
        if (value == null) return appendNull();
        if (type == DataType.DICTIONARY) {
            rotateIfRowLimitReached();
            ((DictionaryBufferBuilder) activeBuilder).append(value);
            activeRows++;
            totalLength++;
            return this;
        }
        if (type != DataType.UTF8) {
            throw new IllegalStateException("Builder is not UTF8 or DICTIONARY, is " + type);
        }

        int utf8Bytes = UTF8Buffer.utf8ByteLength(value);
        if (utf8Bytes > maxUtf8ChunkBytes) {
            throw new IllegalArgumentException(
                    "Single string byte length (" + utf8Bytes + ") exceeds maxUtf8ChunkBytes (" + maxUtf8ChunkBytes + ")"
            );
        }

        rotateIfRowLimitReached();
        UTF8BufferBuilder utf8Builder = (UTF8BufferBuilder) activeBuilder;
        if (activeRows > 0 && (long) utf8Builder.currentDataBytes() + utf8Bytes > maxUtf8ChunkBytes) {
            flushChunk();
            utf8Builder = (UTF8BufferBuilder) activeBuilder;
        }

        utf8Builder.append(value);
        activeRows++;
        totalLength++;
        return this;
    }

    /**
     * Bulk-appends an array of strings, automatically splitting across row and byte chunk boundaries.
     *
     * @param values the array of strings to append
     * @return this builder
     */
    public ChunkedBufferBuilder appendAll(String[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        for (String val : values) {
            append(val);
        }
        return this;
    }

    /**
     * Appends a range of rows from an existing {@link DataBuffer}, splitting across chunk boundaries as needed.
     *
     * @param source   the source buffer
     * @param srcIndex the starting logical index in {@code source}
     * @param length   the number of elements to append
     * @return this builder
     */
    public ChunkedBufferBuilder appendBlock(DataBuffer source, long srcIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        Objects.checkFromIndexSize(srcIndex, length, source.length());
        if (length == 0) return this;

        if (source instanceof ChunkedBuffer cb) {
            for (DataBuffer chunk : cb.slice(srcIndex, length).chunks()) {
                appendBlock(chunk, 0, chunk.length());
            }
            return this;
        }

        long cursor = srcIndex;
        long remaining = length;

        while (remaining > 0) {
            rotateIfRowLimitReached();
            long batch = Math.min(remaining, maxChunkRows - activeRows);

            if (type == DataType.UTF8 && source.type() == DataType.UTF8) {
                UTF8BufferBuilder utf8Builder = (UTF8BufferBuilder) activeBuilder;
                MemorySegment srcOffsets = source.buffer(1);
                long srcPhys = cursor + source.offset();
                long intBytes = ValueLayout.JAVA_INT.byteSize();
                int startByte = srcOffsets.get(ValueLayout.JAVA_INT, srcPhys * intBytes);
                int endByte = srcOffsets.get(ValueLayout.JAVA_INT, (srcPhys + batch) * intBytes);

                if ((long) utf8Builder.currentDataBytes() + (endByte - startByte) > maxUtf8ChunkBytes) {
                    if (activeRows > 0) {
                        flushChunk();
                        utf8Builder = (UTF8BufferBuilder) activeBuilder;
                    }
                    // Narrow batch if the sub-slice alone exceeds maxUtf8ChunkBytes
                    long narrowed = 0;
                    while (narrowed < batch) {
                        int s = srcOffsets.get(ValueLayout.JAVA_INT, (srcPhys + narrowed) * intBytes);
                        int e = srcOffsets.get(ValueLayout.JAVA_INT, (srcPhys + narrowed + 1L) * intBytes);
                        if ((long) utf8Builder.currentDataBytes() + (e - startByte) > maxUtf8ChunkBytes) {
                            if (narrowed == 0) {
                                throw new IllegalArgumentException(
                                        "Single string byte length (" + (e - s) + ") exceeds maxUtf8ChunkBytes"
                                );
                            }
                            break;
                        }
                        narrowed++;
                    }
                    batch = narrowed;
                }
                utf8Builder.appendImportedBlock(source, cursor, batch);
            } else {
                switch (activeBuilder) {
                    case Int32BufferBuilder b -> b.appendImportedBlock(source, cursor, batch);
                    case Int64BufferBuilder b -> b.appendImportedBlock(source, cursor, batch);
                    case Float32BufferBuilder b -> b.appendImportedBlock(source, cursor, batch);
                    case Float64BufferBuilder b -> b.appendImportedBlock(source, cursor, batch);
                    case BooleanBufferBuilder b -> b.appendImportedBlock(source, cursor, batch);
                    case DictionaryBufferBuilder b -> b.appendImportedBlock(source, cursor, batch);
                    default -> throw new IllegalArgumentException("Incompatible block append for type: " + type);
                }
            }

            cursor += batch;
            remaining -= batch;
            activeRows += batch;
            totalLength += batch;
        }
        return this;
    }

    /**
     * Returns the column name of the buffer being constructed.
     *
     * @return the column name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the logical {@link DataType} of the buffer being constructed.
     *
     * @return the column {@link DataType}
     */
    public DataType type() {
        return type;
    }

    /**
     * Returns the total number of rows appended across all chunks so far.
     *
     * @return the total logical length
     */
    public long length() {
        return totalLength;
    }

    /**
     * Returns the maximum number of rows permitted per physical chunk.
     *
     * @return the maximum rows per chunk
     */
    public long maxChunkRows() {
        return maxChunkRows;
    }

    /**
     * Returns the maximum number of UTF-8 payload bytes permitted per physical chunk.
     *
     * @return the maximum UTF-8 bytes per chunk
     */
    public int maxUtf8ChunkBytes() {
        return maxUtf8ChunkBytes;
    }

    /**
     * Flushes any active partial chunk and seals all chunks into an immutable {@link ChunkedBuffer}.
     *
     * @return the constructed {@link ChunkedBuffer}
     */
    public ChunkedBuffer build() {
        flushChunk();
        return new ChunkedBuffer(arena, name, type, completedChunks);
    }
}