package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.ffi.ImportedDataBuffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A logical column composed of multiple discontiguous physical {@link DataBuffer} chunks.
 * Enables O(1) zero-copy batch appending, zero-copy Table concatenation, and multi-gigabyte
 * string columns that bypass single-buffer 2 GB offset limits.
 */
public final class ChunkedBuffer implements DataBuffer {

    private final String name;
    private final DataType type;
    private final Arena arena;
    private final List<DataBuffer> chunks;
    private final long[] chunkOffsets;
    private final long length;
    private long nullCount;

    public ChunkedBuffer(Arena arena, String name, DataType type, List<? extends DataBuffer> rawChunks) {
        this.arena = Objects.requireNonNull(arena, "Arena cannot be null");
        this.name = Objects.requireNonNull(name, "Column name cannot be null");
        this.type = Objects.requireNonNull(type, "DataType cannot be null");
        Objects.requireNonNull(rawChunks, "Chunks list cannot be null");

        List<DataBuffer> flattened = new ArrayList<>(rawChunks.size());
        for (DataBuffer chunk : rawChunks) {
            Objects.requireNonNull(chunk, "Chunk cannot be null");
            if (chunk.type() != this.type) {
                throw new IllegalArgumentException(
                        "Chunk type mismatch: expected " + this.type + ", got " + chunk.type()
                );
            }
            if (chunk instanceof ChunkedBuffer nested) {
                flattened.addAll(nested.chunks);
            } else if (chunk.length() > 0) {
                flattened.add(chunk);
            }
        }

        this.chunks = List.copyOf(flattened);
        this.chunkOffsets = new long[this.chunks.size() + 1];

        long runningOffset = 0L;
        long runningNulls = 0L;
        for (int i = 0; i < this.chunks.size(); i++) {
            this.chunkOffsets[i] = runningOffset;
            DataBuffer c = this.chunks.get(i);
            runningOffset = Math.addExact(runningOffset, c.length());
            runningNulls = Math.addExact(runningNulls, c.nullCount());
        }
        this.chunkOffsets[this.chunks.size()] = runningOffset;
        this.length = runningOffset;
        this.nullCount = runningNulls;
    }

    /**
     * Creates a {@link ChunkedBuffer} from one or more existing {@link DataBuffer} chunks,
     * inferring the column name, type, and arena from the first chunk.
     */
    public static ChunkedBuffer of(DataBuffer... chunks) {
        Objects.requireNonNull(chunks, "Chunks array cannot be null");
        if (chunks.length == 0) {
            throw new IllegalArgumentException("At least one chunk is required to infer column metadata.");
        }
        DataBuffer first = Objects.requireNonNull(chunks[0], "First chunk cannot be null");
        return new ChunkedBuffer(first.arena(), first.name(), first.type(), Arrays.asList(chunks));
    }

    /**
     * Returns a new {@link ChunkedBuffer} with {@code nextChunk} appended in O(C) metadata time (zero data copy).
     */
    public ChunkedBuffer appendChunk(DataBuffer nextChunk) {
        Objects.requireNonNull(nextChunk, "nextChunk cannot be null");
        if (nextChunk.type() != this.type) {
            throw new IllegalArgumentException(
                    "Cannot append chunk of type " + nextChunk.type() + " to ChunkedBuffer of type " + this.type
            );
        }
        if (nextChunk.length() == 0) {
            return this;
        }
        List<DataBuffer> combined = new ArrayList<>(this.chunks.size() + 1);
        combined.addAll(this.chunks);
        combined.add(nextChunk);
        return new ChunkedBuffer(this.arena, this.name, this.type, combined);
    }

    /**
     * Coalesces all chunks into a single contiguous {@link DataBuffer} allocated in {@code targetArena}.
     * If this buffer already consists of a single unsliced chunk in {@code targetArena}, returns it with zero copy.
     */
    public DataBuffer compact(Arena targetArena) {
        Objects.requireNonNull(targetArena, "targetArena cannot be null");
        if (this.chunks.size() == 1) {
            DataBuffer single = this.chunks.getFirst();
            if (single.offset() == 0L && targetArena.equals(single.arena()) && single.name().equals(this.name)) {
                return single;
            }
        }
        return toBuilder(targetArena).build();
    }

    public int chunkCount() {
        return chunks.size();
    }

    public DataBuffer chunk(int chunkIndex) {
        return chunks.get(chunkIndex);
    }

    public List<DataBuffer> chunks() {
        return chunks;
    }

    private int locateChunk(long index) {
        Objects.checkIndex(index, this.length);
        if (chunks.size() == 1) {
            return 0;
        }
        int pos = Arrays.binarySearch(chunkOffsets, 0, chunks.size(), index);
        return pos >= 0 ? pos : (-pos - 2);
    }

    public int getInt(long index) {
        if (type != DataType.INT32) throw new IllegalStateException("Buffer is not INT32, is " + type);
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return switch (chunks.get(c)) {
            case Int32Buffer b -> b.get(localIdx);
            case ImportedDataBuffer b -> b.getInt(localIdx);
            default -> throw new IllegalStateException("Unexpected INT32 chunk implementation");
        };
    }

    public long getLong(long index) {
        if (type != DataType.INT64 && type != DataType.TIMESTAMP_MS && type != DataType.TIMESTAMP_US) {
            throw new IllegalStateException("Buffer is not INT64, is " + type);
        }
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return switch (chunks.get(c)) {
            case Int64Buffer b -> b.get(localIdx);
            case ImportedDataBuffer b -> b.getLong(localIdx);
            default -> throw new IllegalStateException("Unexpected INT64 chunk implementation");
        };
    }

    public float getFloat(long index) {
        if (type != DataType.FLOAT32) throw new IllegalStateException("Buffer is not FLOAT32, is " + type);
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return switch (chunks.get(c)) {
            case Float32Buffer b -> b.get(localIdx);
            case ImportedDataBuffer b -> b.getFloat(localIdx);
            default -> throw new IllegalStateException("Unexpected FLOAT32 chunk implementation");
        };
    }

    public double getDouble(long index) {
        if (type != DataType.FLOAT64) throw new IllegalStateException("Buffer is not FLOAT64, is " + type);
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return switch (chunks.get(c)) {
            case Float64Buffer b -> b.get(localIdx);
            case ImportedDataBuffer b -> b.getDouble(localIdx);
            default -> throw new IllegalStateException("Unexpected FLOAT64 chunk implementation");
        };
    }

    public boolean getBoolean(long index) {
        if (type != DataType.BOOLEAN) throw new IllegalStateException("Buffer is not BOOLEAN, is " + type);
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return switch (chunks.get(c)) {
            case BooleanBuffer b -> b.get(localIdx);
            case ImportedDataBuffer b -> b.getBoolean(localIdx);
            default -> throw new IllegalStateException("Unexpected BOOLEAN chunk implementation");
        };
    }

    public String getString(long index) {
        if (type != DataType.UTF8 && type != DataType.DICTIONARY) {
            throw new IllegalStateException("Buffer is not UTF8 or DICTIONARY, is " + type);
        }
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return switch (chunks.get(c)) {
            case UTF8Buffer b -> b.getString(localIdx);
            case DictionaryBuffer b -> b.getString(localIdx);
            case ImportedDataBuffer b -> b.getString(localIdx);
            default -> throw new IllegalStateException("Unexpected string chunk implementation");
        };
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DataType type() {
        return type;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public long offset() {
        return 0L;
    }

    @Override
    public Arena arena() {
        return arena;
    }

    @Override
    public long nullCount() {
        return this.nullCount;
    }

    @Override
    public boolean isNull(long index) {
        int c = locateChunk(index);
        return chunks.get(c).isNull(index - chunkOffsets[c]);
    }

    @Override
    public void setNull(long index) {
        int c = locateChunk(index);
        DataBuffer targetChunk = chunks.get(c);
        long before = targetChunk.nullCount();
        targetChunk.setNull(index - chunkOffsets[c]);
        this.nullCount += (targetChunk.nullCount() - before);
    }

    @Override
    public void setAllValid() {
        for (DataBuffer chunk : chunks) {
            chunk.setAllValid();
        }
        this.nullCount = 0L;
    }

    @Override
    public int bufferCount() {
        if (chunks.size() == 1) {
            return chunks.getFirst().bufferCount();
        }
        throw new UnsupportedOperationException(
                "ChunkedBuffer with " + chunks.size() + " chunks is discontiguous; iterate chunks() or call compact(arena)."
        );
    }

    @Override
    public MemorySegment buffer(int index) {
        if (chunks.size() == 1) {
            return chunks.getFirst().buffer(index);
        }
        throw new UnsupportedOperationException(
                "ChunkedBuffer with " + chunks.size() + " chunks is discontiguous; iterate chunks() or call compact(arena)."
        );
    }

    @Override
    public ChunkedBuffer slice(long sliceOffset, long sliceLength) {
        Objects.checkFromIndexSize(sliceOffset, sliceLength, this.length);
        if (sliceLength == 0) {
            return new ChunkedBuffer(this.arena, this.name, this.type, List.of());
        }
        if (sliceOffset == 0 && sliceLength == this.length) {
            return this;
        }

        int startChunk = locateChunk(sliceOffset);
        int endChunk = locateChunk(sliceOffset + sliceLength - 1L);

        if (startChunk == endChunk) {
            DataBuffer singleSlice = chunks.get(startChunk).slice(sliceOffset - chunkOffsets[startChunk], sliceLength);
            return new ChunkedBuffer(this.arena, this.name, this.type, List.of(singleSlice));
        }

        List<DataBuffer> slicedChunks = new ArrayList<>(endChunk - startChunk + 1);

        long firstLocalOffset = sliceOffset - chunkOffsets[startChunk];
        long firstLen = chunks.get(startChunk).length() - firstLocalOffset;
        slicedChunks.add(chunks.get(startChunk).slice(firstLocalOffset, firstLen));

        for (int c = startChunk + 1; c < endChunk; c++) {
            slicedChunks.add(chunks.get(c));
        }

        long lastLen = (sliceOffset + sliceLength) - chunkOffsets[endChunk];
        slicedChunks.add(chunks.get(endChunk).slice(0, lastLen));

        return new ChunkedBuffer(this.arena, this.name, this.type, slicedChunks);
    }

    @Override
    public ChunkedBuffer withName(String newName) {
        Objects.requireNonNull(newName, "newName cannot be null");
        return new ChunkedBuffer(this.arena, newName, this.type, this.chunks);
    }

    @Override
    public DataBuffer take(Int32Buffer indices, Arena targetArena) {
        Objects.requireNonNull(indices, "indices cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        long outLen = indices.length();
        boolean idxHasNulls = indices.nullCount() > 0;
        boolean srcHasNulls = this.nullCount() > 0;

        return switch (this.type) {
            case INT32 -> {
                Int32Buffer out = Int32Buffer.allocate(targetArena, this.name, outLen);
                for (long i = 0; i < outLen; i++) {
                    if (idxHasNulls && indices.isNull(i)) out.setNull(i);
                    else {
                        int r = indices.get(i);
                        if (srcHasNulls && isNull(r)) out.setNull(i);
                        else out.set(i, getInt(r));
                    }
                }
                yield out;
            }
            case INT64 -> {
                Int64Buffer out = Int64Buffer.allocate(targetArena, this.name, outLen);
                for (long i = 0; i < outLen; i++) {
                    if (idxHasNulls && indices.isNull(i)) out.setNull(i);
                    else {
                        int r = indices.get(i);
                        if (srcHasNulls && isNull(r)) out.setNull(i);
                        else out.set(i, getLong(r));
                    }
                }
                yield out;
            }
            case FLOAT32 -> {
                Float32Buffer out = Float32Buffer.allocate(targetArena, this.name, outLen);
                for (long i = 0; i < outLen; i++) {
                    if (idxHasNulls && indices.isNull(i)) out.setNull(i);
                    else {
                        int r = indices.get(i);
                        if (srcHasNulls && isNull(r)) out.setNull(i);
                        else out.set(i, getFloat(r));
                    }
                }
                yield out;
            }
            case FLOAT64 -> {
                Float64Buffer out = Float64Buffer.allocate(targetArena, this.name, outLen);
                for (long i = 0; i < outLen; i++) {
                    if (idxHasNulls && indices.isNull(i)) out.setNull(i);
                    else {
                        int r = indices.get(i);
                        if (srcHasNulls && isNull(r)) out.setNull(i);
                        else out.set(i, getDouble(r));
                    }
                }
                yield out;
            }
            case BOOLEAN -> {
                BooleanBuffer out = BooleanBuffer.allocate(targetArena, this.name, outLen);
                for (long i = 0; i < outLen; i++) {
                    if (idxHasNulls && indices.isNull(i)) out.setNull(i);
                    else {
                        int r = indices.get(i);
                        if (srcHasNulls && isNull(r)) out.setNull(i);
                        else out.set(i, getBoolean(r));
                    }
                }
                yield out;
            }
            case UTF8, DICTIONARY -> compact(targetArena).take(indices, targetArena);
            default -> throw new UnsupportedOperationException("take() not supported for chunked type: " + this.type);
        };
    }

    @Override
    public AbstractBufferBuilder toBuilder(Arena targetArena) {
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        return switch (this.type) {
            case INT32 -> {
                Int32BufferBuilder b = new Int32BufferBuilder(targetArena, this.name, this.length);
                for (DataBuffer c : chunks) b.appendImportedBlock(c, 0, c.length());
                yield b;
            }
            case INT64 -> {
                Int64BufferBuilder b = new Int64BufferBuilder(targetArena, this.name, this.length);
                for (DataBuffer c : chunks) b.appendImportedBlock(c, 0, c.length());
                yield b;
            }
            case FLOAT32 -> {
                Float32BufferBuilder b = new Float32BufferBuilder(targetArena, this.name, this.length);
                for (DataBuffer c : chunks) b.appendImportedBlock(c, 0, c.length());
                yield b;
            }
            case FLOAT64 -> {
                Float64BufferBuilder b = new Float64BufferBuilder(targetArena, this.name, this.length);
                for (DataBuffer c : chunks) b.appendImportedBlock(c, 0, c.length());
                yield b;
            }
            case BOOLEAN -> {
                BooleanBufferBuilder b = new BooleanBufferBuilder(targetArena, this.name, this.length);
                for (DataBuffer c : chunks) b.appendImportedBlock(c, 0, c.length());
                yield b;
            }
            case UTF8 -> {
                long totalDataBytes = 0L;
                long intBytes = ValueLayout.JAVA_INT.byteSize();
                for (DataBuffer c : chunks) {
                    MemorySegment offsets = c.buffer(1);
                    int start = offsets.get(ValueLayout.JAVA_INT, c.offset() * intBytes);
                    int end = offsets.get(ValueLayout.JAVA_INT, (c.offset() + c.length()) * intBytes);
                    totalDataBytes = Math.addExact(totalDataBytes, (long) end - start);
                }
                UTF8BufferBuilder b = new UTF8BufferBuilder(targetArena, this.name, this.length, totalDataBytes);
                for (DataBuffer c : chunks) b.appendImportedBlock(c, 0, c.length());
                yield b;
            }
            case DICTIONARY -> {
                DictionaryBufferBuilder b = new DictionaryBufferBuilder(targetArena, this.name, this.length);
                for (DataBuffer c : chunks) b.appendImportedBlock(c, 0, c.length());
                yield b;
            }
            default -> throw new UnsupportedOperationException("toBuilder() not supported for chunked type: " + this.type);
        };
    }
}