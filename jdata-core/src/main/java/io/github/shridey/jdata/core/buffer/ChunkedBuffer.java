package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.ffi.ImportedDataBuffer;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;

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

    /**
     * Constructs a {@link ChunkedBuffer} over an ordered list of chunks, automatically flattening
     * any nested {@link ChunkedBuffer} instances and omitting empty 0-length chunks.
     *
     * @param arena     the governing memory arena
     * @param name      the column name
     * @param type      the logical {@link DataType} shared by all chunks
     * @param rawChunks the ordered list of chunks
     * @throws IllegalArgumentException if any chunk has a mismatched {@link DataType}
     */
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
     *
     * @param chunks the non-empty array of chunks
     * @return a new {@link ChunkedBuffer}
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
     *
     * @param nextChunk the chunk to append
     * @return a new {@link ChunkedBuffer} including {@code nextChunk}
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
     *
     * @param targetArena the arena in which to allocate the compacted buffer
     * @return a single contiguous {@link DataBuffer}
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

    /**
     * Returns the number of physical chunks in this buffer.
     *
     * @return the chunk count
     */
    public int chunkCount() {
        return chunks.size();
    }

    /**
     * Returns the physical chunk at the specified zero-based chunk index.
     *
     * @param chunkIndex the zero-based chunk index
     * @return the chunk {@link DataBuffer}
     */
    public DataBuffer chunk(int chunkIndex) {
        return chunks.get(chunkIndex);
    }

    /**
     * Returns an unmodifiable list of the physical chunks backing this buffer.
     *
     * @return the list of chunks
     */
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

    /**
     * Resolves the chunk index for {@code index} in O(1) time when {@code index} falls in {@code hintChunk}
     * or {@code hintChunk + 1} (the common case for monotonic filter SelectionVectors), falling back to binary search.
     */
    private int locateChunkWithHint(long index, int hintChunk) {
        Objects.checkIndex(index, this.length);
        int size = chunks.size();
        if (size == 1) {
            return 0;
        }
        if (index >= chunkOffsets[hintChunk] && index < chunkOffsets[hintChunk + 1]) {
            return hintChunk;
        }
        int nextChunk = hintChunk + 1;
        if (nextChunk < size && index >= chunkOffsets[nextChunk] && index < chunkOffsets[nextChunk + 1]) {
            return nextChunk;
        }
        int pos = Arrays.binarySearch(chunkOffsets, 0, size, index);
        return pos >= 0 ? pos : (-pos - 2);
    }

    /**
     * Reads the 32-bit integer at the given logical row index via O(log C) chunk resolution.
     *
     * @param index the zero-based logical index
     * @return the {@code int} value at {@code index}
     */
    public int getInt(long index) {
        if (type != DataType.INT32) throw new IllegalStateException("Buffer is not INT32, is " + type);
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return readChunkInt(chunks.get(c), localIdx);
    }

    private static int readChunkInt(DataBuffer chunk, long localIdx) {
        return switch (chunk) {
            case Int32Buffer b -> b.get(localIdx);
            case ImportedDataBuffer b -> b.getInt(localIdx);
            default -> throw new IllegalStateException("Unexpected INT32 chunk implementation");
        };
    }

    /**
     * Reads the 64-bit integer at the given logical row index via O(log C) chunk resolution.
     *
     * @param index the zero-based logical index
     * @return the {@code long} value at {@code index}
     */
    public long getLong(long index) {
        if (type != DataType.INT64 && type != DataType.TIMESTAMP_MS && type != DataType.TIMESTAMP_US) {
            throw new IllegalStateException("Buffer is not INT64, is " + type);
        }
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return readChunkLong(chunks.get(c), localIdx);
    }

    private static long readChunkLong(DataBuffer chunk, long localIdx) {
        return switch (chunk) {
            case Int64Buffer b -> b.get(localIdx);
            case ImportedDataBuffer b -> b.getLong(localIdx);
            default -> throw new IllegalStateException("Unexpected INT64 chunk implementation");
        };
    }

    /**
     * Reads the 32-bit float at the given logical row index via O(log C) chunk resolution.
     *
     * @param index the zero-based logical index
     * @return the {@code float} value at {@code index}
     */
    public float getFloat(long index) {
        if (type != DataType.FLOAT32) throw new IllegalStateException("Buffer is not FLOAT32, is " + type);
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return readChunkFloat(chunks.get(c), localIdx);
    }

    private static float readChunkFloat(DataBuffer chunk, long localIdx) {
        return switch (chunk) {
            case Float32Buffer b -> b.get(localIdx);
            case ImportedDataBuffer b -> b.getFloat(localIdx);
            default -> throw new IllegalStateException("Unexpected FLOAT32 chunk implementation");
        };
    }

    /**
     * Reads the 64-bit double at the given logical row index via O(log C) chunk resolution.
     *
     * @param index the zero-based logical index
     * @return the {@code double} value at {@code index}
     */
    public double getDouble(long index) {
        if (type != DataType.FLOAT64) throw new IllegalStateException("Buffer is not FLOAT64, is " + type);
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return readChunkDouble(chunks.get(c), localIdx);
    }

    private static double readChunkDouble(DataBuffer chunk, long localIdx) {
        return switch (chunk) {
            case Float64Buffer b -> b.get(localIdx);
            case ImportedDataBuffer b -> b.getDouble(localIdx);
            default -> throw new IllegalStateException("Unexpected FLOAT64 chunk implementation");
        };
    }

    /**
     * Reads the boolean value at the given logical row index via O(log C) chunk resolution.
     *
     * @param index the zero-based logical index
     * @return the {@code boolean} value at {@code index}
     */
    public boolean getBoolean(long index) {
        if (type != DataType.BOOLEAN) throw new IllegalStateException("Buffer is not BOOLEAN, is " + type);
        int c = locateChunk(index);
        long localIdx = index - chunkOffsets[c];
        return readChunkBoolean(chunks.get(c), localIdx);
    }

    private static boolean readChunkBoolean(DataBuffer chunk, long localIdx) {
        return switch (chunk) {
            case BooleanBuffer b -> b.get(localIdx);
            case ImportedDataBuffer b -> b.getBoolean(localIdx);
            default -> throw new IllegalStateException("Unexpected BOOLEAN chunk implementation");
        };
    }

    /**
     * Reads the decoded UTF-8 {@link String} at the given logical row index via O(log C) chunk resolution.
     *
     * @param index the zero-based logical index
     * @return the decoded {@link String}, or {@code null} if the slot is null
     */
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
        int hint = 0;

        return switch (this.type) {
            case INT32 -> {
                Int32Buffer out = Int32Buffer.allocate(targetArena, this.name, outLen);
                for (long i = 0; i < outLen; i++) {
                    if (idxHasNulls && indices.isNull(i)) out.setNull(i);
                    else {
                        int r = indices.get(i);
                        hint = locateChunkWithHint(r, hint);
                        DataBuffer ch = chunks.get(hint);
                        long local = r - chunkOffsets[hint];
                        if (srcHasNulls && ch.isNull(local)) out.setNull(i);
                        else out.set(i, readChunkInt(ch, local));
                    }
                }
                yield out;
            }
            case INT64, TIMESTAMP_MS, TIMESTAMP_US -> {
                Int64Buffer out = Int64Buffer.allocate(targetArena, this.name, outLen);
                for (long i = 0; i < outLen; i++) {
                    if (idxHasNulls && indices.isNull(i)) out.setNull(i);
                    else {
                        int r = indices.get(i);
                        hint = locateChunkWithHint(r, hint);
                        DataBuffer ch = chunks.get(hint);
                        long local = r - chunkOffsets[hint];
                        if (srcHasNulls && ch.isNull(local)) out.setNull(i);
                        else out.set(i, readChunkLong(ch, local));
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
                        hint = locateChunkWithHint(r, hint);
                        DataBuffer ch = chunks.get(hint);
                        long local = r - chunkOffsets[hint];
                        if (srcHasNulls && ch.isNull(local)) out.setNull(i);
                        else out.set(i, readChunkFloat(ch, local));
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
                        hint = locateChunkWithHint(r, hint);
                        DataBuffer ch = chunks.get(hint);
                        long local = r - chunkOffsets[hint];
                        if (srcHasNulls && ch.isNull(local)) out.setNull(i);
                        else out.set(i, readChunkDouble(ch, local));
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
                        hint = locateChunkWithHint(r, hint);
                        DataBuffer ch = chunks.get(hint);
                        long local = r - chunkOffsets[hint];
                        if (srcHasNulls && ch.isNull(local)) out.setNull(i);
                        else out.set(i, readChunkBoolean(ch, local));
                    }
                }
                yield out;
            }
            case UTF8 -> takeChunkedUtf8(indices, targetArena, outLen, idxHasNulls, srcHasNulls);
            case DICTIONARY -> takeChunkedDictionary(indices, targetArena, outLen, idxHasNulls, srcHasNulls);
            default -> throw new UnsupportedOperationException("take() not supported for chunked type: " + this.type);
        };
    }

    private UTF8Buffer takeChunkedUtf8(Int32Buffer indices, Arena targetArena,
                                       long outLen, boolean idxHasNulls, boolean srcHasNulls) {
        long intBytes = ValueLayout.JAVA_INT.byteSize();
        long totalBytes = 0;
        int hint = 0;

        // Pass 1: Compute exact UTF-8 byte length of selected rows across chunks without compacting
        for (long i = 0; i < outLen; i++) {
            if (!idxHasNulls || !indices.isNull(i)) {
                int r = indices.get(i);
                hint = locateChunkWithHint(r, hint);
                DataBuffer ch = chunks.get(hint);
                long local = r - chunkOffsets[hint];
                if (!srcHasNulls || !ch.isNull(local)) {
                    long phys = ch.offset() + local;
                    MemorySegment chOffsets = ch.buffer(1);
                    int s = chOffsets.get(ValueLayout.JAVA_INT, phys * intBytes);
                    int e = chOffsets.get(ValueLayout.JAVA_INT, (phys + 1L) * intBytes);
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

        UTF8Buffer out = new UTF8Buffer(this.name, targetArena, outLen, 0, outValidity, outOffsets, outData, 0L);
        out.setAllValid();

        // Pass 2: Copy only the selected UTF-8 slices directly from their respective chunks
        int cursor = 0;
        hint = 0;
        for (long i = 0; i < outLen; i++) {
            outOffsets.set(ValueLayout.JAVA_INT, i * intBytes, cursor);
            if (idxHasNulls && indices.isNull(i)) {
                out.setNull(i);
            } else {
                int r = indices.get(i);
                hint = locateChunkWithHint(r, hint);
                DataBuffer ch = chunks.get(hint);
                long local = r - chunkOffsets[hint];
                if (srcHasNulls && ch.isNull(local)) {
                    out.setNull(i);
                } else {
                    long phys = ch.offset() + local;
                    MemorySegment chOffsets = ch.buffer(1);
                    int s = chOffsets.get(ValueLayout.JAVA_INT, phys * intBytes);
                    int e = chOffsets.get(ValueLayout.JAVA_INT, (phys + 1L) * intBytes);
                    int len = e - s;
                    if (len > 0) {
                        MemorySegment.copy(ch.buffer(2), s, outData, cursor, len);
                        cursor += len;
                    }
                }
            }
        }
        outOffsets.set(ValueLayout.JAVA_INT, outLen * intBytes, cursor);
        return out;
    }

    private DictionaryBuffer takeChunkedDictionary(Int32Buffer indices, Arena targetArena,
                                                   long outLen, boolean idxHasNulls, boolean srcHasNulls) {
        DictionaryBufferBuilder builder = new DictionaryBufferBuilder(targetArena, this.name, outLen);
        long intBytes = ValueLayout.JAVA_INT.byteSize();
        int hint = 0;

        for (long i = 0; i < outLen; i++) {
            if (idxHasNulls && indices.isNull(i)) {
                builder.appendNull();
            } else {
                int r = indices.get(i);
                hint = locateChunkWithHint(r, hint);
                DataBuffer ch = chunks.get(hint);
                long local = r - chunkOffsets[hint];
                if (srcHasNulls && ch.isNull(local)) {
                    builder.appendNull();
                } else {
                    DataBuffer chDict = switch (ch) {
                        case DictionaryBuffer db -> db.dictionary();
                        case ImportedDataBuffer idb when idb.dictionary() != null -> idb.dictionary();
                        default -> throw new IllegalStateException("Unexpected DICTIONARY chunk implementation");
                    };
                    int code = ch.buffer(1).get(ValueLayout.JAVA_INT, (ch.offset() + local) * intBytes);
                    long dictPhys = chDict.offset() + code;
                    MemorySegment dictOffsets = chDict.buffer(1);
                    int s = dictOffsets.get(ValueLayout.JAVA_INT, dictPhys * intBytes);
                    int e = dictOffsets.get(ValueLayout.JAVA_INT, (dictPhys + 1L) * intBytes);
                    builder.appendRawUtf8Entry(chDict.buffer(2), s, e - s);
                }
            }
        }
        return builder.build();
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
            case INT64, TIMESTAMP_MS, TIMESTAMP_US -> {
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