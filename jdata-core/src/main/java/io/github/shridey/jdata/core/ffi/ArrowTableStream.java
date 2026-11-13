package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.buffer.BooleanBuffer;
import io.github.shridey.jdata.core.buffer.DictionaryBuffer;
import io.github.shridey.jdata.core.buffer.Float32Buffer;
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import io.github.shridey.jdata.core.buffer.Int32Buffer;
import io.github.shridey.jdata.core.buffer.Int64Buffer;
import io.github.shridey.jdata.core.buffer.UTF8Buffer;
import io.github.shridey.jdata.core.internal.ArrowCStructs;
import io.github.shridey.jdata.core.layout.Field;
import io.github.shridey.jdata.core.layout.Schema;
import io.github.shridey.jdata.core.layout.Table;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A consumer over an Apache Arrow C Stream Interface ({@code ArrowArrayStream}).
 * Pulls columnar {@link Table} batches on demand, allowing each batch to bind to its own
 * short-lived {@link Arena} (such as a {@link io.github.shridey.jdata.core.PooledArena})
 * so datasets larger than RAM can be processed with bounded memory.
 */
public final class ArrowTableStream implements AutoCloseable {

    private final MemorySegment streamStruct;
    private final Arena streamArena;
    private final MemorySegment heldSchemaStruct;
    private final Schema schema;
    private boolean exhausted = false;
    private boolean closed = false;

    ArrowTableStream(MemorySegment streamStruct, Arena streamArena, MemorySegment heldSchemaStruct, Schema schema) {
        this.streamStruct = streamStruct;
        this.streamArena = streamArena;
        this.heldSchemaStruct = heldSchemaStruct;
        this.schema = schema;
    }

    /**
     * Returns the {@link Schema} shared by all batches in this stream.
     */
    public Schema schema() {
        return schema;
    }

    /**
     * Pulls the next {@link Table} batch from the stream and binds its off-heap buffers to {@code batchArena}.
     *
     * @param batchArena The arena that controls the lifecycle of the returned batch.
     * @return The next {@link Table} batch, or {@code null} if the stream has reached the end.
     */
    public Table nextBatch(Arena batchArena) {
        Objects.requireNonNull(batchArena, "batchArena cannot be null");
        ensureOpen();
        if (exhausted) {
            return null;
        }

        MemorySegment getNextPtr = streamStruct.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_GET_NEXT);
        MemorySegment outArray = batchArena.allocate(ArrowCStructs.ARROW_ARRAY_LAYOUT);

        int errCode;
        try {
            errCode = (int) ArrowCImporter.STREAM_GET_NEXT_CALL.invokeExact(getNextPtr, streamStruct, outArray);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to invoke native ArrowArrayStream.get_next callback", t);
        }

        if (errCode != 0) {
            String msg = lastError();
            throw new IllegalStateException(
                    "ArrowArrayStream.get_next failed with error code " + errCode + (msg != null ? ": " + msg : "")
            );
        }

        // Per Arrow C Stream spec: end of stream is signaled by return code 0 and outArray.release == NULL
        MemorySegment releasePtr = outArray.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE);
        if (releasePtr.equals(MemorySegment.NULL)) {
            exhausted = true;
            return null;
        }

        return ArrowCImporter.importTableFromHeldSchema(outArray, heldSchemaStruct, schema, batchArena);
    }

    /**
     * Pulls the next {@link Table} batch using the stream's own arena.
     */
    public Table nextBatch() {
        return nextBatch(streamArena);
    }

    /**
     * Reads all remaining batches from the stream into {@code targetArena} and concatenates
     * them in O(1) zero-copy into a single unified {@link Table}.
     */
    public Table readAll(Arena targetArena) {
        Objects.requireNonNull(targetArena, "targetArena cannot be null");
        List<Table> batches = new ArrayList<>();
        Table batch;
        while ((batch = nextBatch(targetArena)) != null) {
            if (batch.rowCount() > 0) {
                batches.add(batch);
            }
        }
        if (batches.isEmpty()) {
            return createEmptyTable(schema, targetArena);
        }
        return Table.concat(batches);
    }

    /**
     * Queries {@code get_last_error} on the underlying native stream, or returns {@code null} if none.
     */
    public String lastError() {
        if (closed) {
            return null;
        }
        MemorySegment getLastErrPtr = streamStruct.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_GET_LAST_ERROR);
        if (getLastErrPtr.equals(MemorySegment.NULL)) {
            return null;
        }
        try {
            MemorySegment errStrPtr = (MemorySegment) ArrowCImporter.STREAM_GET_LAST_ERROR_CALL.invokeExact(
                    getLastErrPtr, streamStruct
            );
            if (errStrPtr.equals(MemorySegment.NULL)) {
                return null;
            }
            return errStrPtr.reinterpret(Long.MAX_VALUE).getString(0);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to invoke native ArrowArrayStream.get_last_error callback", t);
        }
    }

    public boolean isClosed() {
        return closed || streamStruct.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_RELEASE).equals(MemorySegment.NULL);
    }

    private void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("ArrowTableStream is already closed.");
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            exhausted = true;
            try {
                ArrowCImporter.releaseSchemaIfPresent(heldSchemaStruct);
            } finally {
                ArrowCImporter.releaseStreamIfPresent(streamStruct);
            }
        }
    }

    private static Table createEmptyTable(Schema schema, Arena arena) {
        List<DataBuffer> emptyCols = new ArrayList<>(schema.columnCount());
        for (int i = 0; i < schema.columnCount(); i++) {
            Field f = schema.getField(i);
            DataBuffer col = switch (f.type()) {
                case INT32 -> Int32Buffer.allocate(arena, f.name(), 0);
                case INT64, TIMESTAMP_MS, TIMESTAMP_US -> Int64Buffer.allocate(arena, f.name(), 0);
                case FLOAT32 -> Float32Buffer.allocate(arena, f.name(), 0);
                case FLOAT64 -> Float64Buffer.allocate(arena, f.name(), 0);
                case BOOLEAN -> BooleanBuffer.allocate(arena, f.name(), 0);
                case UTF8 -> UTF8Buffer.allocate(arena, f.name(), new String[0]);
                case DICTIONARY -> DictionaryBuffer.allocate(arena, f.name(), new String[0]);
                default -> throw new UnsupportedOperationException("Unsupported empty column type: " + f.type());
            };
            emptyCols.add(col);
        }
        return new Table(schema, emptyCols);
    }
}