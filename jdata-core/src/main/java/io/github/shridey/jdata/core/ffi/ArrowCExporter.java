package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.ChunkedBuffer;
import io.github.shridey.jdata.core.buffer.DictionaryBuffer;
import io.github.shridey.jdata.core.internal.ArrowCStructs;
import io.github.shridey.jdata.core.layout.Field;
import io.github.shridey.jdata.core.layout.Schema;
import io.github.shridey.jdata.core.layout.Table;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exports {@link Table}, {@link Schema}, and {@link DataBuffer} instances into native
 * Apache Arrow C Data Interface ({@code ArrowArray}, {@code ArrowSchema}) and
 * Apache Arrow C Stream Interface ({@code ArrowArrayStream}) structs for zero-copy FFI sharing.
 */
public final class ArrowCExporter {

    private static final long ARROW_FLAG_NULLABLE = 2L;
    private static final int ERRNO_EINVAL = 22;
    private static final int ERRNO_EIO = 5;

    private static final MemorySegment ARRAY_RELEASE_STUB;
    private static final MemorySegment SCHEMA_RELEASE_STUB;
    private static final MemorySegment STREAM_GET_SCHEMA_STUB;
    private static final MemorySegment STREAM_GET_NEXT_STUB;
    private static final MemorySegment STREAM_GET_LAST_ERROR_STUB;
    private static final MemorySegment STREAM_RELEASE_STUB;

    private static final AtomicLong STREAM_ID_GENERATOR = new AtomicLong(1L);
    private static final Map<Long, ExportedStreamState> ACTIVE_STREAMS = new ConcurrentHashMap<>();

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Linker linker = Linker.nativeLinker();
            FunctionDescriptor releaseDesc = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);
            FunctionDescriptor streamIntDesc = FunctionDescriptor.of(
                    ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS
            );
            FunctionDescriptor streamErrDesc = FunctionDescriptor.of(
                    ValueLayout.ADDRESS, ValueLayout.ADDRESS
            );

            MethodHandle arrayReleaseHandle = lookup.findStatic(
                    ArrowCExporter.class, "releaseArrayCallback",
                    MethodType.methodType(void.class, MemorySegment.class)
            );
            MethodHandle schemaReleaseHandle = lookup.findStatic(
                    ArrowCExporter.class, "releaseSchemaCallback",
                    MethodType.methodType(void.class, MemorySegment.class)
            );
            MethodHandle streamGetSchemaHandle = lookup.findStatic(
                    ArrowCExporter.class, "streamGetSchemaCallback",
                    MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class)
            );
            MethodHandle streamGetNextHandle = lookup.findStatic(
                    ArrowCExporter.class, "streamGetNextCallback",
                    MethodType.methodType(int.class, MemorySegment.class, MemorySegment.class)
            );
            MethodHandle streamGetLastErrorHandle = lookup.findStatic(
                    ArrowCExporter.class, "streamGetLastErrorCallback",
                    MethodType.methodType(MemorySegment.class, MemorySegment.class)
            );
            MethodHandle streamReleaseHandle = lookup.findStatic(
                    ArrowCExporter.class, "releaseStreamCallback",
                    MethodType.methodType(void.class, MemorySegment.class)
            );

            ARRAY_RELEASE_STUB = linker.upcallStub(arrayReleaseHandle, releaseDesc, Arena.global());
            SCHEMA_RELEASE_STUB = linker.upcallStub(schemaReleaseHandle, releaseDesc, Arena.global());
            STREAM_GET_SCHEMA_STUB = linker.upcallStub(streamGetSchemaHandle, streamIntDesc, Arena.global());
            STREAM_GET_NEXT_STUB = linker.upcallStub(streamGetNextHandle, streamIntDesc, Arena.global());
            STREAM_GET_LAST_ERROR_STUB = linker.upcallStub(streamGetLastErrorHandle, streamErrDesc, Arena.global());
            STREAM_RELEASE_STUB = linker.upcallStub(streamReleaseHandle, releaseDesc, Arena.global());
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private ArrowCExporter() {}

    /**
     * Exports a {@link Table} into a top-level Struct ({@code +s}) {@code ArrowArray} C struct.
     *
     * @param table       the table to export
     * @param structArena the arena used to allocate the C struct metadata
     * @return a {@link MemorySegment} pointing to the populated {@code ArrowArray} struct
     */
    public static MemorySegment exportTableArray(Table table, Arena structArena) {
        Objects.requireNonNull(table, "table cannot be null");
        Objects.requireNonNull(structArena, "structArena cannot be null");
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_ARRAY_LAYOUT);
        populateTableArray(table, struct, structArena);
        return struct;
    }

    private static void populateTableArray(Table table, MemorySegment struct, Arena structArena) {
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_LENGTH, table.rowCount());
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_NULL_COUNT, 0L);
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_OFFSET, 0L);

        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_BUFFERS, 1L);
        MemorySegment buffersPtrArray = structArena.allocate(ValueLayout.ADDRESS, 1);
        buffersPtrArray.setAtIndex(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_BUFFERS, buffersPtrArray);

        int numCols = table.columnCount();
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_CHILDREN, numCols);

        if (numCols > 0) {
            MemorySegment childrenPtrArray = structArena.allocate(ValueLayout.ADDRESS, numCols);
            for (int i = 0; i < numCols; i++) {
                MemorySegment childArray = exportArray(table.getColumn(i), structArena);
                childrenPtrArray.setAtIndex(ValueLayout.ADDRESS, i, childArray);
            }
            struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_CHILDREN, childrenPtrArray);
        } else {
            struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_CHILDREN, MemorySegment.NULL);
        }

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_DICTIONARY, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, ARRAY_RELEASE_STUB);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_PRIVATE_DATA, MemorySegment.NULL);
    }

    /**
     * Exports a {@link Table}'s schema into a top-level Struct ({@code +s}) {@code ArrowSchema} C struct.
     *
     * @param table       the table whose schema should be exported
     * @param structArena the arena used to allocate the C struct metadata
     * @return a {@link MemorySegment} pointing to the populated {@code ArrowSchema} struct
     */
    public static MemorySegment exportTableSchema(Table table, Arena structArena) {
        Objects.requireNonNull(table, "table cannot be null");
        return exportTableSchema(table.schema(), structArena);
    }

    /**
     * Exports a {@link Schema} into a top-level Struct ({@code +s}) {@code ArrowSchema} C struct.
     *
     * @param schema      the schema to export
     * @param structArena the arena used to allocate the C struct metadata
     * @return a {@link MemorySegment} pointing to the populated {@code ArrowSchema} struct
     */
    public static MemorySegment exportTableSchema(Schema schema, Arena structArena) {
        Objects.requireNonNull(schema, "schema cannot be null");
        Objects.requireNonNull(structArena, "structArena cannot be null");
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_SCHEMA_LAYOUT);
        populateTableSchema(schema, struct, structArena);
        return struct;
    }

    private static void populateTableSchema(Schema schema, MemorySegment struct, Arena structArena) {
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT, structArena.allocateFrom("+s"));
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME, structArena.allocateFrom("table"));
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_METADATA, MemorySegment.NULL);
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_FLAGS, 0L);

        int numCols = schema.columnCount();
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_N_CHILDREN, numCols);

        if (numCols > 0) {
            MemorySegment childrenPtrArray = structArena.allocate(ValueLayout.ADDRESS, numCols);
            for (int i = 0; i < numCols; i++) {
                Field field = schema.getField(i);
                MemorySegment childSchema = exportFieldSchema(field.name(), field.type(), field.nullable(), structArena);
                childrenPtrArray.setAtIndex(ValueLayout.ADDRESS, i, childSchema);
            }
            struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN, childrenPtrArray);
        } else {
            struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN, MemorySegment.NULL);
        }

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_DICTIONARY, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE, SCHEMA_RELEASE_STUB);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_PRIVATE_DATA, MemorySegment.NULL);
    }

    /**
     * Exports a lazy {@link Iterator} of {@link Table} batches as an Apache Arrow C {@code ArrowArrayStream}.
     *
     * @param schema      the schema shared by all batches in the stream
     * @param batches     the iterator yielding {@link Table} batches on demand
     * @param structArena the arena used to allocate stream and batch C struct metadata
     * @return a {@link MemorySegment} pointing to the populated {@code ArrowArrayStream} struct
     */
    public static MemorySegment exportTableStream(Schema schema, Iterator<Table> batches, Arena structArena) {
        Objects.requireNonNull(schema, "schema cannot be null");
        Objects.requireNonNull(batches, "batches iterator cannot be null");
        Objects.requireNonNull(structArena, "structArena cannot be null");

        long streamId = STREAM_ID_GENERATOR.getAndIncrement();
        ACTIVE_STREAMS.put(streamId, new ExportedStreamState(schema, batches, structArena));

        MemorySegment rawStream = structArena.allocate(ArrowCStructs.ARROW_ARRAY_STREAM_LAYOUT);
        rawStream.set(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_GET_SCHEMA, STREAM_GET_SCHEMA_STUB);
        rawStream.set(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_GET_NEXT, STREAM_GET_NEXT_STUB);
        rawStream.set(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_GET_LAST_ERROR, STREAM_GET_LAST_ERROR_STUB);
        rawStream.set(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_RELEASE, STREAM_RELEASE_STUB);
        rawStream.set(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_PRIVATE_DATA, MemorySegment.ofAddress(streamId));

        // Ensure ACTIVE_STREAMS entry is cleaned up even if structArena closes before consumer calls release
        return rawStream.reinterpret(
                ArrowCStructs.ARROW_ARRAY_STREAM_LAYOUT.byteSize(),
                structArena,
                _ -> ACTIVE_STREAMS.remove(streamId)
        );
    }

    /**
     * Exports a {@link Table} as an {@code ArrowArrayStream} that yields zero-copy horizontal slices
     * of at most {@code maxBatchRows} rows per batch.
     *
     * @param table        the source table to stream
     * @param maxBatchRows the maximum number of rows per streamed batch (must be {@code > 0})
     * @param structArena  the arena used to allocate stream and batch C struct metadata
     * @return a {@link MemorySegment} pointing to the populated {@code ArrowArrayStream} struct
     * @throws IllegalArgumentException if {@code maxBatchRows <= 0}
     */
    public static MemorySegment exportTableStream(Table table, long maxBatchRows, Arena structArena) {
        Objects.requireNonNull(table, "table cannot be null");
        Objects.requireNonNull(structArena, "structArena cannot be null");
        if (maxBatchRows <= 0) {
            throw new IllegalArgumentException("maxBatchRows must be positive: " + maxBatchRows);
        }

        Iterator<Table> sliceIterator = new Iterator<>() {
            private long cursor = 0L;

            @Override
            public boolean hasNext() {
                return cursor < table.rowCount();
            }

            @Override
            public Table next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                long batchLen = Math.min(maxBatchRows, table.rowCount() - cursor);
                Table slice = table.slice(cursor, batchLen);
                cursor += batchLen;
                return slice;
            }
        };

        return exportTableStream(table.schema(), sliceIterator, structArena);
    }

    /**
     * Exports a single {@link DataBuffer} column into an {@code ArrowArray} C struct.
     * Multi-chunk {@link ChunkedBuffer} instances are automatically compacted into {@code structArena}.
     *
     * @param column      the column buffer to export
     * @param structArena the arena used to allocate the C struct metadata
     * @return a {@link MemorySegment} pointing to the populated {@code ArrowArray} struct
     */
    public static MemorySegment exportArray(DataBuffer column, Arena structArena) {
        Objects.requireNonNull(column, "column cannot be null");
        Objects.requireNonNull(structArena, "structArena cannot be null");

        if (column instanceof ChunkedBuffer cb) {
            column = cb.compact(structArena);
        }

        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_ARRAY_LAYOUT);
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_LENGTH, column.length());
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_NULL_COUNT, column.nullCount());
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_OFFSET, column.offset());
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_BUFFERS, column.bufferCount());
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_CHILDREN, 0L);

        MemorySegment buffersPtrArray = structArena.allocate(ValueLayout.ADDRESS, column.bufferCount());
        for (int i = 0; i < column.bufferCount(); i++) {
            buffersPtrArray.setAtIndex(ValueLayout.ADDRESS, i, column.buffer(i));
        }

        DataBuffer dictBuffer = extractDictionary(column);
        MemorySegment dictArrayPtr = dictBuffer != null
                ? exportArray(dictBuffer, structArena)
                : MemorySegment.NULL;

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_BUFFERS, buffersPtrArray);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_CHILDREN, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_DICTIONARY, dictArrayPtr);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, ARRAY_RELEASE_STUB);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_PRIVATE_DATA, MemorySegment.NULL);

        return struct;
    }

    /**
     * Exports a single {@link DataBuffer} column's metadata into a nullable {@code ArrowSchema} C struct.
     *
     * @param column      the column buffer whose schema should be exported
     * @param structArena the arena used to allocate the C struct metadata
     * @return a {@link MemorySegment} pointing to the populated {@code ArrowSchema} struct
     */
    public static MemorySegment exportSchema(DataBuffer column, Arena structArena) {
        Objects.requireNonNull(column, "column cannot be null");
        Objects.requireNonNull(structArena, "structArena cannot be null");
        return exportFieldSchema(column.name(), column.type(), true, structArena);
    }

    private static MemorySegment exportFieldSchema(String name, DataType type, boolean nullable, Arena structArena) {
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_SCHEMA_LAYOUT);

        boolean isDict = (type == DataType.DICTIONARY);
        String formatString = isDict ? DataType.INT32.getFormat() : type.getFormat();
        MemorySegment dictSchemaPtr = isDict
                ? exportFieldSchema(name + "_dict", DataType.UTF8, false, structArena)
                : MemorySegment.NULL;

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT, structArena.allocateFrom(formatString));
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME, structArena.allocateFrom(name));
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_METADATA, MemorySegment.NULL);
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_FLAGS, nullable ? ARROW_FLAG_NULLABLE : 0L);
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_N_CHILDREN, 0L);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_DICTIONARY, dictSchemaPtr);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE, SCHEMA_RELEASE_STUB);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_PRIVATE_DATA, MemorySegment.NULL);

        return struct;
    }

    private static DataBuffer extractDictionary(DataBuffer column) {
        if (column instanceof DictionaryBuffer db) {
            return db.dictionary();
        }
        if (column instanceof ImportedDataBuffer idb) {
            return idb.dictionary();
        }
        return null;
    }

    /**
     * Native upcall callback for {@code ArrowArrayStream.get_schema}.
     *
     * @param streamPtr    pointer to the {@code ArrowArrayStream} struct
     * @param outSchemaPtr pointer to the output {@code ArrowSchema} struct to populate
     * @return {@code 0} on success, or a POSIX {@code errno} code on failure
     */
    public static int streamGetSchemaCallback(MemorySegment streamPtr, MemorySegment outSchemaPtr) {
        MemorySegment stream = streamPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_STREAM_LAYOUT.byteSize());
        if (stream.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_RELEASE).equals(MemorySegment.NULL)) {
            return ERRNO_EINVAL;
        }
        long streamId = stream.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_PRIVATE_DATA).address();
        ExportedStreamState state = ACTIVE_STREAMS.get(streamId);
        if (state == null) {
            return ERRNO_EINVAL;
        }

        try {
            MemorySegment outSchema = outSchemaPtr.reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
            populateTableSchema(state.schema, outSchema, state.structArena);
            state.lastError = MemorySegment.NULL;
            return 0;
        } catch (Throwable t) {
            state.recordError(t);
            return ERRNO_EIO;
        }
    }

    /**
     * Native upcall callback for {@code ArrowArrayStream.get_next}.
     *
     * @param streamPtr   pointer to the {@code ArrowArrayStream} struct
     * @param outArrayPtr pointer to the output {@code ArrowArray} struct to populate
     * @return {@code 0} on success (or end-of-stream), or a POSIX {@code errno} code on failure
     */
    public static int streamGetNextCallback(MemorySegment streamPtr, MemorySegment outArrayPtr) {
        MemorySegment stream = streamPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_STREAM_LAYOUT.byteSize());
        if (stream.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_RELEASE).equals(MemorySegment.NULL)) {
            return ERRNO_EINVAL;
        }
        long streamId = stream.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_PRIVATE_DATA).address();
        ExportedStreamState state = ACTIVE_STREAMS.get(streamId);
        if (state == null) {
            return ERRNO_EINVAL;
        }

        MemorySegment outArray = outArrayPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize());
        try {
            if (!state.batches.hasNext()) {
                outArray.fill((byte) 0);
                state.lastError = MemorySegment.NULL;
                return 0;
            }

            Table batch = state.batches.next();
            if (batch == null) {
                throw new IllegalStateException("Stream iterator returned a null Table batch.");
            }
            if (!batch.schema().equals(state.schema)) {
                throw new IllegalStateException(
                        "Batch schema " + batch.schema() + " does not match stream schema " + state.schema
                );
            }

            populateTableArray(batch, outArray, state.structArena);
            state.lastError = MemorySegment.NULL;
            return 0;
        } catch (Throwable t) {
            outArray.fill((byte) 0);
            state.recordError(t);
            return ERRNO_EIO;
        }
    }

    /**
     * Native upcall callback for {@code ArrowArrayStream.get_last_error}.
     *
     * @param streamPtr pointer to the {@code ArrowArrayStream} struct
     * @return pointer to a null-terminated UTF-8 error string, or {@link MemorySegment#NULL} if no error occurred
     */
    public static MemorySegment streamGetLastErrorCallback(MemorySegment streamPtr) {
        MemorySegment stream = streamPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_STREAM_LAYOUT.byteSize());
        long streamId = stream.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_PRIVATE_DATA).address();
        ExportedStreamState state = ACTIVE_STREAMS.get(streamId);
        return state != null ? state.lastError : MemorySegment.NULL;
    }

    /**
     * Native upcall callback for {@code ArrowArrayStream.release}.
     *
     * @param streamPtr pointer to the {@code ArrowArrayStream} struct to mark released
     */
    public static void releaseStreamCallback(MemorySegment streamPtr) {
        MemorySegment stream = streamPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_STREAM_LAYOUT.byteSize());
        if (stream.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_RELEASE).equals(MemorySegment.NULL)) {
            return;
        }
        long streamId = stream.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_PRIVATE_DATA).address();
        ACTIVE_STREAMS.remove(streamId);
        stream.set(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_PRIVATE_DATA, MemorySegment.NULL);
        stream.set(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_RELEASE, MemorySegment.NULL);
    }

    /**
     * Native upcall callback for {@code ArrowArray.release}. Recursively releases child and dictionary
     * arrays and sets the struct's {@code release} callback pointer to {@code NULL}.
     *
     * @param structPtr pointer to the {@code ArrowArray} struct to mark released
     */
    public static void releaseArrayCallback(MemorySegment structPtr) {
        MemorySegment struct = structPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize());
        if (struct.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE).equals(MemorySegment.NULL)) {
            return;
        }

        long nChildren = struct.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_CHILDREN);
        MemorySegment childrenPtr = struct.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_CHILDREN);

        if (nChildren > 0 && !childrenPtr.equals(MemorySegment.NULL)) {
            MemorySegment childrenArray = childrenPtr.reinterpret(nChildren * ValueLayout.ADDRESS.byteSize());
            for (long i = 0; i < nChildren; i++) {
                MemorySegment childPtr = childrenArray.getAtIndex(ValueLayout.ADDRESS, i);
                if (!childPtr.equals(MemorySegment.NULL)) {
                    releaseArrayCallback(childPtr);
                }
            }
        }

        MemorySegment dictPtr = struct.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_DICTIONARY);
        if (!dictPtr.equals(MemorySegment.NULL)) {
            releaseArrayCallback(dictPtr);
        }

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, MemorySegment.NULL);
    }

    /**
     * Native upcall callback for {@code ArrowSchema.release}. Recursively releases child and dictionary
     * schemas and sets the struct's {@code release} callback pointer to {@code NULL}.
     *
     * @param structPtr pointer to the {@code ArrowSchema} struct to mark released
     */
    public static void releaseSchemaCallback(MemorySegment structPtr) {
        MemorySegment struct = structPtr.reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
        if (struct.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE).equals(MemorySegment.NULL)) {
            return;
        }

        long nChildren = struct.get(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_N_CHILDREN);
        MemorySegment childrenPtr = struct.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN);

        if (nChildren > 0 && !childrenPtr.equals(MemorySegment.NULL)) {
            MemorySegment childrenArray = childrenPtr.reinterpret(nChildren * ValueLayout.ADDRESS.byteSize());
            for (long i = 0; i < nChildren; i++) {
                MemorySegment childPtr = childrenArray.getAtIndex(ValueLayout.ADDRESS, i);
                if (!childPtr.equals(MemorySegment.NULL)) {
                    releaseSchemaCallback(childPtr);
                }
            }
        }

        MemorySegment dictPtr = struct.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_DICTIONARY);
        if (!dictPtr.equals(MemorySegment.NULL)) {
            releaseSchemaCallback(dictPtr);
        }

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE, MemorySegment.NULL);
    }

    private static final class ExportedStreamState {
        final Schema schema;
        final Iterator<Table> batches;
        final Arena structArena;
        volatile MemorySegment lastError = MemorySegment.NULL;

        ExportedStreamState(Schema schema, Iterator<Table> batches, Arena structArena) {
            this.schema = schema;
            this.batches = batches;
            this.structArena = structArena;
        }

        void recordError(Throwable t) {
            String msg = t.getMessage() != null ? t.getMessage() : t.getClass().getName();
            if (structArena.scope().isAlive()) {
                this.lastError = structArena.allocateFrom(msg);
            }
        }
    }
}