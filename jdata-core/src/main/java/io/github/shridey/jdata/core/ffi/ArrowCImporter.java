package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Imports native Apache Arrow C Data Interface ({@code ArrowArray}, {@code ArrowSchema}) and
 * Apache Arrow C Stream Interface ({@code ArrowArrayStream}) structs into {@link Table},
 * {@link ImportedDataBuffer}, and {@link ArrowTableStream} instances bound to a Java {@link Arena}.
 */
public final class ArrowCImporter {

    private ArrowCImporter() {}

    static final MethodHandle NATIVE_RELEASE_CALL = Linker.nativeLinker().downcallHandle(
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
    );

    static final MethodHandle STREAM_GET_SCHEMA_CALL = Linker.nativeLinker().downcallHandle(
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
    );

    static final MethodHandle STREAM_GET_NEXT_CALL = Linker.nativeLinker().downcallHandle(
            FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS)
    );

    static final MethodHandle STREAM_GET_LAST_ERROR_CALL = Linker.nativeLinker().downcallHandle(
            FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS)
    );

    /**
     * Imports a foreign {@code ArrowArrayStream} C struct into a pull-based {@link ArrowTableStream}.
     *
     * @param streamPtr   pointer to the native {@code ArrowArrayStream} struct
     * @param streamArena the arena governing the stream's lifecycle
     * @return a pull-based {@link ArrowTableStream} consumer
     * @throws IllegalStateException if the stream is already released or {@code get_schema} fails
     */
    public static ArrowTableStream importTableStream(MemorySegment streamPtr, Arena streamArena) {
        Objects.requireNonNull(streamPtr, "streamPtr cannot be null");
        Objects.requireNonNull(streamArena, "streamArena cannot be null");

        MemorySegment rawStream = streamPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_STREAM_LAYOUT.byteSize());
        if (rawStream.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_RELEASE).equals(MemorySegment.NULL)) {
            throw new IllegalStateException("Cannot import an already released ArrowArrayStream struct.");
        }

        MemorySegment boundStream = rawStream.reinterpret(
                ArrowCStructs.ARROW_ARRAY_STREAM_LAYOUT.byteSize(),
                streamArena,
                ArrowCImporter::releaseStreamIfPresent
        );

        MemorySegment rawSchema = streamArena.allocate(ArrowCStructs.ARROW_SCHEMA_LAYOUT);
        MemorySegment getSchemaPtr = boundStream.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_GET_SCHEMA);

        int errCode;
        try {
            errCode = (int) STREAM_GET_SCHEMA_CALL.invokeExact(getSchemaPtr, boundStream, rawSchema);
        } catch (Throwable t) {
            releaseStreamIfPresent(boundStream);
            throw new RuntimeException("Failed to invoke native ArrowArrayStream.get_schema callback", t);
        }

        if (errCode != 0) {
            releaseStreamIfPresent(boundStream);
            throw new IllegalStateException("ArrowArrayStream.get_schema failed with error code: " + errCode);
        }

        MemorySegment heldSchema = rawSchema.reinterpret(
                ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize(),
                streamArena,
                ArrowCImporter::releaseSchemaIfPresent
        );

        try {
            Schema parsedSchema = parseTableSchema(heldSchema);
            return new ArrowTableStream(boundStream, streamArena, heldSchema, parsedSchema);
        } catch (RuntimeException e) {
            releaseSchemaIfPresent(heldSchema);
            releaseStreamIfPresent(boundStream);
            throw e;
        }
    }

    /**
     * Imports a foreign top-level Struct ({@code +s}) {@code ArrowArray} and {@code ArrowSchema} pair
     * into a zero-copy {@link Table} bound to {@code arena}. Immediately releases the consumed
     * {@code ArrowSchema} struct and registers the {@code ArrowArray} release callback on {@code arena}.
     *
     * @param arrayPtr  pointer to the native {@code ArrowArray} struct
     * @param schemaPtr pointer to the native {@code ArrowSchema} struct
     * @param arena     the arena governing the imported table's off-heap memory lifecycle
     * @return the imported zero-copy {@link Table}
     * @throws IllegalStateException    if either struct is already released or child counts mismatch
     * @throws IllegalArgumentException if the top-level schema format is not {@code "+s"}
     */
    public static Table importTable(MemorySegment arrayPtr, MemorySegment schemaPtr, Arena arena) {
        Objects.requireNonNull(arrayPtr, "arrayPtr cannot be null");
        Objects.requireNonNull(schemaPtr, "schemaPtr cannot be null");
        Objects.requireNonNull(arena, "arena cannot be null");

        MemorySegment rawArray = arrayPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize());
        MemorySegment schema = schemaPtr.reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
        ensureNotReleased(rawArray, schema);

        try {
            Schema parsedSchema = parseTableSchema(schema);
            return importTableFromHeldSchema(rawArray, schema, parsedSchema, arena);
        } finally {
            releaseSchemaIfPresent(schema);
        }
    }

    static Schema parseTableSchema(MemorySegment schema) {
        MemorySegment formatPtr = schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT);
        String format = formatPtr.reinterpret(Long.MAX_VALUE).getString(0);

        if (!"+s".equals(format)) {
            throw new IllegalArgumentException("Expected Struct (+s) format for Table, got: " + format);
        }

        long nChildrenSchema = schema.get(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_N_CHILDREN);
        List<Field> fields = new ArrayList<>((int) nChildrenSchema);

        if (nChildrenSchema > 0) {
            MemorySegment schemaChildrenPtr = schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN)
                    .reinterpret(nChildrenSchema * ValueLayout.ADDRESS.byteSize());

            for (int i = 0; i < nChildrenSchema; i++) {
                MemorySegment childSchema = schemaChildrenPtr.getAtIndex(ValueLayout.ADDRESS, i)
                        .reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());

                MemorySegment namePtr = childSchema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME);
                String name = namePtr.equals(MemorySegment.NULL)
                        ? "imported_col_" + i
                        : namePtr.reinterpret(Long.MAX_VALUE).getString(0);

                MemorySegment childFormatPtr = childSchema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT);
                String childFormat = childFormatPtr.reinterpret(Long.MAX_VALUE).getString(0);
                DataType physicalType = DataType.fromFormat(childFormat);

                MemorySegment dictSchemaPtr = childSchema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_DICTIONARY);
                DataType logicalType = dictSchemaPtr.equals(MemorySegment.NULL) ? physicalType : DataType.DICTIONARY;

                long flags = childSchema.get(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_FLAGS);
                boolean nullable = (flags & 2L) != 0;

                fields.add(new Field(name, logicalType, nullable));
            }
        }
        return new Schema(fields);
    }

    static Table importTableFromHeldSchema(MemorySegment rawArray, MemorySegment schema,
                                           Schema parsedSchema, Arena batchArena) {
        MemorySegment array = rawArray.reinterpret(
                ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize(),
                batchArena,
                ArrowCImporter::releaseArrayIfPresent
        );

        long nChildrenSchema = parsedSchema.columnCount();
        long nChildrenArray = array.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_CHILDREN);

        if (nChildrenSchema != nChildrenArray) {
            throw new IllegalStateException("Schema and Array children count mismatch");
        }

        long parentLength = array.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_LENGTH);
        long parentOffset = array.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_OFFSET);

        List<DataBuffer> columns = new ArrayList<>((int) nChildrenSchema);

        if (nChildrenSchema > 0) {
            MemorySegment schemaChildrenPtr = schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN)
                    .reinterpret(nChildrenSchema * ValueLayout.ADDRESS.byteSize());
            MemorySegment arrayChildrenPtr = array.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_CHILDREN)
                    .reinterpret(nChildrenArray * ValueLayout.ADDRESS.byteSize());

            for (int i = 0; i < nChildrenSchema; i++) {
                MemorySegment childSchema = schemaChildrenPtr.getAtIndex(ValueLayout.ADDRESS, i)
                        .reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
                MemorySegment childArrayPtr = arrayChildrenPtr.getAtIndex(ValueLayout.ADDRESS, i);

                ImportedDataBuffer buffer = importBufferFromHeldSchema(childArrayPtr, childSchema, batchArena);
                if (parentOffset > 0 || buffer.length() != parentLength) {
                    buffer = buffer.slice(parentOffset, parentLength);
                }
                columns.add(buffer);
            }
        }

        return new Table(parsedSchema, columns);
    }

    /**
     * Imports a single foreign column {@code ArrowArray} and {@code ArrowSchema} pair into an
     * {@link ImportedDataBuffer} bound to {@code arena}. Immediately releases the consumed
     * {@code ArrowSchema} struct and registers the {@code ArrowArray} release callback on {@code arena}.
     *
     * @param arrayPtr  pointer to the native {@code ArrowArray} struct
     * @param schemaPtr pointer to the native {@code ArrowSchema} struct
     * @param arena     the arena governing the imported buffer's off-heap memory lifecycle
     * @return the imported zero-copy {@link ImportedDataBuffer}
     * @throws IllegalStateException if either struct is already released
     */
    public static ImportedDataBuffer importBuffer(MemorySegment arrayPtr, MemorySegment schemaPtr, Arena arena) {
        Objects.requireNonNull(arrayPtr, "arrayPtr cannot be null");
        Objects.requireNonNull(schemaPtr, "schemaPtr cannot be null");
        Objects.requireNonNull(arena, "arena cannot be null");

        MemorySegment rawArray = arrayPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize());
        MemorySegment schema = schemaPtr.reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
        ensureNotReleased(rawArray, schema);

        try {
            return importBufferFromHeldSchema(rawArray, schema, arena);
        } finally {
            releaseSchemaIfPresent(schema);
        }
    }

    private static ImportedDataBuffer importBufferFromHeldSchema(MemorySegment arrayPtr,
                                                                 MemorySegment schema,
                                                                 Arena arena) {
        MemorySegment array = arrayPtr.reinterpret(
                ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize(),
                arena,
                ArrowCImporter::releaseArrayIfPresent
        );

        MemorySegment formatPtr = schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT);
        String format = formatPtr.reinterpret(Long.MAX_VALUE).getString(0);
        DataType physicalType = DataType.fromFormat(format);

        MemorySegment namePtr = schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME);
        String name = namePtr.equals(MemorySegment.NULL)
                ? "imported_col"
                : namePtr.reinterpret(Long.MAX_VALUE).getString(0);

        long length = array.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_LENGTH);
        long nullCount = array.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_NULL_COUNT);
        long offset = array.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_OFFSET);
        long nBuffers = array.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_BUFFERS);
        long totalElements = Math.addExact(length, offset);

        MemorySegment rawBuffersPtr = array.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_BUFFERS);
        MemorySegment buffersArrayPtr = rawBuffersPtr.reinterpret(nBuffers * ValueLayout.ADDRESS.byteSize());
        MemorySegment[] resolvedBuffers = new MemorySegment[(int) nBuffers];

        for (int i = 0; i < nBuffers; i++) {
            MemorySegment rawPtr = buffersArrayPtr.getAtIndex(ValueLayout.ADDRESS, i);

            if (rawPtr.equals(MemorySegment.NULL)) {
                resolvedBuffers[i] = MemorySegment.NULL;
                continue;
            }

            if (i == 0) {
                long bitmapBytes = (totalElements + 7L) >>> 3;
                resolvedBuffers[i] = rawPtr.reinterpret(bitmapBytes, arena, null);
            } else if (i == 1) {
                long buffer1Bytes;
                if (physicalType == DataType.BOOLEAN) {
                    buffer1Bytes = (totalElements + 7L) >>> 3;
                } else if (physicalType == DataType.UTF8 || physicalType == DataType.BINARY) {
                    buffer1Bytes = Math.multiplyExact(totalElements + 1L, Integer.BYTES);
                } else if (physicalType == DataType.LARGE_UTF8 || physicalType == DataType.LARGE_BINARY) {
                    buffer1Bytes = Math.multiplyExact(totalElements + 1L, Long.BYTES);
                } else if (physicalType.byteSize() > 0) {
                    buffer1Bytes = Math.multiplyExact(totalElements, physicalType.byteSize());
                } else {
                    buffer1Bytes = Integer.MAX_VALUE;
                }
                resolvedBuffers[i] = rawPtr.reinterpret(buffer1Bytes, arena, null);
            } else if (i == 2) {
                long buffer2Bytes;
                if ((physicalType == DataType.UTF8 || physicalType == DataType.BINARY) && !resolvedBuffers[1].equals(MemorySegment.NULL)) {
                    int endByte = resolvedBuffers[1].get(ValueLayout.JAVA_INT, totalElements * Integer.BYTES);
                    buffer2Bytes = Math.max(endByte, 0);
                } else if ((physicalType == DataType.LARGE_UTF8 || physicalType == DataType.LARGE_BINARY) && !resolvedBuffers[1].equals(MemorySegment.NULL)) {
                    long endByte = resolvedBuffers[1].get(ValueLayout.JAVA_LONG, totalElements * Long.BYTES);
                    buffer2Bytes = Math.max(endByte, 0L);
                } else {
                    buffer2Bytes = Integer.MAX_VALUE;
                }
                resolvedBuffers[i] = rawPtr.reinterpret(buffer2Bytes, arena, null);
            } else {
                resolvedBuffers[i] = rawPtr.reinterpret(Integer.MAX_VALUE, arena, null);
            }
        }

        MemorySegment dictSchemaPtr = schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_DICTIONARY);
        MemorySegment dictArrayPtr = array.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_DICTIONARY);

        if (!dictSchemaPtr.equals(MemorySegment.NULL)) {
            if (dictArrayPtr.equals(MemorySegment.NULL)) {
                throw new IllegalStateException("ArrowSchema specifies a dictionary, but ArrowArray.dictionary is NULL.");
            }
            if (physicalType != DataType.INT32) {
                throw new UnsupportedOperationException("Only INT32 dictionary indices are supported, got: " + physicalType);
            }
            MemorySegment dictSchema = dictSchemaPtr.reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
            ImportedDataBuffer dictionaryBuffer = importBufferFromHeldSchema(dictArrayPtr, dictSchema, arena);
            return new ImportedDataBuffer(
                    name, arena, DataType.DICTIONARY, length, offset, nullCount, resolvedBuffers, dictionaryBuffer
            );
        }

        return new ImportedDataBuffer(name, arena, physicalType, length, offset, nullCount, resolvedBuffers, null);
    }

    private static void ensureNotReleased(MemorySegment array, MemorySegment schema) {
        if (array.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE).equals(MemorySegment.NULL)) {
            throw new IllegalStateException("Cannot import an already released ArrowArray struct.");
        }
        if (schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE).equals(MemorySegment.NULL)) {
            throw new IllegalStateException("Cannot import an already released ArrowSchema struct.");
        }
    }

    static void releaseArrayIfPresent(MemorySegment arrayPtr) {
        MemorySegment array = arrayPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize());
        MemorySegment releasePtr = array.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE);
        if (!releasePtr.equals(MemorySegment.NULL)) {
            try {
                NATIVE_RELEASE_CALL.invokeExact(releasePtr, array);
                array.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, MemorySegment.NULL);
            } catch (Throwable t) {
                throw new RuntimeException("Failed to invoke native ArrowArray release callback", t);
            }
        }
    }

    static void releaseSchemaIfPresent(MemorySegment schema) {
        MemorySegment releasePtr = schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE);
        if (!releasePtr.equals(MemorySegment.NULL)) {
            try {
                NATIVE_RELEASE_CALL.invokeExact(releasePtr, schema);
                schema.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE, MemorySegment.NULL);
            } catch (Throwable t) {
                throw new RuntimeException("Failed to invoke native ArrowSchema release callback", t);
            }
        }
    }

    static void releaseStreamIfPresent(MemorySegment streamPtr) {
        MemorySegment stream = streamPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_STREAM_LAYOUT.byteSize());
        MemorySegment releasePtr = stream.get(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_RELEASE);
        if (!releasePtr.equals(MemorySegment.NULL)) {
            try {
                NATIVE_RELEASE_CALL.invokeExact(releasePtr, stream);
                stream.set(ValueLayout.ADDRESS, ArrowCStructs.STREAM_OFFSET_RELEASE, MemorySegment.NULL);
            } catch (Throwable t) {
                throw new RuntimeException("Failed to invoke native ArrowArrayStream release callback", t);
            }
        }
    }
}