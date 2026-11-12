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

public final class ArrowCImporter {

    private ArrowCImporter() {}

    static final MethodHandle NATIVE_RELEASE_CALL = Linker.nativeLinker().downcallHandle(
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
    );

    public static Table importTable(MemorySegment arrayPtr, MemorySegment schemaPtr, Arena arena) {
        Objects.requireNonNull(arrayPtr, "arrayPtr cannot be null");
        Objects.requireNonNull(schemaPtr, "schemaPtr cannot be null");
        Objects.requireNonNull(arena, "arena cannot be null");

        MemorySegment rawArray = arrayPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize());
        MemorySegment schema = schemaPtr.reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
        ensureNotReleased(rawArray, schema);

        MemorySegment array = rawArray.reinterpret(
                ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize(),
                arena,
                ArrowCImporter::releaseArrayIfPresent
        );

        try {
            MemorySegment formatPtr = schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT);
            String format = formatPtr.reinterpret(Long.MAX_VALUE).getString(0);

            if (!"+s".equals(format)) {
                throw new IllegalArgumentException("Expected Struct (+s) format for Table, got: " + format);
            }

            long nChildrenSchema = schema.get(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_N_CHILDREN);
            long nChildrenArray = array.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_CHILDREN);

            if (nChildrenSchema != nChildrenArray) {
                throw new IllegalStateException("Schema and Array children count mismatch");
            }

            long parentLength = array.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_LENGTH);
            long parentOffset = array.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_OFFSET);

            List<Field> fields = new ArrayList<>((int) nChildrenSchema);
            List<DataBuffer> columns = new ArrayList<>((int) nChildrenSchema);

            if (nChildrenSchema > 0) {
                MemorySegment schemaChildrenPtr = schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN)
                        .reinterpret(nChildrenSchema * ValueLayout.ADDRESS.byteSize());
                MemorySegment arrayChildrenPtr = array.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_CHILDREN)
                        .reinterpret(nChildrenArray * ValueLayout.ADDRESS.byteSize());

                for (int i = 0; i < nChildrenSchema; i++) {
                    MemorySegment childSchemaPtr = schemaChildrenPtr.getAtIndex(ValueLayout.ADDRESS, i)
                            .reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
                    MemorySegment childArrayPtr = arrayChildrenPtr.getAtIndex(ValueLayout.ADDRESS, i);

                    long flags = childSchemaPtr.get(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_FLAGS);
                    boolean nullable = (flags & 2L) != 0;

                    ImportedDataBuffer buffer = importBuffer(childArrayPtr, childSchemaPtr, arena);
                    if (parentOffset > 0 || buffer.length() != parentLength) {
                        buffer = buffer.slice(parentOffset, parentLength);
                    }
                    columns.add(buffer);
                    fields.add(new Field(buffer.name(), buffer.type(), nullable));
                }
            }

            return new Table(new Schema(fields), columns);
        } finally {
            releaseSchemaIfPresent(schema);
        }
    }

    public static ImportedDataBuffer importBuffer(MemorySegment arrayPtr, MemorySegment schemaPtr, Arena arena) {
        Objects.requireNonNull(arrayPtr, "arrayPtr cannot be null");
        Objects.requireNonNull(schemaPtr, "schemaPtr cannot be null");
        Objects.requireNonNull(arena, "arena cannot be null");

        MemorySegment rawArray = arrayPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize());
        MemorySegment schema = schemaPtr.reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
        ensureNotReleased(rawArray, schema);

        MemorySegment array = rawArray.reinterpret(
                ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize(),
                arena,
                ArrowCImporter::releaseArrayIfPresent
        );

        try {
            MemorySegment formatPtr = schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT);
            String format = formatPtr.reinterpret(Long.MAX_VALUE).getString(0);
            DataType type = DataType.fromFormat(format);

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
                    if (type == DataType.BOOLEAN) {
                        buffer1Bytes = (totalElements + 7L) >>> 3;
                    } else if (type == DataType.UTF8 || type == DataType.BINARY) {
                        buffer1Bytes = Math.multiplyExact(totalElements + 1L, Integer.BYTES);
                    } else if (type == DataType.LARGE_UTF8 || type == DataType.LARGE_BINARY) {
                        buffer1Bytes = Math.multiplyExact(totalElements + 1L, Long.BYTES);
                    } else if (type.byteSize() > 0) {
                        buffer1Bytes = Math.multiplyExact(totalElements, type.byteSize());
                    } else {
                        buffer1Bytes = Integer.MAX_VALUE;
                    }
                    resolvedBuffers[i] = rawPtr.reinterpret(buffer1Bytes, arena, null);
                } else if (i == 2) {
                    long buffer2Bytes;
                    if ((type == DataType.UTF8 || type == DataType.BINARY) && !resolvedBuffers[1].equals(MemorySegment.NULL)) {
                        int endByte = resolvedBuffers[1].get(ValueLayout.JAVA_INT, totalElements * Integer.BYTES);
                        buffer2Bytes = Math.max(endByte, 0);
                    } else if ((type == DataType.LARGE_UTF8 || type == DataType.LARGE_BINARY) && !resolvedBuffers[1].equals(MemorySegment.NULL)) {
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

            return new ImportedDataBuffer(name, arena, type, length, offset, nullCount, resolvedBuffers);
        } finally {
            releaseSchemaIfPresent(schema);
        }
    }

    private static void ensureNotReleased(MemorySegment array, MemorySegment schema) {
        if (array.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE).equals(MemorySegment.NULL)) {
            throw new IllegalStateException("Cannot import an already released ArrowArray struct.");
        }
        if (schema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE).equals(MemorySegment.NULL)) {
            throw new IllegalStateException("Cannot import an already released ArrowSchema struct.");
        }
    }

    private static void releaseArrayIfPresent(MemorySegment arrayPtr) {
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

    private static void releaseSchemaIfPresent(MemorySegment schema) {
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
}