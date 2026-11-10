package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.layout.Table;
import io.github.shridey.jdata.core.internal.ArrowCStructs;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

public final class ArrowCExporter {

    private ArrowCExporter() {}

    public static MemorySegment exportTableArray(Table table, Arena structArena) {
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_ARRAY_LAYOUT);

        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_LENGTH, table.rowCount());
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_NULL_COUNT, 0L); // Top-level tables don't have row nulls
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_OFFSET, 0L);

        // A Table (Struct) requires exactly 1 buffer: a validity bitmap. (We set it to NULL).
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_BUFFERS, 1L);
        MemorySegment buffersPtrArray = structArena.allocate(ValueLayout.ADDRESS, 1);
        buffersPtrArray.setAtIndex(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_BUFFERS, buffersPtrArray);

        // Populate the children array with pointers to the individual column Arrays
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
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, createArrayReleaseCallback(structArena));

        return struct;
    }

    public static MemorySegment exportTableSchema(Table table, Arena structArena) {
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_SCHEMA_LAYOUT);

        // Arrow format for a Struct (Table/RecordBatch) is exactly "+s"
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT, structArena.allocateFrom("+s"));
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME, structArena.allocateFrom("table"));
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_FLAGS, 0L);

        // Populate the children array with pointers to the individual column Schemas
        int numCols = table.columnCount();
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_N_CHILDREN, numCols);

        if (numCols > 0) {
            MemorySegment childrenPtrArray = structArena.allocate(ValueLayout.ADDRESS, numCols);
            for (int i = 0; i < numCols; i++) {
                MemorySegment childSchema = exportSchema(table.getColumn(i), structArena);
                childrenPtrArray.setAtIndex(ValueLayout.ADDRESS, i, childSchema);
            }
            struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN, childrenPtrArray);
        } else {
            struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN, MemorySegment.NULL);
        }

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_DICTIONARY, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE, createSchemaReleaseCallback(structArena));

        return struct;
    }

    public static MemorySegment exportArray(DataBuffer column, Arena structArena) {
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

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_BUFFERS, buffersPtrArray);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_CHILDREN, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_DICTIONARY, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, createArrayReleaseCallback(structArena));

        return struct;
    }

    public static MemorySegment exportSchema(DataBuffer column, Arena structArena) {
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_SCHEMA_LAYOUT);
        String formatString = column.type().getFormat();

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT, structArena.allocateFrom(formatString));
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME, structArena.allocateFrom(column.name()));
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_FLAGS, 2L);
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_N_CHILDREN, 0L);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_DICTIONARY, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE, createSchemaReleaseCallback(structArena));

        return struct;
    }

    private static MemorySegment createArrayReleaseCallback(Arena arena) {
        try {
            MethodHandle handle = MethodHandles.lookup().findStatic(
                    ArrowCExporter.class, "releaseArrayCallback",
                    java.lang.invoke.MethodType.methodType(void.class, MemorySegment.class)
            );
            return Linker.nativeLinker().upcallStub(handle, FunctionDescriptor.ofVoid(ValueLayout.ADDRESS), arena);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("Failed to bind Array release callback", e);
        }
    }

    private static MemorySegment createSchemaReleaseCallback(Arena arena) {
        try {
            MethodHandle handle = MethodHandles.lookup().findStatic(
                    ArrowCExporter.class, "releaseSchemaCallback",
                    java.lang.invoke.MethodType.methodType(void.class, MemorySegment.class)
            );
            return Linker.nativeLinker().upcallStub(handle, FunctionDescriptor.ofVoid(ValueLayout.ADDRESS), arena);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("Failed to bind Schema release callback", e);
        }
    }

    public static void releaseArrayCallback(MemorySegment structPtr) {
        MemorySegment struct = structPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize());
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, MemorySegment.NULL);
    }

    public static void releaseSchemaCallback(MemorySegment structPtr) {
        MemorySegment struct = structPtr.reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE, MemorySegment.NULL);
    }
}