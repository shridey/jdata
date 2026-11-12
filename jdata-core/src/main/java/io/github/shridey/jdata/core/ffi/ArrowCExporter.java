package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.DictionaryBuffer;
import io.github.shridey.jdata.core.internal.ArrowCStructs;
import io.github.shridey.jdata.core.layout.Table;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

public final class ArrowCExporter {

    private static final long ARROW_FLAG_NULLABLE = 2L;

    private static final MemorySegment ARRAY_RELEASE_STUB;
    private static final MemorySegment SCHEMA_RELEASE_STUB;

    static {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Linker linker = Linker.nativeLinker();
            FunctionDescriptor descriptor = FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

            MethodHandle arrayReleaseHandle = lookup.findStatic(
                    ArrowCExporter.class, "releaseArrayCallback",
                    MethodType.methodType(void.class, MemorySegment.class)
            );
            MethodHandle schemaReleaseHandle = lookup.findStatic(
                    ArrowCExporter.class, "releaseSchemaCallback",
                    MethodType.methodType(void.class, MemorySegment.class)
            );

            ARRAY_RELEASE_STUB = linker.upcallStub(arrayReleaseHandle, descriptor, Arena.global());
            SCHEMA_RELEASE_STUB = linker.upcallStub(schemaReleaseHandle, descriptor, Arena.global());
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private ArrowCExporter() {}

    public static MemorySegment exportTableArray(Table table, Arena structArena) {
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_ARRAY_LAYOUT);

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

        return struct;
    }

    public static MemorySegment exportTableSchema(Table table, Arena structArena) {
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_SCHEMA_LAYOUT);

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT, structArena.allocateFrom("+s"));
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME, structArena.allocateFrom("table"));
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_METADATA, MemorySegment.NULL);
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_FLAGS, 0L);

        int numCols = table.columnCount();
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_N_CHILDREN, numCols);

        if (numCols > 0) {
            MemorySegment childrenPtrArray = structArena.allocate(ValueLayout.ADDRESS, numCols);
            for (int i = 0; i < numCols; i++) {
                boolean nullable = table.schema().getField(i).nullable();
                MemorySegment childSchema = exportSchema(table.getColumn(i), nullable, structArena);
                childrenPtrArray.setAtIndex(ValueLayout.ADDRESS, i, childSchema);
            }
            struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN, childrenPtrArray);
        } else {
            struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN, MemorySegment.NULL);
        }

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_DICTIONARY, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE, SCHEMA_RELEASE_STUB);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_PRIVATE_DATA, MemorySegment.NULL);

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

    public static MemorySegment exportSchema(DataBuffer column, Arena structArena) {
        return exportSchema(column, true, structArena);
    }

    private static MemorySegment exportSchema(DataBuffer column, boolean nullable, Arena structArena) {
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_SCHEMA_LAYOUT);

        DataBuffer dictBuffer = extractDictionary(column);
        // Per Arrow C Data spec: a dictionary-encoded schema's format is its index integer type ("i" for INT32)
        String formatString = dictBuffer != null
                ? DataType.INT32.getFormat()
                : column.type().getFormat();
        MemorySegment dictSchemaPtr = dictBuffer != null
                ? exportSchema(dictBuffer, false, structArena)
                : MemorySegment.NULL;

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT, structArena.allocateFrom(formatString));
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME, structArena.allocateFrom(column.name()));
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
}