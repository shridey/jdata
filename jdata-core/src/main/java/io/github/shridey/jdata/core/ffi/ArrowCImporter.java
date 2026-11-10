package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.layout.Field;
import io.github.shridey.jdata.core.layout.Schema;
import io.github.shridey.jdata.core.layout.Table;
import io.github.shridey.jdata.core.internal.ArrowCStructs;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

public final class ArrowCImporter {

    private ArrowCImporter() {}

    static final MethodHandle NATIVE_RELEASE_CALL = Linker.nativeLinker().downcallHandle(
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
    );

    public static Table importTable(MemorySegment arrayPtr, MemorySegment schemaPtr) {
        MemorySegment formatPtr = schemaPtr.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT);
        String format = formatPtr.reinterpret(16).getString(0);

        if (!"+s".equals(format)) {
            throw new IllegalArgumentException("Expected Struct (+s) format for Table, got: " + format);
        }

        long nChildrenSchema = schemaPtr.get(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_N_CHILDREN);
        long nChildrenArray = arrayPtr.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_CHILDREN);

        if (nChildrenSchema != nChildrenArray) {
            throw new IllegalStateException("Schema and Array children count mismatch");
        }

        MemorySegment schemaChildrenPtr = schemaPtr.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN)
                .reinterpret(nChildrenSchema * ValueLayout.ADDRESS.byteSize());
        MemorySegment arrayChildrenPtr = arrayPtr.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_CHILDREN)
                .reinterpret(nChildrenArray * ValueLayout.ADDRESS.byteSize());

        List<Field> fields = new ArrayList<>();
        List<DataBuffer> columns = new ArrayList<>();

        for (int i = 0; i < nChildrenSchema; i++) {
            // 1. REINTERPRET THE CHILD POINTERS TO GIVE THEM VALID BYTE SIZES
            MemorySegment childSchemaPtr = schemaChildrenPtr.getAtIndex(ValueLayout.ADDRESS, i)
                    .reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
            MemorySegment childArrayPtr = arrayChildrenPtr.getAtIndex(ValueLayout.ADDRESS, i)
                    .reinterpret(ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize());

            // 2. Import the child column buffers safely
            ImportedDataBuffer buffer = importBuffer(childArrayPtr, childSchemaPtr);
            columns.add(buffer);

            // 3. Read the nullable flag safely now that bounds are established
            long flags = childSchemaPtr.get(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_FLAGS);
            boolean nullable = (flags & 2L) != 0;

            fields.add(new Field(buffer.name(), buffer.type(), nullable));
        }

        return new Table(new Schema(fields), columns);
    }

    public static ImportedDataBuffer importBuffer(MemorySegment arrayPtr, MemorySegment schemaPtr) {
        MemorySegment formatPtr = schemaPtr.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT);
        String format = formatPtr.reinterpret(16).getString(0);
        DataType type = DataType.fromFormat(format);

        MemorySegment namePtr = schemaPtr.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME);
        String name = namePtr.equals(MemorySegment.NULL) ? "imported_col" : namePtr.reinterpret(256).getString(0);

        long length = arrayPtr.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_LENGTH);
        long nBuffers = arrayPtr.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_BUFFERS);
        long offset = arrayPtr.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_OFFSET);

        MemorySegment rawBuffersPtr = arrayPtr.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_BUFFERS);
        MemorySegment buffersArrayPtr = rawBuffersPtr.reinterpret(nBuffers * ValueLayout.ADDRESS.byteSize());
        MemorySegment[] resolvedBuffers = new MemorySegment[(int) nBuffers];

        for (int i = 0; i < nBuffers; i++) {
            MemorySegment rawPtr = buffersArrayPtr.getAtIndex(ValueLayout.ADDRESS, i);

            if (rawPtr.equals(MemorySegment.NULL)) {
                resolvedBuffers[i] = MemorySegment.NULL;
                continue;
            }

            if (i == 0) {
                long bitmapBytes = (length + offset + 7) / 8;
                resolvedBuffers[i] = rawPtr.reinterpret(bitmapBytes, Arena.global(), null);
            } else if (i == 1) {
                if (type == DataType.FLOAT64 || type == DataType.INT64 || type == DataType.DATE64) {
                    resolvedBuffers[i] = rawPtr.reinterpret((length + offset) * 8, Arena.global(), null);
                } else if (type == DataType.INT32 || type == DataType.FLOAT32 || type == DataType.DATE32) {
                    resolvedBuffers[i] = rawPtr.reinterpret((length + offset) * 4, Arena.global(), null);
                } else {
                    resolvedBuffers[i] = rawPtr.reinterpret(Integer.MAX_VALUE, Arena.global(), null);
                }
            } else {
                resolvedBuffers[i] = rawPtr.reinterpret(Integer.MAX_VALUE, Arena.global(), null);
            }
        }

        MemorySegment releaseFuncPtr = arrayPtr.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE);
        return new ImportedDataBuffer(name, type, length, offset, resolvedBuffers, arrayPtr, releaseFuncPtr);
    }
}