package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.internal.ArrowCStructs;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

public final class ArrowCImporter {

    private ArrowCImporter() {}

    static final MethodHandle NATIVE_RELEASE_CALL = Linker.nativeLinker().downcallHandle(
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
    );

    public static ImportedDataBuffer importBuffer(MemorySegment arrayPtr, MemorySegment schemaPtr) {

        // 1. Read the Schema
        MemorySegment formatPtr = schemaPtr.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT);
        String format = formatPtr.reinterpret(16).getString(0);
        DataType type = DataType.fromFormat(format);

        MemorySegment namePtr = schemaPtr.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME);
        String name = namePtr.equals(MemorySegment.NULL) ? "imported_col" : namePtr.reinterpret(256).getString(0);

        // 2. Read Metadata
        long length = arrayPtr.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_LENGTH);
        long nBuffers = arrayPtr.get(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_BUFFERS);

        // 3. READ THE FOREIGN OFFSET
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

            // We must add the logical offset to the length when safely expanding the MemorySegment
            // bounds, because the data physically starts further down in the memory block.
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

        // 4. Pass the offset into the concrete implementation
        return new ImportedDataBuffer(name, type, length, offset, resolvedBuffers, arrayPtr, releaseFuncPtr);
    }
}