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

    // Package-private so ImportedDataBuffer can access it during close()
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

        // 3. Extract and Resize Buffer Pointers Dynamically
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
                // Buffer 0 is always the validity bitmap (1 bit per element)
                long bitmapBytes = (length + 7) / 8;
                resolvedBuffers[i] = rawPtr.reinterpret(bitmapBytes, Arena.global(), null);
            } else if (i == 1) {
                // Buffer 1 is the primary data buffer
                if (type == DataType.FLOAT64 || type == DataType.INT64 || type == DataType.DATE64) {
                    resolvedBuffers[i] = rawPtr.reinterpret(length * 8, Arena.global(), null);
                } else if (type == DataType.INT32 || type == DataType.FLOAT32 || type == DataType.DATE32) {
                    resolvedBuffers[i] = rawPtr.reinterpret(length * 4, Arena.global(), null);
                } else {
                    resolvedBuffers[i] = rawPtr.reinterpret(Integer.MAX_VALUE, Arena.global(), null);
                }
            } else {
                // Buffer 2+ (e.g., Offsets or string character data for UTF8).
                resolvedBuffers[i] = rawPtr.reinterpret(Integer.MAX_VALUE, Arena.global(), null);
            }
        }

        // 4. Extract Callback
        MemorySegment releaseFuncPtr = arrayPtr.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE);

        // 5. Return the concrete universal implementation
        return new ImportedDataBuffer(name, type, length, resolvedBuffers, arrayPtr, releaseFuncPtr);
    }
}