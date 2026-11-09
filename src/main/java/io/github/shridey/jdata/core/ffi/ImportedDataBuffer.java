package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.AbstractDataBuffer;
import io.github.shridey.jdata.core.internal.ArrowCStructs;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A concrete, universal buffer imported from a foreign runtime.
 * Safely holds the memory segments for ANY Arrow data type (Primitives, Strings, etc.)
 * while managing the foreign memory lifecycle via a native C release callback.
 */
public final class ImportedDataBuffer extends AbstractDataBuffer implements AutoCloseable {

    private final DataType type;
    private final MemorySegment[] buffers;
    private final MemorySegment arrayPtr;
    private final MemorySegment releaseFuncPtr;

    ImportedDataBuffer(String name, DataType type, long length,
                       MemorySegment[] buffers, MemorySegment arrayPtr,
                       MemorySegment releaseFuncPtr) {
        // Buffer 0 is always the validity bitmap in Arrow
        super(name, Arena.global(), length, buffers[0]);
        this.type = type;
        this.buffers = buffers;
        this.arrayPtr = arrayPtr;
        this.releaseFuncPtr = releaseFuncPtr;

        // Imported memory comes pre-populated; calculate nulls immediately
        this.nullCount();
    }

    @Override
    public DataType type() {
        return type;
    }

    @Override
    public int bufferCount() {
        return buffers.length;
    }

    @Override
    public MemorySegment buffer(int index) {
        if (index < 0 || index >= buffers.length) {
            throw new IndexOutOfBoundsException("Invalid buffer index: " + index);
        }
        return buffers[index];
    }

    // ==========================================
    // Foreign Memory Lifecycle Management
    // ==========================================

    @Override
    public void close() {
        if (!releaseFuncPtr.equals(MemorySegment.NULL)) {
            try {
                // Dynamically call the C++/Rust release function to trigger memory cleanup
                ArrowCImporter.NATIVE_RELEASE_CALL.invokeExact(releaseFuncPtr, arrayPtr);

                // Set pointer to NULL locally to prevent catastrophic double-freeing
                arrayPtr.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, MemorySegment.NULL);
            } catch (Throwable t) {
                throw new RuntimeException("Failed to invoke native Arrow release callback", t);
            }
        }
    }
}