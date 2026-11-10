package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;
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

    // 1. Add offset to constructor
    ImportedDataBuffer(String name, DataType type, long length, long offset,
                       MemorySegment[] buffers, MemorySegment arrayPtr,
                       MemorySegment releaseFuncPtr) {
        // Pass offset up to the parent validity bitmap manager
        super(name, Arena.global(), length, offset, buffers[0]);
        this.type = type;
        this.buffers = buffers;
        this.arrayPtr = arrayPtr;
        this.releaseFuncPtr = releaseFuncPtr;

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

    @Override
    public Object toBuilder(Arena arena) {
        // Dynamically route the foreign memory to the correct typed builder
        return switch (this.type) {
            case INT32 -> new Int32BufferBuilder(arena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case INT64 -> new Int64BufferBuilder(arena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case FLOAT32 -> new Float32BufferBuilder(arena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case FLOAT64 -> new Float64BufferBuilder(arena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case BOOLEAN -> new BooleanBufferBuilder(arena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case UTF8 -> {
                // Determine raw text footprint by applying the physical offset
                long absoluteEndIndex = this.offset + this.length;
                long startOffsetByteIndex = this.offset * ValueLayout.JAVA_INT.byteSize();
                long endOffsetByteIndex = absoluteEndIndex * ValueLayout.JAVA_INT.byteSize();

                int startByte = this.buffers[1].get(ValueLayout.JAVA_INT, startOffsetByteIndex);
                int endByte = this.buffers[1].get(ValueLayout.JAVA_INT, endOffsetByteIndex);

                yield new UTF8BufferBuilder(arena, this.name, this.length, (endByte - startByte))
                        .appendImportedBlock(this, 0, this.length);
            }
            default -> throw new UnsupportedOperationException("toBuilder() not yet supported for imported type: " + this.type);
        };
    }

    @Override
    public ImportedDataBuffer slice(long sliceOffset, long sliceLength) {
        if (sliceOffset < 0 || sliceLength < 0 || sliceOffset + sliceLength > this.length) {
            throw new IndexOutOfBoundsException("Invalid slice parameters");
        }
        long absoluteOffset = this.offset + sliceOffset;

        // Return a zero-copy view of the foreign memory.
        return new ImportedDataBuffer(this.name, this.type, sliceLength, absoluteOffset,
                this.buffers, this.arrayPtr, this.releaseFuncPtr);
    }

    @Override
    public void close() {
        if (!releaseFuncPtr.equals(MemorySegment.NULL)) {
            try {
                ArrowCImporter.NATIVE_RELEASE_CALL.invokeExact(releaseFuncPtr, arrayPtr);
                arrayPtr.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, MemorySegment.NULL);
            } catch (Throwable t) {
                throw new RuntimeException("Failed to invoke native Arrow release callback", t);
            }
        }
    }
}