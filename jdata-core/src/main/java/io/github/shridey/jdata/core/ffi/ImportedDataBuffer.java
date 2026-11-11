package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;
import io.github.shridey.jdata.core.internal.ArrowCStructs;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

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

    ImportedDataBuffer(String name, DataType type, long length, long offset, long importedNullCount,
                       MemorySegment[] buffers, MemorySegment arrayPtr,
                       MemorySegment releaseFuncPtr) {
        super(name, Arena.global(), length, offset, buffers.length > 0 ? buffers[0] : MemorySegment.NULL);
        this.type = type;
        this.buffers = buffers;
        this.arrayPtr = arrayPtr;
        this.releaseFuncPtr = releaseFuncPtr;

        if (importedNullCount >= 0) {
            this.nullCount = importedNullCount;
        } else {
            this.nullCount();
        }
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
    public AbstractBufferBuilder toBuilder(Arena arena) {
        return switch (this.type) {
            case INT32 -> new Int32BufferBuilder(arena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case INT64 -> new Int64BufferBuilder(arena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case FLOAT32 -> new Float32BufferBuilder(arena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case FLOAT64 -> new Float64BufferBuilder(arena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case BOOLEAN -> new BooleanBufferBuilder(arena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case UTF8 -> {
                long absoluteEndIndex = this.offset + this.length;
                long startOffsetByteIndex = this.offset * ValueLayout.JAVA_INT.byteSize();
                long endOffsetByteIndex = absoluteEndIndex * ValueLayout.JAVA_INT.byteSize();

                int startByte = this.buffers[1].get(ValueLayout.JAVA_INT, startOffsetByteIndex);
                int endByte = this.buffers[1].get(ValueLayout.JAVA_INT, endOffsetByteIndex);

                yield new UTF8BufferBuilder(arena, this.name, this.length, endByte - startByte)
                        .appendImportedBlock(this, 0, this.length);
            }
            default -> throw new UnsupportedOperationException("toBuilder() not yet supported for imported type: " + this.type);
        };
    }

    @Override
    public ImportedDataBuffer slice(long sliceOffset, long sliceLength) {
        Objects.checkFromIndexSize(sliceOffset, sliceLength, this.length);
        long absoluteOffset = this.offset + sliceOffset;
        long sliceNullCount = (this.nullCount == 0 || sliceLength == 0) ? 0L : -1L;

        // Slices are non-owning views: pass NULL for arrayPtr and releaseFuncPtr so closing a slice does not free the parent.
        return new ImportedDataBuffer(this.name, this.type, sliceLength, absoluteOffset, sliceNullCount,
                this.buffers, MemorySegment.NULL, MemorySegment.NULL);
    }

    public int getInt(long index) {
        if (type != DataType.INT32) throw new IllegalStateException("Buffer is not INT32, is " + type);
        checkBounds(index);
        long physicalIndex = offset + index;
        return buffers[1].get(ValueLayout.JAVA_INT, physicalIndex * ValueLayout.JAVA_INT.byteSize());
    }

    public double getDouble(long index) {
        if (type != DataType.FLOAT64) throw new IllegalStateException("Buffer is not FLOAT64, is " + type);
        checkBounds(index);
        long physicalIndex = offset + index;
        return buffers[1].get(ValueLayout.JAVA_DOUBLE, physicalIndex * ValueLayout.JAVA_DOUBLE.byteSize());
    }

    public long getLong(long index) {
        if (type != DataType.INT64) throw new IllegalStateException("Buffer is not INT64, is " + type);
        checkBounds(index);
        long physicalIndex = offset + index;
        return buffers[1].get(ValueLayout.JAVA_LONG, physicalIndex * ValueLayout.JAVA_LONG.byteSize());
    }

    public float getFloat(long index) {
        if (type != DataType.FLOAT32) throw new IllegalStateException("Buffer is not FLOAT32, is " + type);
        checkBounds(index);
        long physicalIndex = offset + index;
        return buffers[1].get(ValueLayout.JAVA_FLOAT, physicalIndex * ValueLayout.JAVA_FLOAT.byteSize());
    }

    /**
     * Retrieves an imported boolean value at the specified index, respecting the logical offset and bit-packing.
     */
    public boolean getBoolean(long index) {
        if (type != DataType.BOOLEAN) throw new IllegalStateException("Buffer is not BOOLEAN, is " + type);
        checkBounds(index);

        long physicalIndex = offset + index;
        long byteIndex = physicalIndex >>> 3;
        int bitOffset = (int) (physicalIndex & 7L);

        byte b = buffers[1].get(ValueLayout.JAVA_BYTE, byteIndex);
        return ((b >>> bitOffset) & 1) == 1;
    }

    /**
     * Retrieves an imported UTF-8 string value at the specified index, respecting the logical offset.
     */
    public String getString(long index) {
        if (type != DataType.UTF8) throw new IllegalStateException("Buffer is not UTF8, is " + type);
        checkBounds(index);
        if (isNull(index)) {
            return null;
        }

        long physicalIndex = offset + index;
        long startOffsetByteIndex = physicalIndex * ValueLayout.JAVA_INT.byteSize();

        MemorySegment offsetsBuffer = buffers[1];
        MemorySegment dataBuffer = buffers[2];

        int startByteOffset = offsetsBuffer.get(ValueLayout.JAVA_INT, startOffsetByteIndex);
        int endByteOffset = offsetsBuffer.get(ValueLayout.JAVA_INT, startOffsetByteIndex + ValueLayout.JAVA_INT.byteSize());

        int stringLength = endByteOffset - startByteOffset;
        if (stringLength <= 0) {
            return "";
        }

        byte[] bytes = new byte[stringLength];
        MemorySegment.copy(dataBuffer, ValueLayout.JAVA_BYTE, startByteOffset, bytes, 0, stringLength);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        if (!arrayPtr.equals(MemorySegment.NULL) && !releaseFuncPtr.equals(MemorySegment.NULL)) {
            MemorySegment liveRelease = arrayPtr.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE);
            if (!liveRelease.equals(MemorySegment.NULL)) {
                try {
                    ArrowCImporter.NATIVE_RELEASE_CALL.invokeExact(liveRelease, arrayPtr);
                    arrayPtr.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, MemorySegment.NULL);
                } catch (Throwable t) {
                    throw new RuntimeException("Failed to invoke native Arrow release callback", t);
                }
            }
        }
    }
}