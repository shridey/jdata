package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A concrete, universal buffer imported from a foreign runtime.
 * Safely holds the memory segments for ANY Arrow data type (Primitives, Strings, Dictionaries, etc.)
 * while delegating the foreign memory lifecycle to the bound Arena.
 */
public final class ImportedDataBuffer extends AbstractDataBuffer {

    private final DataType type;
    private final MemorySegment[] buffers;
    private final ImportedDataBuffer dictionary;

    ImportedDataBuffer(String name, Arena arena, DataType type, long length, long offset,
                       long importedNullCount, MemorySegment[] buffers, ImportedDataBuffer dictionary) {
        super(name, arena, length, offset, buffers.length > 0 ? buffers[0] : MemorySegment.NULL);
        this.type = type;
        this.buffers = buffers;
        this.dictionary = dictionary;

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

    /**
     * Returns the imported dictionary buffer if this buffer is of type {@link DataType#DICTIONARY}, or {@code null} otherwise.
     */
    public ImportedDataBuffer dictionary() {
        return dictionary;
    }

    @Override
    public AbstractBufferBuilder toBuilder(Arena targetArena) {
        return switch (this.type) {
            case INT32 -> new Int32BufferBuilder(targetArena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case INT64 -> new Int64BufferBuilder(targetArena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case FLOAT32 -> new Float32BufferBuilder(targetArena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case FLOAT64 -> new Float64BufferBuilder(targetArena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case BOOLEAN -> new BooleanBufferBuilder(targetArena, this.name, this.length).appendImportedBlock(this, 0, this.length);
            case UTF8 -> {
                long absoluteEndIndex = this.offset + this.length;
                long startOffsetByteIndex = this.offset * ValueLayout.JAVA_INT.byteSize();
                long endOffsetByteIndex = absoluteEndIndex * ValueLayout.JAVA_INT.byteSize();

                int startByte = this.buffers[1].get(ValueLayout.JAVA_INT, startOffsetByteIndex);
                int endByte = this.buffers[1].get(ValueLayout.JAVA_INT, endOffsetByteIndex);

                yield new UTF8BufferBuilder(targetArena, this.name, this.length, endByte - startByte)
                        .appendImportedBlock(this, 0, this.length);
            }
            case DICTIONARY -> new DictionaryBufferBuilder(
                    targetArena, this.name, this.length, (int) Math.max(this.dictionary.length(), 16L)
            ).appendImportedBlock(this, 0, this.length);
            default -> throw new UnsupportedOperationException("toBuilder() not yet supported for imported type: " + this.type);
        };
    }

    @Override
    public ImportedDataBuffer slice(long sliceOffset, long sliceLength) {
        Objects.checkFromIndexSize(sliceOffset, sliceLength, this.length);
        long absoluteOffset = this.offset + sliceOffset;
        long sliceNullCount = (this.nullCount == 0 || sliceLength == 0) ? 0L : -1L;

        return new ImportedDataBuffer(this.name, this.arena, this.type, sliceLength,
                absoluteOffset, sliceNullCount, this.buffers, this.dictionary);
    }

    public int getInt(long index) {
        if (type != DataType.INT32) throw new IllegalStateException("Buffer is not INT32, is " + type);
        checkBounds(index);
        long physicalIndex = offset + index;
        return buffers[1].get(ValueLayout.JAVA_INT, physicalIndex * ValueLayout.JAVA_INT.byteSize());
    }

    public int getIndex(long index) {
        if (type != DataType.DICTIONARY) throw new IllegalStateException("Buffer is not DICTIONARY, is " + type);
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

    public boolean getBoolean(long index) {
        if (type != DataType.BOOLEAN) throw new IllegalStateException("Buffer is not BOOLEAN, is " + type);
        checkBounds(index);

        long physicalIndex = offset + index;
        long byteIndex = physicalIndex >>> 3;
        int bitOffset = (int) (physicalIndex & 7L);

        byte b = buffers[1].get(ValueLayout.JAVA_BYTE, byteIndex);
        return ((b >>> bitOffset) & 1) == 1;
    }

    public String getString(long index) {
        if (type == DataType.DICTIONARY) {
            checkBounds(index);
            if (isNull(index)) {
                return null;
            }
            return dictionary.getString(getIndex(index));
        }

        if (type != DataType.UTF8) throw new IllegalStateException("Buffer is not UTF8 or DICTIONARY, is " + type);
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
}