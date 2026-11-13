package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A concrete, universal buffer imported from a foreign runtime via the Apache Arrow C Data Interface.
 * Safely holds the memory segments for any Arrow data type (primitives, booleans, strings, dictionaries)
 * while delegating the foreign memory lifecycle to the bound {@link Arena}.
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
     * Returns the imported dictionary buffer if this buffer is of type {@link DataType#DICTIONARY},
     * or {@code null} otherwise.
     *
     * @return the child dictionary {@link ImportedDataBuffer}, or {@code null}
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

    @Override
    public ImportedDataBuffer withName(String newName) {
        Objects.requireNonNull(newName, "newName cannot be null");
        return new ImportedDataBuffer(newName, this.arena, this.type, this.length,
                this.offset, this.nullCount, this.buffers, this.dictionary);
    }

    @Override
    public DataBuffer take(Int32Buffer indices, Arena targetArena) {
        Objects.requireNonNull(indices, "indices cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");
        return this.toBuilder(targetArena).build().take(indices, targetArena);
    }

    /**
     * Reads the 32-bit signed integer at the given logical index.
     *
     * @param index the zero-based logical index
     * @return the {@code int} value at {@code index}
     * @throws IllegalStateException     if this buffer is not of type {@link DataType#INT32}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public int getInt(long index) {
        if (type != DataType.INT32) throw new IllegalStateException("Buffer is not INT32, is " + type);
        checkBounds(index);
        long physicalIndex = offset + index;
        return buffers[1].get(ValueLayout.JAVA_INT, physicalIndex * ValueLayout.JAVA_INT.byteSize());
    }

    /**
     * Reads the 32-bit dictionary index code at the given logical index.
     *
     * @param index the zero-based logical index
     * @return the 32-bit dictionary index code at {@code index}
     * @throws IllegalStateException     if this buffer is not of type {@link DataType#DICTIONARY}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public int getIndex(long index) {
        if (type != DataType.DICTIONARY) throw new IllegalStateException("Buffer is not DICTIONARY, is " + type);
        checkBounds(index);
        long physicalIndex = offset + index;
        return buffers[1].get(ValueLayout.JAVA_INT, physicalIndex * ValueLayout.JAVA_INT.byteSize());
    }

    /**
     * Reads the 64-bit IEEE 754 double at the given logical index.
     *
     * @param index the zero-based logical index
     * @return the {@code double} value at {@code index}
     * @throws IllegalStateException     if this buffer is not of type {@link DataType#FLOAT64}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public double getDouble(long index) {
        if (type != DataType.FLOAT64) throw new IllegalStateException("Buffer is not FLOAT64, is " + type);
        checkBounds(index);
        long physicalIndex = offset + index;
        return buffers[1].get(ValueLayout.JAVA_DOUBLE, physicalIndex * ValueLayout.JAVA_DOUBLE.byteSize());
    }

    /**
     * Reads the 64-bit signed integer (or 64-bit timestamp) at the given logical index.
     *
     * @param index the zero-based logical index
     * @return the {@code long} value at {@code index}
     * @throws IllegalStateException     if this buffer is not {@code INT64}, {@code TIMESTAMP_MS}, or {@code TIMESTAMP_US}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public long getLong(long index) {
        if (type != DataType.INT64 && type != DataType.TIMESTAMP_MS && type != DataType.TIMESTAMP_US) {
            throw new IllegalStateException("Buffer is not INT64, is " + type);
        }
        checkBounds(index);
        long physicalIndex = offset + index;
        return buffers[1].get(ValueLayout.JAVA_LONG, physicalIndex * ValueLayout.JAVA_LONG.byteSize());
    }

    /**
     * Reads the 32-bit IEEE 754 float at the given logical index.
     *
     * @param index the zero-based logical index
     * @return the {@code float} value at {@code index}
     * @throws IllegalStateException     if this buffer is not of type {@link DataType#FLOAT32}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
    public float getFloat(long index) {
        if (type != DataType.FLOAT32) throw new IllegalStateException("Buffer is not FLOAT32, is " + type);
        checkBounds(index);
        long physicalIndex = offset + index;
        return buffers[1].get(ValueLayout.JAVA_FLOAT, physicalIndex * ValueLayout.JAVA_FLOAT.byteSize());
    }

    /**
     * Reads the boolean value at the given logical index.
     *
     * @param index the zero-based logical index
     * @return the {@code boolean} value at {@code index}
     * @throws IllegalStateException     if this buffer is not of type {@link DataType#BOOLEAN}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
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
     * Reads and decodes the UTF-8 {@link String} at the given logical index for {@link DataType#UTF8}
     * or {@link DataType#DICTIONARY} buffers, or returns {@code null} if the slot is null.
     *
     * @param index the zero-based logical index
     * @return the decoded {@link String}, or {@code null} if null
     * @throws IllegalStateException     if this buffer is not {@link DataType#UTF8} or {@link DataType#DICTIONARY}
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= length()}
     */
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