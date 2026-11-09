package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Dynamically builds a bit-packed Boolean column.
 * Used when ingesting boolean data of an unknown size.
 */
public final class BooleanBufferBuilder extends AbstractBufferBuilder {

    // The physical data buffer storing the bit-packed true/false values
    private MemorySegment dataBuffer;

    public BooleanBufferBuilder(Arena arena, String name, long initialLogicalCapacity) {
        super(arena, name, initialLogicalCapacity);

        // Calculate required bytes for bit-packed booleans (same math as validity bitmap)
        long dataBytes = (logicalCapacity + 7) / 8;
        this.dataBuffer = ArrowMemoryAllocator.allocateAligned(arena, dataBytes);
        this.dataBuffer.fill((byte) 0x00); // Default all values to false
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        // Parent class doubled the rows, so we must double our bit-packed data buffer
        long newDataBytes = (newLogicalCapacity + 7) / 8;
        MemorySegment newData = ArrowMemoryAllocator.allocateAligned(arena, newDataBytes);
        newData.fill((byte) 0x00);

        // Native OS-level copy of the old bits into the new segment
        long currentBytes = (logicalCapacity + 7) / 8;
        MemorySegment.copy(dataBuffer, 0, newData, 0, currentBytes);

        this.dataBuffer = newData;
    }

    /**
     * Appends a new boolean value to the end of the column.
     */
    public BooleanBufferBuilder append(boolean value) {
        ensureLogicalCapacity(1); // Double memory if we are full

        long byteIndex = logicalLength / 8;
        int bitIndex = (int) (logicalLength % 8);
        byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);

        if (value) {
            b = (byte) (b | (1 << bitIndex));  // Write 1 (true)
        } else {
            b = (byte) (b & ~(1 << bitIndex)); // Write 0 (false)
        }

        dataBuffer.set(ValueLayout.JAVA_BYTE, byteIndex, b);
        logicalLength++;
        return this;
    }

    /**
     * Bulk appends an entire array of booleans.
     * This proves why ensureLogicalCapacity must accept values > 1.
     */
    public BooleanBufferBuilder appendAll(boolean[] values) {
        ensureLogicalCapacity(values.length); // Here, additionalRows is values.length

        for (boolean value : values) {
            long byteIndex = logicalLength / 8;
            int bitIndex = (int) (logicalLength % 8);
            byte b = dataBuffer.get(ValueLayout.JAVA_BYTE, byteIndex);

            if (value) {
                b = (byte) (b | (1 << bitIndex));
            } else {
                b = (byte) (b & ~(1 << bitIndex));
            }

            dataBuffer.set(ValueLayout.JAVA_BYTE, byteIndex, b);
            logicalLength++;
        }
        return this;
    }

    /**
     * Appends a null row to the end of the column.
     */
    public BooleanBufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull(); // Parent marks the validity bitmap as 0
        logicalLength++;
        return this;
    }

    /**
     * Seals the builder into an immutable BooleanBuffer for the execution engine.
     */
    public BooleanBuffer build() {
        long exactBytes = (logicalLength + 7) / 8;

        // Slice the memory exactly to the written boundaries to prevent downstream leaks
        MemorySegment sealedValidity = validityBitmap.asSlice(0, exactBytes);
        MemorySegment sealedData = dataBuffer.asSlice(0, exactBytes);

        return new BooleanBuffer(name, arena, logicalLength, sealedValidity, sealedData);
    }
}