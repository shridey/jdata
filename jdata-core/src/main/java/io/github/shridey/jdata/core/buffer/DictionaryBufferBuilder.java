package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.ffi.ImportedDataBuffer;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Dynamically builds an Apache Arrow Dictionary-Encoded column ({@code INT32} codes + {@code UTF8} dictionary).
 * Supports both fast heap {@link String} deduplication and zero-allocation off-heap UTF-8 byte deduplication.
 */
public final class DictionaryBufferBuilder extends AbstractBufferBuilder {

    private static final int EMPTY_SLOT = -1;

    private MemorySegment indicesBuffer;
    private final UTF8BufferBuilder dictionaryBuilder;

    // Open-addressed off-heap byte hash table mapping UTF-8 byte slices -> dictionary code
    private int[] hashTable;
    private int hashMask;
    private int maxEntriesBeforeRehash;
    private int dictionarySize = 0;

    // Fast-path cache for heap String references to avoid re-encoding UTF-8 bytes on cache hits
    private final Map<String, Integer> heapCache;

    /**
     * Creates a new {@link DictionaryBufferBuilder} with a default expected cardinality of 64 unique values.
     *
     * @param arena                  the memory arena governing allocations
     * @param name                   the column name
     * @param initialLogicalCapacity the initial row capacity
     */
    public DictionaryBufferBuilder(Arena arena, String name, long initialLogicalCapacity) {
        this(arena, name, initialLogicalCapacity, 64);
    }

    /**
     * Creates a new {@link DictionaryBufferBuilder} with custom row capacity and expected dictionary cardinality.
     *
     * @param arena                  the memory arena governing allocations
     * @param name                   the column name
     * @param initialLogicalCapacity the initial row capacity
     * @param expectedCardinality    the expected number of unique strings in the dictionary
     */
    public DictionaryBufferBuilder(Arena arena, String name, long initialLogicalCapacity, int expectedCardinality) {
        super(arena, name, initialLogicalCapacity);
        if (expectedCardinality < 0) {
            throw new IllegalArgumentException("expectedCardinality cannot be negative: " + expectedCardinality);
        }

        long indicesBytes = Math.multiplyExact(logicalCapacity, ValueLayout.JAVA_INT.byteSize());
        this.indicesBuffer = ArrowMemoryAllocator.allocateAligned(arena, indicesBytes);

        int dictCap = Math.max(expectedCardinality, 16);
        this.dictionaryBuilder = new UTF8BufferBuilder(arena, name + "_dict", dictCap, dictCap * 16L);

        int tableSize = tableSizeFor(dictCap);
        this.hashTable = new int[tableSize];
        Arrays.fill(this.hashTable, EMPTY_SLOT);
        this.hashMask = tableSize - 1;
        this.maxEntriesBeforeRehash = (int) (tableSize * 0.7f);
        this.heapCache = new HashMap<>(dictCap);
    }

    private static int tableSizeFor(int expectedItems) {
        int minSize = Math.max(32, (int) Math.ceil(expectedItems / 0.65));
        int highestBit = Integer.highestOneBit(minSize - 1);
        int powerOfTwo = highestBit << 1;
        return powerOfTwo > 0 ? powerOfTwo : (1 << 30);
    }

    @Override
    protected void resizeAdditionalLogicalBuffers(long newLogicalCapacity) {
        long newIndicesBytes = Math.multiplyExact(newLogicalCapacity, ValueLayout.JAVA_INT.byteSize());
        long currentIndicesBytes = logicalLength * ValueLayout.JAVA_INT.byteSize();
        this.indicesBuffer = ArrowMemoryAllocator.reallocateAligned(
                arena, indicesBuffer, currentIndicesBytes, newIndicesBytes
        );
    }

    /**
     * Appends a single string value (or marks the slot null if {@code value == null}),
     * deduplicating it into the underlying dictionary.
     *
     * @param value the string value to append (may be {@code null})
     * @return this builder
     */
    public DictionaryBufferBuilder append(String value) {
        if (value == null) {
            return appendNull();
        }

        ensureLogicalCapacity(1);
        int code = findOrInsertHeapString(value);
        indicesBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), code);
        logicalLength++;
        return this;
    }

    /**
     * Appends a single null entry.
     *
     * @return this builder
     */
    public DictionaryBufferBuilder appendNull() {
        ensureLogicalCapacity(1);
        markCurrentNull();
        indicesBuffer.set(ValueLayout.JAVA_INT, logicalLength * ValueLayout.JAVA_INT.byteSize(), 0);
        logicalLength++;
        return this;
    }

    /**
     * Bulk-appends an array of strings, deduplicating each non-null value into the dictionary.
     *
     * @param values the array of strings to append
     * @return this builder
     */
    public DictionaryBufferBuilder appendAll(String[] values) {
        Objects.requireNonNull(values, "Values array cannot be null");
        if (values.length == 0) return this;

        ensureLogicalCapacity(values.length);
        long intBytes = ValueLayout.JAVA_INT.byteSize();

        for (String val : values) {
            if (val == null) {
                markCurrentNull();
                indicesBuffer.set(ValueLayout.JAVA_INT, logicalLength * intBytes, 0);
            } else {
                int code = findOrInsertHeapString(val);
                indicesBuffer.set(ValueLayout.JAVA_INT, logicalLength * intBytes, code);
            }
            logicalLength++;
        }
        return this;
    }

    /**
     * Encodes and appends a range of strings directly from an off-heap {@link DataType#UTF8} {@link DataBuffer}
     * without allocating intermediate Java Strings.
     *
     * @param source   the source UTF-8 buffer
     * @param srcIndex the starting logical index in {@code source}
     * @param length   the number of elements to append
     * @return this builder
     */
    public DictionaryBufferBuilder appendUTF8Block(DataBuffer source, long srcIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        if (source.type() != DataType.UTF8) {
            throw new IllegalArgumentException("Expected UTF8 source buffer, got: " + source.type());
        }
        Objects.checkFromIndexSize(srcIndex, length, source.length());
        if (length == 0) return this;

        if (source instanceof ChunkedBuffer cb) {
            for (DataBuffer chunk : cb.slice(srcIndex, length).chunks()) {
                appendImportedBlock(chunk, 0, chunk.length());
            }
            return this;
        }

        ensureLogicalCapacity(length);

        MemorySegment srcOffsets = source.buffer(1);
        MemorySegment srcData = source.buffer(2);
        long srcPhysicalStart = srcIndex + source.offset();
        long intBytes = ValueLayout.JAVA_INT.byteSize();
        boolean hasNulls = source.nullCount() > 0;

        for (long i = 0; i < length; i++) {
            if (hasNulls && source.isNull(srcIndex + i)) {
                markCurrentNull();
                indicesBuffer.set(ValueLayout.JAVA_INT, logicalLength * intBytes, 0);
            } else {
                long physicalIdx = srcPhysicalStart + i;
                int startByte = srcOffsets.get(ValueLayout.JAVA_INT, physicalIdx * intBytes);
                int endByte = srcOffsets.get(ValueLayout.JAVA_INT, (physicalIdx + 1L) * intBytes);
                int code = findOrInsertOffHeapBytes(srcData, startByte, endByte - startByte);
                indicesBuffer.set(ValueLayout.JAVA_INT, logicalLength * intBytes, code);
            }
            logicalLength++;
        }
        return this;
    }

    /**
     * Appends a block from an existing {@link DictionaryBuffer}, lazily remapping only the unique
     * dictionary entries referenced within {@code [srcIndex, srcIndex + length)}.
     *
     * @param source   the source dictionary buffer
     * @param srcIndex the starting logical index in {@code source}
     * @param length   the number of elements to append
     * @return this builder
     */
    public DictionaryBufferBuilder appendBlock(DictionaryBuffer source, long srcIndex, long length) {
        return appendImportedBlock(source, srcIndex, length);
    }

    /**
     * Appends a block from a {@link DataBuffer} of type {@link DataType#DICTIONARY} or {@link DataType#UTF8}.
     * Lazily remaps only the dictionary entries actually referenced within {@code [srcIndex, srcIndex + length)}.
     *
     * @param source   the source dictionary or UTF-8 buffer
     * @param srcIndex the starting logical index in {@code source}
     * @param length   the number of elements to append
     * @return this builder
     */
    public DictionaryBufferBuilder appendImportedBlock(DataBuffer source, long srcIndex, long length) {
        Objects.requireNonNull(source, "Source buffer cannot be null");
        if (source.type() == DataType.UTF8) {
            return appendUTF8Block(source, srcIndex, length);
        }
        if (source.type() != DataType.DICTIONARY) {
            throw new IllegalArgumentException("Cannot append " + source.type() + " into a DictionaryBufferBuilder.");
        }
        Objects.checkFromIndexSize(srcIndex, length, source.length());
        if (length == 0) return this;

        if (source instanceof ChunkedBuffer cb) {
            for (DataBuffer chunk : cb.slice(srcIndex, length).chunks()) {
                appendImportedBlock(chunk, 0, chunk.length());
            }
            return this;
        }

        DataBuffer srcDict = switch (source) {
            case DictionaryBuffer db -> db.dictionary();
            case ImportedDataBuffer idb when idb.dictionary() != null -> idb.dictionary();
            default -> throw new IllegalArgumentException("Source DICTIONARY buffer is missing its dictionary.");
        };

        ensureLogicalCapacity(length);

        // Lazy O(K_used) code remap table: only inserts dictionary entries actually referenced in the slice
        int srcDictLen = (int) srcDict.length();
        int[] codeRemap = new int[srcDictLen];
        Arrays.fill(codeRemap, EMPTY_SLOT);

        MemorySegment srcDictOffsets = srcDict.buffer(1);
        MemorySegment srcDictData = srcDict.buffer(2);
        long srcDictPhysicalOffset = srcDict.offset();
        long intBytes = ValueLayout.JAVA_INT.byteSize();

        MemorySegment srcIndices = source.buffer(1);
        long srcPhysicalStart = srcIndex + source.offset();
        boolean hasNulls = source.nullCount() > 0;

        for (long i = 0; i < length; i++) {
            if (hasNulls && source.isNull(srcIndex + i)) {
                markCurrentNull();
                indicesBuffer.set(ValueLayout.JAVA_INT, logicalLength * intBytes, 0);
            } else {
                int srcCode = srcIndices.get(ValueLayout.JAVA_INT, (srcPhysicalStart + i) * intBytes);
                int targetCode = codeRemap[srcCode];
                if (targetCode == EMPTY_SLOT) {
                    long physK = srcDictPhysicalOffset + srcCode;
                    int startByte = srcDictOffsets.get(ValueLayout.JAVA_INT, physK * intBytes);
                    int endByte = srcDictOffsets.get(ValueLayout.JAVA_INT, (physK + 1L) * intBytes);
                    targetCode = findOrInsertOffHeapBytes(srcDictData, startByte, endByte - startByte);
                    codeRemap[srcCode] = targetCode;
                }
                indicesBuffer.set(ValueLayout.JAVA_INT, logicalLength * intBytes, targetCode);
            }
            logicalLength++;
        }
        return this;
    }

    /**
     * Returns the current number of unique entries in this builder's dictionary.
     *
     * @return the current dictionary cardinality
     */
    public int cardinality() {
        return dictionarySize;
    }

    private int findOrInsertHeapString(String value) {
        Integer cached = heapCache.get(value);
        if (cached != null) {
            return cached;
        }

        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        MemorySegment tempSeg = MemorySegment.ofArray(utf8);
        int code = findOrInsertOffHeapBytes(tempSeg, 0, utf8.length);
        heapCache.put(value, code);
        return code;
    }

    private int findOrInsertOffHeapBytes(MemorySegment srcData, long srcStart, int byteLen) {
        int hash = hashBytes(srcData, srcStart, byteLen);
        int slot = hash & hashMask;

        MemorySegment dictOffsets = dictionaryBuilder.offsetsSegment();
        MemorySegment dictData = dictionaryBuilder.dataSegment();
        long intBytes = ValueLayout.JAVA_INT.byteSize();

        while (true) {
            int candidateCode = hashTable[slot];
            if (candidateCode == EMPTY_SLOT) {
                break;
            }

            int candStart = dictOffsets.get(ValueLayout.JAVA_INT, (long) candidateCode * intBytes);
            int candEnd = dictOffsets.get(ValueLayout.JAVA_INT, (long) (candidateCode + 1) * intBytes);
            if ((candEnd - candStart) == byteLen) {
                if (byteLen == 0 || MemorySegment.mismatch(
                        dictData, candStart, candEnd,
                        srcData, srcStart, srcStart + byteLen) == -1L) {
                    return candidateCode;
                }
            }
            slot = (slot + 1) & hashMask;
        }

        // Not found: append new unique entry into dictionaryBuilder
        int newCode = dictionarySize++;
        dictionaryBuilder.appendRawBytes(srcData, srcStart, byteLen);
        hashTable[slot] = newCode;

        if (dictionarySize >= maxEntriesBeforeRehash) {
            rehash();
        }
        return newCode;
    }

    private void rehash() {
        int newCapacity = hashTable.length << 1;
        if (newCapacity <= 0) {
            throw new IllegalStateException("Dictionary hash table exceeded maximum capacity.");
        }

        int[] newTable = new int[newCapacity];
        Arrays.fill(newTable, EMPTY_SLOT);
        int newMask = newCapacity - 1;

        MemorySegment dictOffsets = dictionaryBuilder.offsetsSegment();
        MemorySegment dictData = dictionaryBuilder.dataSegment();
        long intBytes = ValueLayout.JAVA_INT.byteSize();

        for (int code = 0; code < dictionarySize; code++) {
            int start = dictOffsets.get(ValueLayout.JAVA_INT, (long) code * intBytes);
            int end = dictOffsets.get(ValueLayout.JAVA_INT, (long) (code + 1) * intBytes);
            int h = hashBytes(dictData, start, end - start);
            int slot = h & newMask;
            while (newTable[slot] != EMPTY_SLOT) {
                slot = (slot + 1) & newMask;
            }
            newTable[slot] = code;
        }

        this.hashTable = newTable;
        this.hashMask = newMask;
        this.maxEntriesBeforeRehash = (int) (newCapacity * 0.7f);
    }

    private static int hashBytes(MemorySegment segment, long start, int length) {
        long h = 0x9E3779B97F4A7C15L ^ length;
        long i = 0;
        long limit = length & ~7L;

        while (i < limit) {
            long word = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, start + i);
            h ^= word;
            h = Long.rotateLeft(h, 27) * 0x3C79AC492BA7B653L + 0x1C69B3F74AC4AE35L;
            i += 8L;
        }

        while (i < length) {
            h ^= (segment.get(ValueLayout.JAVA_BYTE, start + i) & 0xFFL);
            h *= 0x100000001B3L;
            i++;
        }

        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        return (int) h;
    }

    @Override
    public DictionaryBuffer build() {
        long paddedValidityBytes = ArrowMemoryAllocator.padTo64((logicalLength + 7L) >>> 3);
        long paddedIndicesBytes = ArrowMemoryAllocator.padTo64(logicalLength * ValueLayout.JAVA_INT.byteSize());

        MemorySegment sealedValidity = validityBitmap.asSlice(0, paddedValidityBytes);
        MemorySegment sealedIndices = indicesBuffer.asSlice(0, paddedIndicesBytes);
        UTF8Buffer sealedDictionary = dictionaryBuilder.build();

        return new DictionaryBuffer(name, arena, logicalLength, 0, sealedValidity, sealedIndices, sealedDictionary);
    }
}