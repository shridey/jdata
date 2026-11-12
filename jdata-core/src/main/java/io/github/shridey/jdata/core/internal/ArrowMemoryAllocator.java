package io.github.shridey.jdata.core.internal;

import io.github.shridey.jdata.core.PooledArena;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * A utility class responsible for allocating off-heap memory that strictly complies
 * with Apache Arrow's 64-byte alignment and 64-byte tail padding requirements.
 */
public final class ArrowMemoryAllocator {

    private ArrowMemoryAllocator() {}

    /**
     * Rounds a byte size up to the nearest multiple of 64 bytes (minimum 64 bytes) for SIMD / AVX-512 tail safety.
     */
    public static long padTo64(long byteSize) {
        if (byteSize < 0) {
            throw new IllegalArgumentException("Allocation byteSize cannot be negative: " + byteSize);
        }
        if (byteSize == 0) {
            return 64L;
        }
        if (byteSize > Long.MAX_VALUE - 63L) {
            throw new ArithmeticException("Byte size overflow during 64-byte padding: " + byteSize);
        }
        return (byteSize + 63L) & ~63L;
    }

    /**
     * Allocates a contiguous block of off-heap memory aligned and padded to a 64-byte boundary.
     */
    public static MemorySegment allocateAligned(Arena arena, long byteSize) {
        Objects.requireNonNull(arena, "Arena cannot be null.");
        long paddedBytes = padTo64(byteSize);
        return arena.allocate(paddedBytes, 64);
    }

    /**
     * Reallocates an existing buffer to a larger 64-byte-padded capacity.
     * Routes through {@link PooledArena#reallocate} when pooled to expand in-place or recycle old blocks.
     */
    public static MemorySegment reallocateAligned(Arena arena, MemorySegment current, long bytesToCopy, long newByteSize) {
        Objects.requireNonNull(arena, "Arena cannot be null.");
        Objects.requireNonNull(current, "Current segment cannot be null.");

        long newPaddedBytes = padTo64(newByteSize);
        if (newPaddedBytes <= current.byteSize()) {
            return current;
        }

        if (arena instanceof PooledArena pooledArena) {
            return pooledArena.reallocate(current, bytesToCopy, newPaddedBytes);
        }

        MemorySegment next = arena.allocate(newPaddedBytes, 64);
        long safeCopyBytes = Math.min(bytesToCopy, current.byteSize());
        if (safeCopyBytes > 0) {
            MemorySegment.copy(current, 0, next, 0, safeCopyBytes);
        }
        return next;
    }

    /**
     * Validates common invariants when wrapping external or pre-computed MemorySegments into a DataBuffer.
     */
    public static long validateWrapBounds(String name, Arena arena, long length, long offset,
                                          MemorySegment validityBitmap, MemorySegment dataBuffer, long minDataBytes) {
        Objects.requireNonNull(name, "Column name cannot be null");
        Objects.requireNonNull(arena, "Arena cannot be null");
        Objects.requireNonNull(validityBitmap, "validityBitmap cannot be null");
        Objects.requireNonNull(dataBuffer, "dataBuffer cannot be null");

        if (length < 0) {
            throw new IllegalArgumentException("Length cannot be negative: " + length);
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }

        long totalElements = Math.addExact(length, offset);
        if (!validityBitmap.equals(MemorySegment.NULL)) {
            long minBitmapBytes = (totalElements + 7L) >>> 3;
            if (validityBitmap.byteSize() < minBitmapBytes) {
                throw new IllegalArgumentException(
                        "validityBitmap byteSize (" + validityBitmap.byteSize() + ") is smaller than required (" + minBitmapBytes + ")"
                );
            }
        }

        if (dataBuffer.byteSize() < minDataBytes) {
            throw new IllegalArgumentException(
                    "dataBuffer byteSize (" + dataBuffer.byteSize() + ") is smaller than required (" + minDataBytes + ")"
            );
        }
        return totalElements;
    }
}