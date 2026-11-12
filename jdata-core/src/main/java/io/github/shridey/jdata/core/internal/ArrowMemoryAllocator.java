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
     * Rounds a byte size up to the nearest multiple of 64 bytes for SIMD / AVX-512 tail safety.
     */
    public static long padTo64(long byteSize) {
        if (byteSize < 0) {
            throw new IllegalArgumentException("Allocation byteSize cannot be negative: " + byteSize);
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
}