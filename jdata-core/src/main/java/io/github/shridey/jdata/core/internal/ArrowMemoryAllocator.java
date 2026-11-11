package io.github.shridey.jdata.core.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * A utility class responsible for allocating off-heap memory that strictly complies
 * with Apache Arrow's 64-byte alignment requirement.
 *
 * @version 1.0
 * @since 1.0
 */
public final class ArrowMemoryAllocator {

    private ArrowMemoryAllocator() {}

    /**
     * Allocates a contiguous block of off-heap memory aligned to a 64-byte boundary.
     * 64-byte alignment is mandatory for Arrow to allow modern CPUs to use
     * AVX-512 (SIMD) hardware vectorization instructions efficiently.
     *
     * @param arena The memory arena that controls the lifecycle of this allocation.
     * @param byteSize The exact number of bytes to allocate.
     * @return A safely aligned {@link MemorySegment} initialized to zero.
     * @throws IllegalArgumentException if byteSize is negative.
     */
    public static MemorySegment allocateAligned(Arena arena, long byteSize) {
        Objects.requireNonNull(arena, "Arena cannot be null.");
        if (byteSize < 0) {
            throw new IllegalArgumentException("Allocation byteSize cannot be negative: " + byteSize);
        }

        // Standard JDK Arenas guarantee newly allocated memory is zero-initialized.
        return arena.allocate(byteSize, 64);
    }
}