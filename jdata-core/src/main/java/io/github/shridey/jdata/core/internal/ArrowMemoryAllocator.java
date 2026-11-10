package io.github.shridey.jdata.core.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * A utility class responsible for allocating off-heap memory that strictly complies
 * with Apache Arrow's 64-byte alignment requirement.
 *
 * @version 1.0
 * @since 1.0
 */
public final class ArrowMemoryAllocator {

    // Prevent instantiation of this utility class.
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
        if (byteSize < 0) {
            throw new IllegalArgumentException("Allocation byteSize cannot be negative.");
        }

        // Allocate the segment with strict 64-byte alignment.
        MemorySegment segment = arena.allocate(byteSize, 64);

        // Arrow specification requires newly allocated memory to be zeroed out.
        segment.fill((byte) 0);

        return segment;
    }
}