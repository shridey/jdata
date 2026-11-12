package io.github.shridey.jdata.core;

import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A high-performance {@link Arena} implementation that sub-allocates 64-byte-aligned
 * slices from pooled off-heap slabs in O(1) pointer-bump time, supports in-place
 * tail expansion, and recycles relocated builder blocks across multi-column resizes.
 */
public final class PooledArena implements Arena {

    private final ArrowMemoryPool pool;
    private final Arena checkoutArena;
    private final boolean shared;
    private final List<CheckedOutSlab> acquiredSlabs = new ArrayList<>(4);
    private final Map<Long, Deque<MemorySegment>> freeBlocks = new HashMap<>();
    private final Map<Long, Arena> humongousArenas = new HashMap<>();

    private ArrowMemoryPool.Slab currentSlab;
    private MemorySegment currentScopedSegment;
    private long slabCursor = 0;
    private long lastAllocationOffset = -1;
    private boolean closed = false;

    PooledArena(ArrowMemoryPool pool, boolean shared) {
        this.pool = pool;
        this.shared = shared;
        this.checkoutArena = shared ? Arena.ofShared() : Arena.ofConfined();
    }

    @Override
    public MemorySegment allocate(long byteSize, long byteAlignment) {
        if (byteSize < 0) {
            throw new IllegalArgumentException("byteSize cannot be negative: " + byteSize);
        }
        if (byteAlignment <= 0 || (byteAlignment & (byteAlignment - 1L)) != 0L) {
            throw new IllegalArgumentException("byteAlignment must be a positive power of two: " + byteAlignment);
        }

        if (shared) {
            synchronized (this) {
                return allocateInternal(byteSize, byteAlignment);
            }
        }
        return allocateInternal(byteSize, byteAlignment);
    }

    private MemorySegment allocateInternal(long byteSize, long byteAlignment) {
        ensureOpen();

        long effectiveAlignment = Math.max(byteAlignment, 64L);
        long paddedSize = ArrowMemoryAllocator.padTo64(byteSize);

        // 1. Humongous allocation (> 1 slab): allocate in a dedicated Arena so it can be freed immediately if resized
        if (paddedSize > pool.slabByteSize()) {
            Arena humongousArena = shared ? Arena.ofShared() : Arena.ofConfined();
            MemorySegment segment = humongousArena.allocate(paddedSize, effectiveAlignment);
            humongousArenas.put(segment.address(), humongousArena);
            return segment;
        }

        // 2. Check intra-arena free list for a recycled power-of-two block from an earlier builder resize
        if (effectiveAlignment == 64L) {
            Deque<MemorySegment> bucket = freeBlocks.get(paddedSize);
            if (bucket != null && !bucket.isEmpty()) {
                return bucket.pollFirst();
            }
        }

        // 3. Pointer-bump allocation on the active slab
        if (currentSlab == null) {
            advanceToNewSlab();
        }

        long alignedOffset = (slabCursor + (effectiveAlignment - 1L)) & -effectiveAlignment;
        if (alignedOffset + paddedSize > pool.slabByteSize()) {
            advanceToNewSlab();
            alignedOffset = 0L;
        }

        lastAllocationOffset = alignedOffset;
        slabCursor = alignedOffset + paddedSize;

        return currentScopedSegment.asSlice(alignedOffset, paddedSize);
    }

    /**
     * Reallocates a builder buffer: expands in-place if it sits at the tip of the active slab,
     * or allocates a new block, copies live bytes, and recycles/frees the old block immediately.
     */
    public MemorySegment reallocate(MemorySegment current, long bytesToCopy, long newPaddedSize) {
        if (shared) {
            synchronized (this) {
                return reallocateInternal(current, bytesToCopy, newPaddedSize);
            }
        }
        return reallocateInternal(current, bytesToCopy, newPaddedSize);
    }

    private MemorySegment reallocateInternal(MemorySegment current, long bytesToCopy, long newPaddedSize) {
        ensureOpen();

        // Fast path: in-place expansion at the tail of the current slab (zero copy, zero dead memory)
        MemorySegment expanded = tryExpandInternal(current, newPaddedSize);
        if (expanded != null) {
            return expanded;
        }

        // Relocate path: allocate new block, copy valid bytes, and immediately recycle the old block
        MemorySegment next = allocateInternal(newPaddedSize, 64L);
        long safeCopyBytes = Math.min(bytesToCopy, current.byteSize());
        if (safeCopyBytes > 0) {
            MemorySegment.copy(current, 0, next, 0, safeCopyBytes);
        }

        recycleOldSegment(current, safeCopyBytes);
        return next;
    }

    private void recycleOldSegment(MemorySegment oldSegment, long dirtyBytes) {
        // If the old segment was a humongous allocation (> 1 slab), close its dedicated Arena to free OS RAM immediately
        Arena humongousArena = humongousArenas.remove(oldSegment.address());
        if (humongousArena != null) {
            humongousArena.close();
            return;
        }

        // Otherwise it is a slab slice: zero out its dirty prefix and cache it for reuse by other builders
        if (dirtyBytes > 0) {
            oldSegment.asSlice(0, dirtyBytes).fill((byte) 0);
        }
        freeBlocks.computeIfAbsent(oldSegment.byteSize(), _ -> new ArrayDeque<>(4))
                .offerFirst(oldSegment);
    }

    private MemorySegment tryExpandInternal(MemorySegment segment, long newPaddedSize) {
        if (currentScopedSegment == null || lastAllocationOffset < 0) {
            return null;
        }

        long expectedAddress = currentScopedSegment.address() + lastAllocationOffset;
        if (segment.address() == expectedAddress
                && lastAllocationOffset + segment.byteSize() == slabCursor
                && lastAllocationOffset + newPaddedSize <= pool.slabByteSize()) {
            slabCursor = lastAllocationOffset + newPaddedSize;
            return currentScopedSegment.asSlice(lastAllocationOffset, newPaddedSize);
        }
        return null;
    }

    /**
     * Returns the number of recycled blocks currently available for immediate reuse in this arena.
     */
    public int recycledBlockCount() {
        if (shared) {
            synchronized (this) {
                return freeBlocks.values().stream().mapToInt(Deque::size).sum();
            }
        }
        return freeBlocks.values().stream().mapToInt(Deque::size).sum();
    }

    /**
     * Returns the number of active humongous allocations (> slab size) currently open in this arena.
     */
    public int activeHumongousCount() {
        if (shared) {
            synchronized (this) {
                return humongousArenas.size();
            }
        }
        return humongousArenas.size();
    }

    private void advanceToNewSlab() {
        if (currentSlab != null) {
            acquiredSlabs.add(new CheckedOutSlab(currentSlab, slabCursor));
        }
        currentSlab = pool.acquireSlab();
        currentScopedSegment = currentSlab.rawSegment().reinterpret(pool.slabByteSize(), checkoutArena, null);
        slabCursor = 0L;
        lastAllocationOffset = -1L;
    }

    private void ensureOpen() {
        if (closed || !checkoutArena.scope().isAlive()) {
            throw new IllegalStateException("PooledArena is already closed.");
        }
    }

    @Override
    public MemorySegment.Scope scope() {
        return checkoutArena.scope();
    }

    @Override
    public void close() {
        if (shared) {
            synchronized (this) {
                closeInternal();
            }
        } else {
            closeInternal();
        }
    }

    private void closeInternal() {
        if (closed) {
            return;
        }
        // 1. Validate thread confinement and invalidate all slab slices first
        checkoutArena.close();
        closed = true;

        // 2. Close any remaining humongous allocations
        for (Arena humongous : humongousArenas.values()) {
            try {
                humongous.close();
            } catch (RuntimeException ignored) {
            }
        }
        humongousArenas.clear();
        freeBlocks.clear();

        // 3. Return all slabs to the pool, zeroing only their high-water-mark used prefix
        try {
            for (CheckedOutSlab checkedOut : acquiredSlabs) {
                pool.releaseSlab(checkedOut.slab(), checkedOut.usedBytes());
            }
            acquiredSlabs.clear();

            if (currentSlab != null) {
                pool.releaseSlab(currentSlab, slabCursor);
                currentSlab = null;
                currentScopedSegment = null;
            }
        } finally {
            pool.onArenaClosed();
        }
    }

    private record CheckedOutSlab(ArrowMemoryPool.Slab slab, long usedBytes) {}
}