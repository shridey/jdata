package io.github.shridey.jdata.core;

import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread-safe off-heap Slab Memory Pool for high-throughput analytical workloads.
 * Pre-allocates and recycles 64-byte-aligned off-heap memory slabs across query lifecycles
 * to eliminate OS malloc/mmap overhead and page faults.
 */
public final class ArrowMemoryPool implements AutoCloseable {

    /** Default slab size: 4 MB. */
    public static final long DEFAULT_SLAB_BYTE_SIZE = 4L * 1024L * 1024L;

    /** Default maximum idle slabs retained in the pool (16 * 4 MB = 64 MB). */
    public static final int DEFAULT_MAX_CACHED_SLABS = 16;

    private final long slabByteSize;
    private final int maxCachedSlabs;
    private final Deque<Slab> freeSlabs = new ConcurrentLinkedDeque<>();
    private final AtomicInteger cachedSlabCount = new AtomicInteger(0);
    private final AtomicInteger totalAllocatedSlabs = new AtomicInteger(0);
    private final AtomicInteger activeCheckouts = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a memory pool with default settings (4 MB slabs, up to 16 cached slabs).
     */
    public ArrowMemoryPool() {
        this(DEFAULT_SLAB_BYTE_SIZE, DEFAULT_MAX_CACHED_SLABS);
    }

    /**
     * Creates a customized off-heap slab memory pool.
     *
     * @param slabByteSize   The size in bytes of each physical slab (rounded up to a multiple of 64).
     * @param maxCachedSlabs Maximum number of idle slabs to keep cached before freeing excess slabs to the OS.
     */
    public ArrowMemoryPool(long slabByteSize, int maxCachedSlabs) {
        if (slabByteSize <= 0) {
            throw new IllegalArgumentException("slabByteSize must be positive: " + slabByteSize);
        }
        if (maxCachedSlabs < 0) {
            throw new IllegalArgumentException("maxCachedSlabs cannot be negative: " + maxCachedSlabs);
        }
        this.slabByteSize = ArrowMemoryAllocator.padTo64(slabByteSize);
        this.maxCachedSlabs = maxCachedSlabs;
    }

    /**
     * Acquires a thread-confined {@link PooledArena} backed by this pool.
     */
    public PooledArena acquireArena() {
        ensureOpen();
        activeCheckouts.incrementAndGet();
        if (closed.get()) {
            activeCheckouts.decrementAndGet();
            throw new IllegalStateException("ArrowMemoryPool is already closed.");
        }
        return new PooledArena(this, false);
    }

    /**
     * Acquires a thread-safe shared {@link PooledArena} backed by this pool.
     */
    public PooledArena acquireSharedArena() {
        ensureOpen();
        activeCheckouts.incrementAndGet();
        if (closed.get()) {
            activeCheckouts.decrementAndGet();
            throw new IllegalStateException("ArrowMemoryPool is already closed.");
        }
        return new PooledArena(this, true);
    }

    Slab acquireSlab() {
        ensureOpen();
        Slab slab = freeSlabs.pollFirst();
        if (slab != null) {
            cachedSlabCount.decrementAndGet();
            return slab;
        }

        Arena slabArena = Arena.ofShared();
        MemorySegment rawSegment = slabArena.allocate(slabByteSize, 64);
        totalAllocatedSlabs.incrementAndGet();
        return new Slab(slabArena, rawSegment);
    }

    void releaseSlab(Slab slab, long usedBytes) {
        // Zero out only the dirty prefix of the slab that was actually handed out
        if (usedBytes > 0) {
            slab.rawSegment().asSlice(0, usedBytes).fill((byte) 0);
        }

        if (closed.get()) {
            destroySlab(slab);
            return;
        }

        // CAS loop to strictly enforce maxCachedSlabs under concurrent returns
        while (true) {
            int current = cachedSlabCount.get();
            if (current >= maxCachedSlabs) {
                destroySlab(slab);
                return;
            }
            if (cachedSlabCount.compareAndSet(current, current + 1)) {
                freeSlabs.offerFirst(slab);
                // Handle race if pool was closed concurrently
                if (closed.get() && freeSlabs.remove(slab)) {
                    cachedSlabCount.decrementAndGet();
                    destroySlab(slab);
                }
                return;
            }
        }
    }

    void onArenaClosed() {
        activeCheckouts.decrementAndGet();
    }

    private void destroySlab(Slab slab) {
        totalAllocatedSlabs.decrementAndGet();
        slab.slabArena().close();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("ArrowMemoryPool is already closed.");
        }
    }

    public long slabByteSize() {
        return slabByteSize;
    }

    public int maxCachedSlabs() {
        return maxCachedSlabs;
    }

    public int cachedSlabCount() {
        return cachedSlabCount.get();
    }

    public int totalAllocatedSlabs() {
        return totalAllocatedSlabs.get();
    }

    public int activeCheckoutCount() {
        return activeCheckouts.get();
    }

    public long cachedBytes() {
        return (long) cachedSlabCount.get() * slabByteSize;
    }

    public long totalAllocatedSlabBytes() {
        return (long) totalAllocatedSlabs.get() * slabByteSize;
    }

    @Override
    public void close() {
        int active = activeCheckouts.get();
        if (active > 0) {
            throw new IllegalStateException(
                    "Cannot close ArrowMemoryPool while " + active + " PooledArena(s) are still open."
            );
        }
        if (closed.compareAndSet(false, true)) {
            int racedActive = activeCheckouts.get();
            if (racedActive > 0) {
                closed.set(false);
                throw new IllegalStateException(
                        "Cannot close ArrowMemoryPool while " + racedActive + " PooledArena(s) are still open."
                );
            }
            Slab slab;
            while ((slab = freeSlabs.pollFirst()) != null) {
                cachedSlabCount.decrementAndGet();
                destroySlab(slab);
            }
        }
    }

    record Slab(Arena slabArena, MemorySegment rawSegment) {}
}