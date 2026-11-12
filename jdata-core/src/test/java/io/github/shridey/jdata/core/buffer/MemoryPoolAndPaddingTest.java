package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.ArrowMemoryPool;
import io.github.shridey.jdata.core.PooledArena;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class MemoryPoolAndPaddingTest {

    @Test
    void test64ByteTailPaddingOnAllocatedAndBuiltBuffers() {
        try (Arena arena = Arena.ofConfined()) {
            // Allocate 3 ints (12 logical bytes -> must be padded to 64 physical bytes)
            Int32Buffer allocated = Int32Buffer.allocate(arena, "simd_safe", 3);
            allocated.copyFrom(new int[]{10, 20, 30});

            assertEquals(64L, allocated.buffer(0).byteSize(), "Validity bitmap must be 64-byte padded");
            assertEquals(64L, allocated.buffer(1).byteSize(), "Data buffer must be 64-byte padded");

            // Simulate AVX-512 (64-byte = 8 longs) vector load across the entire 64-byte block without IndexOutOfBoundsException
            for (long offset = 0; offset < 64; offset += Long.BYTES) {
                long byteOffset = offset;
                assertDoesNotThrow(() -> allocated.buffer(1).get(ValueLayout.JAVA_LONG, byteOffset));
            }
            // Tail padding bytes [12..64) must be zero-initialized
            assertEquals(0, allocated.buffer(1).get(ValueLayout.JAVA_INT, 12));
            assertEquals(0L, allocated.buffer(1).get(ValueLayout.JAVA_LONG, 56));

            // Verify Builder.build() also preserves 64-byte tail padding
            Int32Buffer built = new Int32BufferBuilder(arena, "built_simd", 2)
                    .append(100)
                    .append(200)
                    .append(300)
                    .build();

            assertEquals(64L, built.buffer(0).byteSize());
            assertEquals(64L, built.buffer(1).byteSize());
            assertEquals(0, built.buffer(1).get(ValueLayout.JAVA_INT, 12));
        }
    }

    @Test
    void testSlabRecyclingAndDirtyPrefixZeroing() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(64 * 1024, 4)) {
            assertEquals(64 * 1024L, pool.slabByteSize());
            assertEquals(4, pool.maxCachedSlabs());

            long firstCheckoutAddress;

            // Checkout 1: Allocate buffer, write non-zero data, and close arena
            try (PooledArena arena1 = pool.acquireArena()) {
                assertTrue(arena1.scope().isAlive());
                assertEquals(1, pool.activeCheckoutCount());

                Int64Buffer col1 = Int64Buffer.allocate(arena1, "col1", 100);
                firstCheckoutAddress = col1.buffer(1).address();
                for (int i = 0; i < 100; i++) {
                    col1.set(i, 0xCAFEBABE_DEADBEEFL);
                }
                assertEquals(1, pool.totalAllocatedSlabs());
                assertEquals(64 * 1024L, pool.totalAllocatedSlabBytes());
                assertEquals(0, pool.cachedSlabCount());
            }

            assertEquals(0, pool.activeCheckoutCount());
            assertEquals(1, pool.cachedSlabCount());
            assertEquals(64 * 1024L, pool.cachedBytes());

            // Checkout 2: Must reuse the exact same physical off-heap slab address and be cleanly zeroed
            try (PooledArena arena2 = pool.acquireArena()) {
                Int64Buffer col2 = Int64Buffer.allocate(arena2, "col2", 100);
                assertEquals(firstCheckoutAddress, col2.buffer(1).address(),
                        "PooledArena must recycle the exact same physical off-heap slab");

                for (int i = 0; i < 100; i++) {
                    assertEquals(0L, col2.get(i), "Recycled slab memory must be zero-initialized");
                }
            }
        }
    }

    @Test
    void testSharedPooledArenaConcurrencyAndDefaultPoolMetrics() throws Exception {
        try (ArrowMemoryPool defaultPool = new ArrowMemoryPool()) {
            assertEquals(ArrowMemoryPool.DEFAULT_SLAB_BYTE_SIZE, defaultPool.slabByteSize());
            assertEquals(ArrowMemoryPool.DEFAULT_MAX_CACHED_SLABS, defaultPool.maxCachedSlabs());
            assertEquals(0, defaultPool.activeCheckoutCount());
            assertEquals(0L, defaultPool.totalAllocatedSlabBytes());

            try (PooledArena sharedArena = defaultPool.acquireSharedArena();
                 ExecutorService executor = Executors.newFixedThreadPool(4)) {

                assertEquals(1, defaultPool.activeCheckoutCount());
                // Closing the pool while a checkout is active must be rejected
                assertThrows(IllegalStateException.class, defaultPool::close);

                List<Callable<Long>> tasks = createTasks(sharedArena);

                for (Future<Long> future : executor.invokeAll(tasks)) {
                    assertTrue(future.get() > 0);
                }

                assertEquals(ArrowMemoryPool.DEFAULT_SLAB_BYTE_SIZE, defaultPool.totalAllocatedSlabBytes());
            }

            assertEquals(0, defaultPool.activeCheckoutCount());
        }
    }

    private static @NonNull List<Callable<Long>> createTasks(PooledArena sharedArena) {
        List<Callable<Long>> tasks = new ArrayList<>();
        for (int t = 0; t < 4; t++) {
            int threadId = t;
            tasks.add(() -> {
                Int64Buffer buf = Int64Buffer.allocate(sharedArena, "shared_" + threadId, 128);
                long sum = 0;
                for (int i = 0; i < 128; i++) {
                    buf.set(i, i + threadId);
                    sum += buf.get(i);
                }
                return sum;
            });
        }
        return tasks;
    }

    @Test
    void testInPlaceBuilderExpansionInPooledArena() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(256 * 1024, 2);
             PooledArena arena = pool.acquireArena()) {

            // Start with small initial capacity (8 rows = 64 bytes)
            Int64BufferBuilder builder = new Int64BufferBuilder(arena, "in_place", 8);
            long initialValidityAddr = builder.validityBitmap.address();

            // Append 500 rows:
            // - validityBitmap (500 bits = 63 bytes <= 64 bytes) never moves or reallocates
            // - dataBuffer (last allocation on the slab) expands in-place from 64B -> 4096B
            for (int i = 0; i < 500; i++) {
                builder.append(i * 7L);
            }

            Int64Buffer built = builder.build();
            assertEquals(initialValidityAddr, built.buffer(0).address(),
                    "Validity bitmap should not reallocate up to 512 rows");
            assertEquals(initialValidityAddr + 64L, built.buffer(1).address(),
                    "Data buffer should expand in-place on the slab without moving its base address");

            for (int i = 0; i < 500; i++) {
                assertEquals(i * 7L, built.get(i));
            }
        }
    }

    @Test
    void testMultiColumnBuildersRecycleRelocatedBlocksAcrossResizes() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(64 * 1024, 2);
             PooledArena arena = pool.acquireArena()) {

            // Simulate ingesting 4 columns in parallel (interleaved on the same 64 KB slab)
            Int64BufferBuilder c1 = new Int64BufferBuilder(arena, "c1", 8);
            Int64BufferBuilder c2 = new Int64BufferBuilder(arena, "c2", 8);
            Int64BufferBuilder c3 = new Int64BufferBuilder(arena, "c3", 8);
            Int64BufferBuilder c4 = new Int64BufferBuilder(arena, "c4", 8);

            for (int row = 0; row < 200; row++) {
                c1.append(row);
                c2.append(row * 2L);
                c3.append(row * 3L);
                c4.append(row * 4L);
            }

            // All 4 columns must fit comfortably inside a single 64 KB slab and recycle intermediate blocks
            assertEquals(1, pool.totalAllocatedSlabs(), "Interleaved builders must recycle blocks within 1 slab");
            assertTrue(arena.recycledBlockCount() > 0, "Relocated power-of-two blocks must be cached in freeBlocks");

            Int64Buffer b1 = c1.build();
            Int64Buffer b4 = c4.build();
            assertEquals(199L, b1.get(199));
            assertEquals(199L * 4L, b4.get(199));
        }
    }

    @Test
    void testHumongousBuilderResizeImmediatelyFreesOldHumongousBlock() {
        // Use a small 1 KB slab size so allocations > 1 KB are treated as humongous
        try (ArrowMemoryPool pool = new ArrowMemoryPool(1024, 2);
             PooledArena arena = pool.acquireArena()) {

            // Initial capacity: 256 longs = 2,048 bytes (> 1,024B slab size -> 1 humongous block)
            Int64BufferBuilder builder = new Int64BufferBuilder(arena, "humongous", 256);
            assertEquals(1, arena.activeHumongousCount());

            // Append 600 longs -> resizes from 2 KB -> 4 KB -> 8 KB.
            // Each resize must immediately close and free the previous humongous Arena.
            for (int i = 0; i < 600; i++) {
                builder.append(i);
            }

            // Only the current 8 KB humongous block should remain active
            assertEquals(1, arena.activeHumongousCount(),
                    "Old humongous blocks must be freed immediately upon resize, not accumulated");

            Int64Buffer built = builder.build();
            assertEquals(599L, built.get(599));
        }
    }

    @Test
    void testUseAfterCloseThrowsIllegalStateExceptionAndEvictsExcessSlabs() {
        // Pool configured with 4 KB slabs and maxCachedSlabs = 1
        try (ArrowMemoryPool pool = new ArrowMemoryPool(4096, 1)) {
            Int32Buffer escapedBuffer;

            try (PooledArena arena = pool.acquireArena()) {
                // Allocate 3 buffers of 2 KB each -> forces 3 slabs to be allocated
                Int32Buffer b1 = Int32Buffer.allocate(arena, "b1", 500);
                Int32Buffer b2 = Int32Buffer.allocate(arena, "b2", 500);
                Int32Buffer b3 = Int32Buffer.allocate(arena, "b3", 500);
                b1.set(0, 1);
                b2.set(0, 2);
                b3.set(0, 3);
                escapedBuffer = b1;

                assertEquals(3, pool.totalAllocatedSlabs());
            }

            // Because maxCachedSlabs == 1, 2 excess slabs must have been freed back to the OS
            assertEquals(1, pool.cachedSlabCount());
            assertEquals(1, pool.totalAllocatedSlabs());

            // Reading from a buffer whose PooledArena was closed must throw IllegalStateException (temporal safety)
            assertThrows(IllegalStateException.class, () -> escapedBuffer.get(0));
        }
    }
}