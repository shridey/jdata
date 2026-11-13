package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.memory.ArrowMemoryPool;
import io.github.shridey.jdata.core.memory.PooledArena;
import io.github.shridey.jdata.core.internal.ArrowMemoryAllocator;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
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
            Int32Buffer allocated = Int32Buffer.allocate(arena, "simd_safe", 3);
            allocated.copyFrom(new int[]{10, 20, 30});

            assertEquals(64L, allocated.buffer(0).byteSize(), "Validity bitmap must be 64-byte padded");
            assertEquals(64L, allocated.buffer(1).byteSize(), "Data buffer must be 64-byte padded");

            for (long offset = 0; offset < 64; offset += Long.BYTES) {
                long byteOffset = offset;
                assertDoesNotThrow(() -> allocated.buffer(1).get(ValueLayout.JAVA_LONG, byteOffset));
            }
            assertEquals(0, allocated.buffer(1).get(ValueLayout.JAVA_INT, 12));
            assertEquals(0L, allocated.buffer(1).get(ValueLayout.JAVA_LONG, 56));

            Int32BufferBuilder builder = new Int32BufferBuilder(arena, "built_simd", 2);
            assertEquals("built_simd", builder.name());
            assertTrue(builder.capacity() >= 8L);
            builder.append(100).append(200).append(300);
            assertEquals(3L, builder.length());

            Int32Buffer built = builder.build();
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
            assertFalse(pool.isClosed());

            long firstCheckoutAddress;

            try (PooledArena arena1 = pool.acquireArena()) {
                assertFalse(arena1.isShared());
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

                assertTrue(sharedArena.isShared());
                assertEquals(1, defaultPool.activeCheckoutCount());
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

    private static List<Callable<Long>> createTasks(PooledArena sharedArena) {
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

            Int64BufferBuilder builder = new Int64BufferBuilder(arena, "in_place", 8);
            long initialValidityAddr = builder.validityBitmap.address();

            for (int i = 0; i < 500; i++) {
                builder.append(i * 7L);
            }

            Int64Buffer built = builder.build();
            assertEquals(initialValidityAddr, built.buffer(0).address());
            assertEquals(initialValidityAddr + 64L, built.buffer(1).address());

            for (int i = 0; i < 500; i++) {
                assertEquals(i * 7L, built.get(i));
            }
        }
    }

    @Test
    void testMultiColumnBuildersRecycleRelocatedBlocksAcrossResizes() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(64 * 1024, 2);
             PooledArena arena = pool.acquireArena()) {

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

            assertEquals(1, pool.totalAllocatedSlabs());
            assertTrue(arena.recycledBlockCount() > 0);

            Int64Buffer b1 = c1.build();
            Int64Buffer b4 = c4.build();
            assertEquals(199L, b1.get(199));
            assertEquals(199L * 4L, b4.get(199));
        }
    }

    @Test
    void testHumongousBuilderResizeImmediatelyFreesOldHumongousBlock() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(1024, 2);
             PooledArena arena = pool.acquireArena()) {

            Int64BufferBuilder builder = new Int64BufferBuilder(arena, "humongous", 256);
            assertEquals(1, arena.activeHumongousCount());

            for (int i = 0; i < 600; i++) {
                builder.append(i);
            }

            assertEquals(1, arena.activeHumongousCount());
            Int64Buffer built = builder.build();
            assertEquals(599L, built.get(599));
        }
    }

    @Test
    void testUseAfterCloseThrowsIllegalStateExceptionAndEvictsExcessSlabs() {
        ArrowMemoryPool pool = new ArrowMemoryPool(4096, 1);
        Int32Buffer escapedBuffer;

        try (PooledArena arena = pool.acquireArena()) {
            Int32Buffer b1 = Int32Buffer.allocate(arena, "b1", 500);
            Int32Buffer b2 = Int32Buffer.allocate(arena, "b2", 500);
            Int32Buffer b3 = Int32Buffer.allocate(arena, "b3", 500);
            b1.set(0, 1);
            b2.set(0, 2);
            b3.set(0, 3);
            escapedBuffer = b1;

            assertEquals(3, pool.totalAllocatedSlabs());

            assertThrows(IllegalArgumentException.class, () -> arena.allocate(-1, 64));
            assertThrows(IllegalArgumentException.class, () -> arena.allocate(64, 0));
            assertThrows(IllegalArgumentException.class, () -> arena.allocate(64, 3));
            assertThrows(IllegalArgumentException.class, () -> arena.reallocate(b1.buffer(1), -1, 128));
            assertSame(b1.buffer(1), arena.reallocate(b1.buffer(1), 64, 64));
        }

        assertEquals(1, pool.cachedSlabCount());
        assertEquals(1, pool.totalAllocatedSlabs());
        assertThrows(IllegalStateException.class, () -> escapedBuffer.get(0));

        pool.close();
        assertTrue(pool.isClosed());
        assertThrows(IllegalStateException.class, pool::acquireArena);
        assertThrows(IllegalStateException.class, pool::acquireSharedArena);
    }

    @Test
    void testAllocatorAndPoolValidationGuards() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (ArrowMemoryPool ignored = new ArrowMemoryPool(0, 1)) {
                fail("Expected IllegalArgumentException for non-positive slab size");
            }
        });
        assertThrows(IllegalArgumentException.class, () -> {
            try (ArrowMemoryPool ignored = new ArrowMemoryPool(1024, -1)) {
                fail("Expected IllegalArgumentException for negative maxCachedSlabs");
            }
        });
        assertThrows(IllegalArgumentException.class, () -> ArrowMemoryAllocator.padTo64(-1));
        assertThrows(ArithmeticException.class, () -> ArrowMemoryAllocator.padTo64(Long.MAX_VALUE));
        assertEquals(64L, ArrowMemoryAllocator.padTo64(0));

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment seg64 = ArrowMemoryAllocator.allocateAligned(arena, 64);
            assertSame(seg64, ArrowMemoryAllocator.reallocateAligned(arena, seg64, 32, 32));

            MemorySegment grown = ArrowMemoryAllocator.reallocateAligned(arena, seg64, 32, 128);
            assertEquals(128L, grown.byteSize());

            MemorySegment tinySeg = arena.allocate(4);
            assertThrows(IllegalArgumentException.class,
                    () -> Int32Buffer.wrap("bad", arena, -1, 0, seg64, seg64));
            assertThrows(IllegalArgumentException.class,
                    () -> Int32Buffer.wrap("bad", arena, 2, -1, seg64, seg64));
            assertThrows(IllegalArgumentException.class,
                    () -> Int32Buffer.wrap("bad", arena, 64, 0, tinySeg, seg64));
            assertThrows(IllegalArgumentException.class,
                    () -> Int32Buffer.wrap("bad", arena, 10, 0, seg64, tinySeg));
        }
    }
}