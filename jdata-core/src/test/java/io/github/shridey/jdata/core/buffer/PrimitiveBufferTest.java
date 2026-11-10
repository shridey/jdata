package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import org.junit.jupiter.api.Test;
import java.lang.foreign.Arena;
import static org.junit.jupiter.api.Assertions.*;

class PrimitiveBufferTest {

    @Test
    void testFloat64AllocationAndValues() {
        try (Arena arena = Arena.ofConfined()) {
            Float64Buffer col = Float64Buffer.allocate(arena, "prices", 100);

            assertEquals("prices", col.name());
            assertEquals(DataType.FLOAT64, col.type());
            assertEquals(100, col.length());

            // Test writing and reading exactly at hardware byte boundaries
            col.set(0, 42.5);
            col.set(99, -100.99);

            assertEquals(42.5, col.get(0));
            assertEquals(-100.99, col.get(99));
        }
    }

    @Test
    void testAbstractColumnNullStateLogic() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer col = Int32Buffer.allocate(arena, "ids", 10);

            // 1. Initial state should have 0 nulls (0xFF initialization)
            assertEquals(0, col.nullCount());
            assertFalse(col.isNull(5));

            // 2. Setting a null should flip the bit and invalidate the cache
            col.setNull(5);
            assertTrue(col.isNull(5));
            assertEquals(1, col.nullCount());

            // 3. The "Once Null, Always Null" fix: Overwriting should heal the null state
            col.set(5, 999);
            assertFalse(col.isNull(5));
            assertEquals(0, col.nullCount()); // Cache must recalculate to 0
        }
    }

    @Test
    void testToBuilderForPrimitive() {
        try (Arena arena = Arena.ofConfined()) {
            // 1. Create a sealed buffer with data and nulls
            Int32Buffer original = Int32Buffer.allocate(arena, "col", 3);
            original.set(0, 10);
            original.setNull(1);
            original.set(2, 30);

            // 2. Unseal it into a builder
            Int32BufferBuilder builder = original.toBuilder(arena);

            // 3. Mutate it to prove it's a completely decoupled, mutable copy
            builder.append(40);
            Int32Buffer rebuilt = builder.build();

            // 4. Verify all bounds and states
            assertEquals(4, rebuilt.length());
            assertEquals(10, rebuilt.get(0));
            assertTrue(rebuilt.isNull(1));
            assertEquals(30, rebuilt.get(2));
            assertEquals(40, rebuilt.get(3));
        }
    }

    @Test
    void testZeroCopySliceLogic() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer original = Int32Buffer.allocate(arena, "numbers", 5);
            original.copyFrom(new int[]{10, 20, 30, 40, 50});
            original.setNull(1); // 20 becomes null

            // Create a zero-copy view of rows 2, 3, and 4
            Int32Buffer slice = original.slice(2, 3);

            // 1. Verify Logical Metadata
            assertEquals(3, slice.length());
            assertEquals(2, slice.offset());
            assertEquals("numbers", slice.name());

            // 2. Verify Shifted Reads (Hardware physicalIndex math)
            assertEquals(30, slice.get(0));
            assertEquals(40, slice.get(1));
            assertEquals(50, slice.get(2));

            // 3. Verify the slow-path toArray extraction
            assertArrayEquals(new int[]{30, 40, 50}, slice.toArray());

            // 4. Verify that mutating the original affects the slice (proving they share memory)
            original.set(2, 999);
            assertEquals(999, slice.get(0));
        }
    }
}