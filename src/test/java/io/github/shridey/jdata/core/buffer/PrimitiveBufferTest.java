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
}