package io.github.shridey.jdata.core.buffer;

import org.junit.jupiter.api.Test;
import java.lang.foreign.Arena;
import static org.junit.jupiter.api.Assertions.*;

class BooleanBufferTest {

    @Test
    void testBooleanBitPacking() {
        try (Arena arena = Arena.ofConfined()) {
            // Allocate 10 elements (requires 2 bytes of data buffer)
            BooleanBuffer col = BooleanBuffer.allocate(arena, "flags", 10);

            // Set two bits in the same byte
            col.set(0, true);
            col.set(1, false);
            col.set(2, true);

            assertTrue(col.get(0));
            assertFalse(col.get(1));
            assertTrue(col.get(2));

            // Ensure padding bits in the second byte aren't corrupted
            col.set(9, true);
            assertTrue(col.get(9));
            assertFalse(col.get(8)); // Should default to false (0x00 initialization)
        }
    }

    @Test
    void testToBuilderForBoolean() {
        try (Arena arena = Arena.ofConfined()) {
            BooleanBuffer original = BooleanBuffer.allocate(arena, "bools", 3);
            original.set(0, true);
            original.setNull(1);
            original.set(2, false);

            BooleanBufferBuilder builder = original.toBuilder(arena);
            builder.append(true);
            BooleanBuffer rebuilt = builder.build();

            assertEquals(4, rebuilt.length());
            assertTrue(rebuilt.get(0));
            assertTrue(rebuilt.isNull(1));
            assertFalse(rebuilt.get(2));
            assertTrue(rebuilt.get(3));
        }
    }

    @Test
    void testZeroCopySliceBoolean() {
        try (Arena arena = Arena.ofConfined()) {
            BooleanBuffer original = BooleanBuffer.allocate(arena, "flags", 10);
            original.copyFrom(new boolean[]{true, false, true, true, false, false, true, false, true, true});

            // Slice starting at an awkward bit offset (3)
            // Values should be: true, false, false, true
            BooleanBuffer slice = original.slice(3, 4);

            assertEquals(4, slice.length());
            assertEquals(3, slice.offset());

            assertTrue(slice.get(0));
            assertFalse(slice.get(1));
            assertFalse(slice.get(2));
            assertTrue(slice.get(3));

            // Verify the offset-aware toArray export
            boolean[] expected = {true, false, false, true};
            assertArrayEquals(expected, slice.toArray());
        }
    }
}