package io.github.shridey.jdata.core.buffer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.*;

class BooleanBufferTest {

    @Test
    void testAlignmentAndBitPackingAcrossWordBoundaries() {
        try (Arena arena = Arena.ofConfined()) {
            int length = 200; // Crosses multiple 8-bit byte and 64-bit word boundaries
            BooleanBuffer col = BooleanBuffer.allocate(arena, "flags", length);

            assertEquals(0L, col.buffer(0).address() % 64, "Validity bitmap must be 64-byte aligned");
            assertEquals(0L, col.buffer(1).address() % 64, "Data buffer must be 64-byte aligned");
            assertEquals(0L, col.nullCount());

            for (int i = 0; i < length; i++) {
                if (i % 7 == 0) {
                    col.setNull(i);
                } else {
                    col.set(i, i % 3 == 0);
                }
            }

            long expectedNulls = (length + 6) / 7;
            assertEquals(expectedNulls, col.nullCount());

            for (int i = 0; i < length; i++) {
                if (i % 7 == 0) {
                    assertTrue(col.isNull(i), "Expected null at index " + i);
                } else {
                    assertFalse(col.isNull(i), "Expected valid at index " + i);
                    assertEquals(i % 3 == 0, col.get(i), "Mismatch at index " + i);
                }
            }

            // Overwriting a null bit must heal the validity bit and decrement nullCount in O(1)
            col.set(7, true);
            assertFalse(col.isNull(7));
            assertTrue(col.get(7));
            assertEquals(expectedNulls - 1, col.nullCount());

            // Idempotent setNull must not double-increment nullCount
            col.setNull(14);
            assertEquals(expectedNulls - 1, col.nullCount());
        }
    }

    @Test
    void testCopyFromFastAndSlicedPathsDoNotClobberNeighbors() {
        try (Arena arena = Arena.ofConfined()) {
            // 1. Unsliced fast path with non-multiple-of-8 length (19 bits = 2 full bytes + 3 tail bits)
            boolean[] input = new boolean[19];
            for (int i = 0; i < input.length; i++) {
                input[i] = (i % 2 == 0);
            }
            BooleanBuffer col = BooleanBuffer.allocate(arena, "fast_copy", input.length);
            col.setNull(5);
            col.copyFrom(input);

            assertEquals(0L, col.nullCount(), "copyFrom must reset all bits to valid");
            assertArrayEquals(input, col.toArray());

            // 2. Byte-aligned slice (offset = 8, length = 11) must preserve parent bits outside [8, 19)
            BooleanBuffer parent = BooleanBuffer.allocate(arena, "parent", 24);
            for (int i = 0; i < 24; i++) {
                parent.set(i, true);
            }

            BooleanBuffer alignedSlice = parent.slice(8, 11);
            boolean[] sliceInput = new boolean[11]; // all false
            alignedSlice.copyFrom(sliceInput);

            for (int i = 0; i < 8; i++) {
                assertTrue(parent.get(i), "Bit before slice at " + i + " was clobbered");
            }
            for (int i = 8; i < 19; i++) {
                assertFalse(parent.get(i), "Slice bit at " + i + " was not updated");
            }
            for (int i = 19; i < 24; i++) {
                assertTrue(parent.get(i), "Bit after slice at " + i + " was clobbered");
            }

            // 3. Unaligned slice (offset = 3, length = 10)
            BooleanBuffer unalignedSlice = parent.slice(3, 10);
            boolean[] alternating = {false, true, false, true, false, true, false, true, false, true};
            unalignedSlice.copyFrom(alternating);
            assertArrayEquals(alternating, unalignedSlice.toArray());
            assertTrue(parent.get(2), "Neighbor bit 2 before unaligned slice was clobbered");
        }
    }

    @Test
    void testCopyBlockFastAndSlowPathsPropagateNulls() {
        try (Arena arena = Arena.ofConfined()) {
            // Fast path: byte-aligned offsets (0 -> 8) and byte-multiple length (16)
            BooleanBuffer src = BooleanBuffer.allocate(arena, "src", 24);
            for (int i = 0; i < 24; i++) {
                src.set(i, i % 2 == 0);
            }
            src.setNull(2);
            src.setNull(11);

            BooleanBuffer dest = BooleanBuffer.allocate(arena, "dest", 32);
            assertEquals(0L, dest.nullCount()); // Cache initialized to 0

            dest.copyBlock(src, 0, 8, 16);

            // Fast path must invalidate dest's cached nullCount so the 2 copied nulls are detected
            assertEquals(2L, dest.nullCount());
            assertTrue(dest.isNull(10)); // src index 2 -> dest index 10
            assertTrue(dest.isNull(19)); // src index 11 -> dest index 19
            assertEquals(src.get(5), dest.get(13));

            // Slow path: unaligned bit offsets (srcIndex = 3, destIndex = 5, length = 13)
            BooleanBuffer destSlow = BooleanBuffer.allocate(arena, "dest_slow", 24);
            destSlow.copyBlock(src, 3, 5, 13);
            for (int i = 0; i < 13; i++) {
                assertEquals(src.isNull(3 + i), destSlow.isNull(5 + i));
                if (!src.isNull(3 + i)) {
                    assertEquals(src.get(3 + i), destSlow.get(5 + i));
                }
            }
        }
    }

    @Test
    void testSetAllValidOnSlicedWindows() {
        try (Arena arena = Arena.ofConfined()) {
            BooleanBuffer buf = BooleanBuffer.allocate(arena, "validity_window", 40);
            for (int i = 0; i < 40; i++) {
                buf.setNull(i);
            }
            assertEquals(40L, buf.nullCount());

            // 1. Sub-byte slice within a single byte: bits [2..5)
            BooleanBuffer tinySlice = buf.slice(2, 3);
            tinySlice.setAllValid();
            assertEquals(0L, tinySlice.nullCount());
            assertTrue(buf.isNull(1));
            assertFalse(buf.isNull(2));
            assertFalse(buf.isNull(3));
            assertFalse(buf.isNull(4));
            assertTrue(buf.isNull(5));

            // 2. Multi-byte unaligned slice: bits [11..35)
            BooleanBuffer multiByteSlice = buf.slice(11, 24);
            multiByteSlice.setAllValid();
            assertEquals(0L, multiByteSlice.nullCount());
            assertTrue(buf.isNull(10));
            for (int i = 11; i < 35; i++) {
                assertFalse(buf.isNull(i), "Bit " + i + " should be valid");
            }
            assertTrue(buf.isNull(35));
        }
    }

    @Test
    void testBuilderDynamicGrowthAndBlockAppends() {
        try (Arena arena = Arena.ofConfined()) {
            BooleanBufferBuilder builder = new BooleanBufferBuilder(arena, "stream", 2);

            // Append 3 booleans so write-head is unaligned (logicalLength = 3)
            builder.append(true).append(false).appendNull();

            // appendAll from an unaligned write-head (25 elements -> crosses multiple bytes)
            boolean[] batch = new boolean[25];
            for (int i = 0; i < batch.length; i++) {
                batch[i] = (i % 3 == 0);
            }
            builder.appendAll(batch);

            BooleanBuffer built = builder.build();
            assertEquals(28L, built.length());
            assertEquals(1L, built.nullCount());
            assertTrue(built.get(0));
            assertFalse(built.get(1));
            assertTrue(built.isNull(2));
            for (int i = 0; i < batch.length; i++) {
                assertFalse(built.isNull(3 + i));
                assertEquals(batch[i], built.get(3 + i));
            }

            // Test toBuilder round-trip on an unaligned slice
            BooleanBuffer slice = built.slice(1, 20);
            BooleanBuffer rebuilt = slice.toBuilder(arena).append(true).build();
            assertEquals(21L, rebuilt.length());
            assertEquals(1L, rebuilt.nullCount());
            assertFalse(rebuilt.get(0));
            assertTrue(rebuilt.isNull(1));
            assertTrue(rebuilt.get(20));
        }
    }

    @Test
    void testStrictBoundsAndOverflowChecks() {
        try (Arena arena = Arena.ofConfined()) {
            assertThrows(IllegalArgumentException.class, () -> BooleanBuffer.allocate(arena, "bad", -5));

            BooleanBuffer buf = BooleanBuffer.allocate(arena, "bounds", 16);
            assertThrows(IndexOutOfBoundsException.class, () -> buf.get(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.get(16));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.slice(-1, 5));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.slice(10, 8));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.slice(10, Long.MAX_VALUE));

            BooleanBuffer slice = buf.slice(8, 8);
            // Negative destIndex on a slice must not bypass bounds check to corrupt parent offset
            assertThrows(IndexOutOfBoundsException.class, () -> slice.copyBlock(buf, 0, -2, 4));
            assertThrows(IndexOutOfBoundsException.class, () -> slice.copyBlock(buf, -2, 0, 4));
        }
    }
}