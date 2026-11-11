package io.github.shridey.jdata.core.buffer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.*;

class PrimitiveBufferTest {

    @Test
    void testAllPrimitiveTypesAlignmentAndExtremeValues() {
        try (Arena arena = Arena.ofConfined()) {
            // Int32
            Int32Buffer i32 = Int32Buffer.allocate(arena, "i32", 4);
            assertEquals(0L, i32.buffer(0).address() % 64);
            assertEquals(0L, i32.buffer(1).address() % 64);
            i32.copyFrom(new int[]{Integer.MIN_VALUE, -1, 0, Integer.MAX_VALUE});
            assertArrayEquals(new int[]{Integer.MIN_VALUE, -1, 0, Integer.MAX_VALUE}, i32.toArray());

            // Int64
            Int64Buffer i64 = Int64Buffer.allocate(arena, "i64", 4);
            assertEquals(0L, i64.buffer(1).address() % 64);
            i64.copyFrom(new long[]{Long.MIN_VALUE, -1L, 0L, Long.MAX_VALUE});
            assertArrayEquals(new long[]{Long.MIN_VALUE, -1L, 0L, Long.MAX_VALUE}, i64.toArray());

            // Float32
            Float32Buffer f32 = Float32Buffer.allocate(arena, "f32", 4);
            assertEquals(0L, f32.buffer(1).address() % 64);
            float[] f32Vals = {Float.NEGATIVE_INFINITY, -0.0f, Float.NaN, Float.MAX_VALUE};
            f32.copyFrom(f32Vals);
            assertArrayEquals(f32Vals, f32.toArray());

            // Float64
            Float64Buffer f64 = Float64Buffer.allocate(arena, "f64", 4);
            assertEquals(0L, f64.buffer(1).address() % 64);
            double[] f64Vals = {Double.NEGATIVE_INFINITY, -0.0, Double.NaN, Double.MAX_VALUE};
            f64.copyFrom(f64Vals);
            assertArrayEquals(f64Vals, f64.toArray());
        }
    }

    @Test
    void test64BitPopcntNullRecalculationOnLargeAndSlicedBuffers() {
        try (Arena arena = Arena.ofConfined()) {
            // 1,500 rows exercises: 64-bit word loop (23 longs = 1472 bits) + 3 full bytes (24 bits) + 4 tail bits
            int rows = 1500;
            Int64Buffer col = Int64Buffer.allocate(arena, "metrics", rows);

            int expectedNulls = 0;
            for (int i = 0; i < rows; i++) {
                if (i % 11 == 0) {
                    col.setNull(i);
                    expectedNulls++;
                } else {
                    col.set(i, i * 10L);
                }
            }
            assertEquals(expectedNulls, col.nullCount());

            // Byte-aligned slice (offset = 64, length = 700) -> exercises 64-bit POPCNT fast path on a slice
            Int64Buffer alignedSlice = col.slice(64, 700);
            int expectedAlignedNulls = 0;
            for (int i = 64; i < 764; i++) {
                if (i % 11 == 0) expectedAlignedNulls++;
            }
            assertEquals(expectedAlignedNulls, alignedSlice.nullCount());

            // Unaligned bit slice (offset = 19, length = 500) -> exercises unaligned bit-scan path
            Int64Buffer unalignedSlice = col.slice(19, 500);
            int expectedUnalignedNulls = 0;
            for (int i = 19; i < 519; i++) {
                if (i % 11 == 0) expectedUnalignedNulls++;
            }
            assertEquals(expectedUnalignedNulls, unalignedSlice.nullCount());
        }
    }

    @Test
    void testChainedSlicesAndBulkCopyIsolation() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer root = Int32Buffer.allocate(arena, "root", 20);
            int[] initial = new int[20];
            for (int i = 0; i < 20; i++) initial[i] = i;
            root.copyFrom(initial);

            // Chained slice: root[5..15) -> subSlice[2..7) == root[7..12)
            Int32Buffer slice1 = root.slice(5, 10);
            Int32Buffer slice2 = slice1.slice(2, 5);

            assertEquals(7L, slice2.offset());
            assertEquals(5L, slice2.length());
            assertArrayEquals(new int[]{7, 8, 9, 10, 11}, slice2.toArray());

            // Bulk copyFrom into slice2 must only overwrite root[7..12)
            slice2.copyFrom(new int[]{700, 800, 900, 1000, 1100});
            assertEquals(6, root.get(6));
            assertEquals(700, root.get(7));
            assertEquals(1100, root.get(11));
            assertEquals(12, root.get(12));
        }
    }

    @Test
    void testBuildersScaleAndPreserveNullsAcrossBlocks() {
        try (Arena arena = Arena.ofConfined()) {
            Float64BufferBuilder builder = new Float64BufferBuilder(arena, "revenue", 2);

            for (int i = 0; i < 1000; i++) {
                if (i % 100 == 0) {
                    builder.appendNull();
                } else {
                    builder.append(i * 1.5);
                }
            }
            builder.appendAll(new double[]{999.1, 999.2, 999.3});

            Float64Buffer built = builder.build();
            assertEquals(1003L, built.length());
            assertEquals(10L, built.nullCount());
            assertTrue(built.isNull(0));
            assertTrue(built.isNull(900));
            assertEquals(1.5, built.get(1));
            assertEquals(999.3, built.get(1002));

            // Test toBuilder on a sliced view
            Float64Buffer sliced = built.slice(95, 15); // Contains index 100 (null)
            Float64Buffer rebuilt = sliced.toBuilder(arena).build();
            assertEquals(15L, rebuilt.length());
            assertEquals(0L, rebuilt.offset());
            assertEquals(1L, rebuilt.nullCount());
            assertTrue(rebuilt.isNull(5)); // 95 + 5 = 100
            assertEquals(96 * 1.5, rebuilt.get(1));
        }
    }

    @Test
    void testPrimitiveBoundsAndOverflowProtection() {
        try (Arena arena = Arena.ofConfined()) {
            assertThrows(IllegalArgumentException.class, () -> Int32Buffer.allocate(arena, "neg", -1));
            assertThrows(ArithmeticException.class, () -> Int64Buffer.allocate(arena, "overflow", Long.MAX_VALUE));

            Int32Buffer buf = Int32Buffer.allocate(arena, "buf", 10);
            Int32Buffer slice = buf.slice(4, 4);

            assertThrows(IndexOutOfBoundsException.class, () -> slice.get(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> slice.get(4));
            assertThrows(IndexOutOfBoundsException.class, () -> slice.copyBlock(buf, 0, -1, 2));
            assertThrows(IndexOutOfBoundsException.class, () -> slice.copyBlock(buf, 9, 0, 2));

            Int32BufferBuilder builder = new Int32BufferBuilder(arena, "b", 4);
            assertThrows(IndexOutOfBoundsException.class, () -> builder.appendBlock(buf, -1, 3));
            assertThrows(IndexOutOfBoundsException.class, () -> builder.appendBlock(buf, 8, 5));
            assertThrows(IllegalArgumentException.class, () -> builder.appendImportedBlock(
                    Float32Buffer.allocate(arena, "f", 2), 0, 2
            ));
        }
    }
}