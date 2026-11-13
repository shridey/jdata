package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.ffi.ArrowCExporter;
import io.github.shridey.jdata.core.ffi.ArrowCImporter;
import io.github.shridey.jdata.core.ffi.ImportedDataBuffer;
import io.github.shridey.jdata.core.layout.Field;
import io.github.shridey.jdata.core.layout.Schema;
import io.github.shridey.jdata.core.layout.Table;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BitmapsAndTableOpsTest {

    @Test
    void testWordLevelBitmapsLogicAndSelectionIndicesOnSlicedBuffers() {
        try (Arena arena = Arena.ofConfined()) {
            BooleanBuffer rawA = BooleanBuffer.allocate(arena, "a", 75);
            BooleanBuffer rawB = BooleanBuffer.allocate(arena, "b", 75);

            for (int i = 0; i < 75; i++) {
                rawA.set(i, (i % 2) == 0);
                rawB.set(i, (i % 3) == 0);
            }

            // Test intersectValidity fast path when neither buffer has nulls
            MemorySegment nonNullIntersect = Bitmaps.intersectValidity(rawA, rawB, arena);
            assertEquals(75L, Long.bitCount(Bitmaps.extractWord(nonNullIntersect, 0, 64))
                    + Long.bitCount(Bitmaps.extractWord(nonNullIntersect, 64, 11)));

            rawA.setNull(9);  // slice index 6
            rawB.setNull(15); // slice index 12

            BooleanBuffer sliceA = rawA.slice(3, 70);
            BooleanBuffer sliceB = rawB.slice(3, 70);

            BooleanBuffer andBuf = Bitmaps.and(sliceA, sliceB, "and_col", arena);
            BooleanBuffer orBuf = Bitmaps.or(sliceA, sliceB, "or_col", arena);
            BooleanBuffer xorBuf = Bitmaps.xor(sliceA, sliceB, "xor_col", arena);
            BooleanBuffer notBuf = Bitmaps.not(sliceA, "not_col", arena);

            assertEquals(70L, andBuf.length());
            assertEquals(2L, andBuf.nullCount());
            assertTrue(andBuf.isNull(6));
            assertTrue(andBuf.isNull(12));

            for (int i = 0; i < 70; i++) {
                int origIdx = i + 3;
                boolean aVal = (origIdx % 2) == 0;
                boolean bVal = (origIdx % 3) == 0;
                if (i != 6 && i != 12) {
                    assertEquals(aVal && bVal, andBuf.get(i));
                    assertEquals(aVal || bVal, orBuf.get(i));
                    assertEquals(aVal ^ bVal, xorBuf.get(i));
                }
                if (i != 6) {
                    assertEquals(!aVal, notBuf.get(i));
                } else {
                    assertTrue(notBuf.isNull(i));
                }
            }

            Int32Buffer sel = Bitmaps.toSelectionIndices(andBuf, arena);
            for (int i = 0; i < sel.length(); i++) {
                int matchedRow = sel.get(i);
                assertNotEquals(6, matchedRow);
                assertNotEquals(12, matchedRow);
                assertTrue(andBuf.get(matchedRow));
            }

            // Validation guards on Bitmaps
            assertThrows(IllegalArgumentException.class, () -> Bitmaps.extractWord(rawA.buffer(0), -1, 8));
            assertThrows(IllegalArgumentException.class, () -> Bitmaps.extractWord(rawA.buffer(0), 0, 0));
            assertThrows(IllegalArgumentException.class, () -> Bitmaps.extractWord(rawA.buffer(0), 0, 65));
            assertThrows(IllegalArgumentException.class, () -> Bitmaps.intersectValidity(rawA, sliceA, arena));
            assertThrows(IllegalArgumentException.class, () -> Bitmaps.and(rawA, sliceA, "bad", arena));
        }
    }

    @Test
    void testTakeAndWithNameAcrossAllBufferImplementations() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer indices = Int32Buffer.allocate(arena, "idx", 4);
            indices.copyFrom(new int[]{3, 1, 0, 2});
            indices.setNull(2);

            Int32Buffer i32 = Int32Buffer.allocate(arena, "i32", 4);
            i32.copyFrom(new int[]{10, 20, 30, 40});
            Int32Buffer gI32 = i32.withName("i32_renamed").take(indices, arena);
            assertEquals("i32_renamed", gI32.name());
            assertEquals(40, gI32.get(0));
            assertEquals(20, gI32.get(1));
            assertTrue(gI32.isNull(2));
            assertEquals(30, gI32.get(3));

            Int64Buffer i64 = Int64Buffer.allocate(arena, "i64", 4);
            i64.copyFrom(new long[]{100L, 200L, 300L, 400L});
            Int64Buffer gI64 = i64.withName("i64_r").take(indices, arena);
            assertEquals(400L, gI64.get(0));
            assertTrue(gI64.isNull(2));

            Float32Buffer f32 = Float32Buffer.allocate(arena, "f32", 4);
            f32.copyFrom(new float[]{1.5f, 2.5f, 3.5f, 4.5f});
            Float32Buffer gF32 = f32.withName("f32_r").take(indices, arena);
            assertEquals(4.5f, gF32.get(0));
            assertTrue(gF32.isNull(2));

            Float64Buffer f64 = Float64Buffer.allocate(arena, "f64", 4);
            f64.copyFrom(new double[]{10.5, 20.5, 30.5, 40.5});
            Float64Buffer gF64 = f64.withName("f64_r").take(indices, arena);
            assertEquals(40.5, gF64.get(0));
            assertTrue(gF64.isNull(2));

            BooleanBuffer bool = BooleanBuffer.allocate(arena, "bool", 4);
            bool.copyFrom(new boolean[]{false, true, false, true});
            BooleanBuffer gBool = bool.withName("bool_r").take(indices, arena);
            assertTrue(gBool.get(0));
            assertTrue(gBool.isNull(2));

            UTF8Buffer utf8 = UTF8Buffer.allocate(arena, "utf8", new String[]{"a", "b", "c", "d"});
            UTF8Buffer gUtf8 = utf8.withName("utf8_r").take(indices, arena);
            assertArrayEquals(new String[]{"d", "b", null, "c"}, gUtf8.toArray());

            DictionaryBuffer dict = DictionaryBuffer.allocate(arena, "dict", new String[]{"X", "Y", "X", "Z"});
            DictionaryBuffer gDict = dict.withName("dict_r").take(indices, arena);
            assertArrayEquals(new String[]{"Z", "Y", null, "X"}, gDict.toArray());

            // ChunkedBuffer take() across all types
            assertEquals(40, ((Int32Buffer) ChunkedBuffer.of(i32.slice(0, 2), i32.slice(2, 2)).withName("c").take(indices, arena)).get(0));
            assertEquals(400L, ((Int64Buffer) ChunkedBuffer.of(i64.slice(0, 2), i64.slice(2, 2)).take(indices, arena)).get(0));
            assertEquals(4.5f, ((Float32Buffer) ChunkedBuffer.of(f32.slice(0, 2), f32.slice(2, 2)).take(indices, arena)).get(0));
            assertEquals(40.5, ((Float64Buffer) ChunkedBuffer.of(f64.slice(0, 2), f64.slice(2, 2)).take(indices, arena)).get(0));
            assertTrue(((BooleanBuffer) ChunkedBuffer.of(bool.slice(0, 2), bool.slice(2, 2)).take(indices, arena)).get(0));
            assertEquals("d", ((UTF8Buffer) ChunkedBuffer.of(utf8.slice(0, 2), utf8.slice(2, 2)).take(indices, arena)).getString(0));
            assertEquals("Z", ((DictionaryBuffer) ChunkedBuffer.of(dict.slice(0, 2), dict.slice(2, 2)).take(indices, arena)).getString(0));

            MemorySegment cArr = ArrowCExporter.exportArray(i32, arena);
            MemorySegment cSch = ArrowCExporter.exportSchema(i32, arena);
            ImportedDataBuffer imp = ArrowCImporter.importBuffer(cArr, cSch, arena).withName("imp_r");
            assertEquals("imp_r", imp.name());
            Int32Buffer gImp = (Int32Buffer) imp.take(indices, arena);
            assertEquals(40, gImp.get(0));
            assertTrue(gImp.isNull(2));
        }
    }

    @Test
    void testTableZeroCopyStructuralOpsHeadTailFilterAndTake() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer id = Int32Buffer.allocate(arena, "id", 5);
            id.copyFrom(new int[]{1, 2, 3, 4, 5});

            Float64Buffer price = Float64Buffer.allocate(arena, "price", 5);
            price.copyFrom(new double[]{10.0, 50.0, 20.0, 80.0, 30.0});

            DictionaryBuffer tier = DictionaryBuffer.allocate(arena, "tier",
                    new String[]{"Bronze", "Gold", "Silver", "Gold", "Silver"});

            Schema schema = new Schema(List.of(
                    new Field("id", DataType.INT32, false),
                    new Field("price", DataType.FLOAT64, false),
                    new Field("tier", DataType.DICTIONARY, true)
            ));
            assertTrue(schema.toString().contains("id"));
            assertEquals(schema.hashCode(), schema.select("id", "price", "tier").hashCode());

            Table table = new Table(schema, List.of(id, price, tier));

            assertEquals(2L, table.head(2).rowCount());
            assertEquals(4, ((Int32Buffer) table.tail(2).getColumn("id")).get(0));

            Table projected = table.select("id", "tier");
            assertEquals(2, projected.columnCount());

            Table renamed = projected.renameColumn("tier", "membership");
            assertEquals("membership", renamed.getColumn(1).name());

            Table withPrice = renamed.addColumn(new Field("price", DataType.FLOAT64, false), price);
            assertEquals(3, withPrice.columnCount());

            Int32Buffer newIds = Int32Buffer.allocate(arena, "id", 5);
            newIds.copyFrom(new int[]{10, 20, 30, 40, 50});
            Table replaced = withPrice.replaceColumn(0, new Field("id", DataType.INT32, false), newIds);
            assertEquals(10, ((Int32Buffer) replaced.getColumn("id")).get(0));

            Table dropped = replaced.dropColumn("price");
            assertEquals(2, dropped.columnCount());

            BooleanBuffer mask = BooleanBuffer.allocate(arena, "mask", 5);
            mask.copyFrom(new boolean[]{false, true, false, true, true});

            Table filtered = table.filter(mask, arena);
            assertEquals(3L, filtered.rowCount());
            assertArrayEquals(new int[]{2, 4, 5}, ((Int32Buffer) filtered.getColumn("id")).toArray());
            assertArrayEquals(new double[]{50.0, 80.0, 30.0}, ((Float64Buffer) filtered.getColumn("price")).toArray());
            assertArrayEquals(new String[]{"Gold", "Gold", "Silver"}, ((DictionaryBuffer) filtered.getColumn("tier")).toArray());

            assertThrows(IllegalArgumentException.class,
                    () -> table.filter(BooleanBuffer.allocate(arena, "bad_mask", 2), arena));
        }
    }
}