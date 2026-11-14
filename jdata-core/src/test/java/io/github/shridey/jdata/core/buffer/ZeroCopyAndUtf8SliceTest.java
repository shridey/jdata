package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.ffi.ArrowCExporter;
import io.github.shridey.jdata.core.ffi.ArrowCImporter;
import io.github.shridey.jdata.core.ffi.ImportedDataBuffer;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ZeroCopyAndUtf8SliceTest {

    @Test
    void testBuilderO1NullCountTrackingAcrossAllBuilders() {
        try (Arena arena = Arena.ofConfined()) {
            Int32BufferBuilder i32b = new Int32BufferBuilder(arena, "i32", 8)
                    .append(1).appendNull().append(3);
            assertEquals(1L, i32b.nullCount());
            assertEquals(1L, i32b.build().nullCount());

            Int64BufferBuilder i64b = new Int64BufferBuilder(arena, "i64", 8)
                    .append(10L).appendNull().append(20L).appendNull();
            assertEquals(2L, i64b.nullCount());
            assertEquals(2L, i64b.build().nullCount());

            Float32BufferBuilder f32b = new Float32BufferBuilder(arena, "f32", 8)
                    .appendNull().append(1.5f);
            assertEquals(1L, f32b.nullCount());
            assertEquals(1L, f32b.build().nullCount());

            Float64BufferBuilder f64b = new Float64BufferBuilder(arena, "f64", 8)
                    .append(2.5).appendNull();
            assertEquals(1L, f64b.nullCount());
            assertEquals(1L, f64b.build().nullCount());

            BooleanBufferBuilder bb = new BooleanBufferBuilder(arena, "bool", 8)
                    .append(true).appendNull();
            assertEquals(1L, bb.nullCount());
            assertEquals(1L, bb.build().nullCount());

            UTF8BufferBuilder ub = new UTF8BufferBuilder(arena, "utf8", 8)
                    .append("a").appendNull();
            assertEquals(1L, ub.nullCount());
            assertEquals(1L, ub.build().nullCount());

            DictionaryBufferBuilder db = new DictionaryBufferBuilder(arena, "dict", 8)
                    .append("x").appendNull().append("x");
            assertEquals(1L, db.nullCount());
            assertEquals(1L, db.build().nullCount());
        }
    }

    @Test
    void testUtf8ZeroAllocationByteSlicesAndSimdComparisons() {
        try (Arena arena = Arena.ofConfined()) {
            UTF8Buffer buf = UTF8Buffer.allocate(arena, "words",
                    new String[]{"Alpha", "Beta", null, "Alpha", "Alphabet", ""});

            assertEquals(5, buf.getByteLength(0));
            assertEquals(4, buf.getByteLength(1));
            assertEquals(0, buf.getByteLength(2));
            assertEquals(0, buf.getByteLength(5));

            MemorySegment slice0 = buf.getByteSlice(0);
            assertEquals(5L, slice0.byteSize());
            assertEquals(MemorySegment.NULL, buf.getByteSlice(2));
            assertEquals(0L, buf.getByteSlice(5).byteSize());

            MemorySegment targetAlpha = MemorySegment.ofArray("Alpha".getBytes(StandardCharsets.UTF_8));
            MemorySegment targetEmpty = MemorySegment.ofArray(new byte[0]);
            assertTrue(buf.equalsBytes(0, targetAlpha));
            assertFalse(buf.equalsBytes(1, targetAlpha));
            assertFalse(buf.equalsBytes(2, targetAlpha));
            assertFalse(buf.equalsBytes(4, targetAlpha));
            assertTrue(buf.equalsBytes(5, targetEmpty));

            // Lexicographical unsigned UTF-8 comparisons
            assertEquals(0, buf.compareBytes(0, buf, 3));
            assertTrue(buf.compareBytes(0, buf, 1) < 0); // "Alpha" < "Beta"
            assertTrue(buf.compareBytes(1, buf, 0) > 0); // "Beta" > "Alpha"
            assertTrue(buf.compareBytes(0, buf, 4) < 0); // "Alpha" < "Alphabet"
            assertTrue(buf.compareBytes(4, buf, 0) > 0); // "Alphabet" > "Alpha"
            assertTrue(buf.compareBytes(1, buf, 2) < 0); // Non-null sorts before null
            assertTrue(buf.compareBytes(2, buf, 1) > 0); // Null sorts after non-null
            assertEquals(0, buf.compareBytes(2, buf, 2)); // Null == Null
        }
    }

    @Test
    void testImportedDataBufferZeroCopyNativeViewsAndFastTake() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer i32 = Int32Buffer.allocate(arena, "i32", 3);
            i32.copyFrom(new int[]{10, 20, 30});
            ImportedDataBuffer impI32 = ArrowCImporter.importBuffer(
                    ArrowCExporter.exportArray(i32, arena), ArrowCExporter.exportSchema(i32, arena), arena);
            assertEquals(20, impI32.asInt32Buffer().get(1));
            assertInstanceOf(Int32Buffer.class, impI32.toNativeView());
            assertThrows(IllegalStateException.class, impI32::asInt64Buffer);
            assertThrows(IllegalStateException.class, impI32::asFloat32Buffer);
            assertThrows(IllegalStateException.class, impI32::asFloat64Buffer);
            assertThrows(IllegalStateException.class, impI32::asBooleanBuffer);
            assertThrows(IllegalStateException.class, impI32::asUTF8Buffer);
            assertThrows(IllegalStateException.class, impI32::asDictionaryBuffer);

            Int64Buffer i64 = Int64Buffer.allocate(arena, "i64", 2);
            i64.copyFrom(new long[]{100L, 200L});
            ImportedDataBuffer impI64 = ArrowCImporter.importBuffer(
                    ArrowCExporter.exportArray(i64, arena), ArrowCExporter.exportSchema(i64, arena), arena);
            assertEquals(200L, impI64.asInt64Buffer().get(1));
            assertInstanceOf(Int64Buffer.class, impI64.toNativeView());
            assertThrows(IllegalStateException.class, impI64::asInt32Buffer);

            Float32Buffer f32 = Float32Buffer.allocate(arena, "f32", 2);
            f32.copyFrom(new float[]{1.5f, 2.5f});
            ImportedDataBuffer impF32 = ArrowCImporter.importBuffer(
                    ArrowCExporter.exportArray(f32, arena), ArrowCExporter.exportSchema(f32, arena), arena);
            assertEquals(2.5f, impF32.asFloat32Buffer().get(1));
            assertInstanceOf(Float32Buffer.class, impF32.toNativeView());

            Float64Buffer f64 = Float64Buffer.allocate(arena, "f64", 2);
            f64.copyFrom(new double[]{10.5, 20.5});
            ImportedDataBuffer impF64 = ArrowCImporter.importBuffer(
                    ArrowCExporter.exportArray(f64, arena), ArrowCExporter.exportSchema(f64, arena), arena);
            assertEquals(20.5, impF64.asFloat64Buffer().get(1));
            assertInstanceOf(Float64Buffer.class, impF64.toNativeView());

            BooleanBuffer bool = BooleanBuffer.allocate(arena, "bool", 2);
            bool.copyFrom(new boolean[]{false, true});
            ImportedDataBuffer impBool = ArrowCImporter.importBuffer(
                    ArrowCExporter.exportArray(bool, arena), ArrowCExporter.exportSchema(bool, arena), arena);
            assertTrue(impBool.asBooleanBuffer().get(1));
            assertInstanceOf(BooleanBuffer.class, impBool.toNativeView());

            UTF8Buffer utf8 = UTF8Buffer.allocate(arena, "utf8", new String[]{"X", "Y"});
            ImportedDataBuffer impUtf8 = ArrowCImporter.importBuffer(
                    ArrowCExporter.exportArray(utf8, arena), ArrowCExporter.exportSchema(utf8, arena), arena);
            assertEquals("Y", impUtf8.asUTF8Buffer().getString(1));
            assertInstanceOf(UTF8Buffer.class, impUtf8.toNativeView());

            DictionaryBuffer dict = DictionaryBuffer.allocate(arena, "dict", new String[]{"A", "B", "A"});
            ImportedDataBuffer impDict = ArrowCImporter.importBuffer(
                    ArrowCExporter.exportArray(dict, arena), ArrowCExporter.exportSchema(dict, arena), arena);
            assertEquals("B", impDict.asDictionaryBuffer().getString(1));
            assertInstanceOf(DictionaryBuffer.class, impDict.toNativeView());

            Int32Buffer sel = Int32Buffer.allocate(arena, "sel", 2);
            sel.copyFrom(new int[]{2, 0});
            DataBuffer gathered = impDict.take(sel, arena);
            assertArrayEquals(new String[]{"A", "A"}, ((DictionaryBuffer) gathered).toArray());
        }
    }

    @Test
    void testDirectChunkedUtf8AndDictionaryTakeWithMonotonicAndRandomIndices() {
        try (Arena arena = Arena.ofConfined()) {
            UTF8Buffer u1 = UTF8Buffer.allocate(arena, "u", new String[]{"row0", null});
            UTF8Buffer u2 = UTF8Buffer.allocate(arena, "u", new String[]{"row2", "row3"});
            UTF8Buffer u3 = UTF8Buffer.allocate(arena, "u", new String[]{"row4", "row5"});
            ChunkedBuffer chunkedUtf8 = ChunkedBuffer.of(u1, u2, u3);

            DictionaryBuffer d1 = DictionaryBuffer.allocate(arena, "d", new String[]{"IN", "US"});
            DictionaryBuffer d2 = DictionaryBuffer.allocate(arena, "d", new String[]{null, "UK"});
            DictionaryBuffer d3 = DictionaryBuffer.allocate(arena, "d", new String[]{"DE", "IN"});
            ChunkedBuffer chunkedDict = ChunkedBuffer.of(d1, d2, d3);

            // Non-monotonic selection vector with a null slot (exercises hint hit, nextChunk hit, and binarySearch fallback)
            Int32Buffer sel = Int32Buffer.allocate(arena, "sel", 6);
            sel.copyFrom(new int[]{0, 1, 2, 5, 0, 3});
            sel.setNull(4);

            UTF8Buffer takenUtf8 = (UTF8Buffer) chunkedUtf8.take(sel, arena);
            assertArrayEquals(new String[]{"row0", null, "row2", "row5", null, "row3"}, takenUtf8.toArray());

            DictionaryBuffer takenDict = (DictionaryBuffer) chunkedDict.take(sel, arena);
            assertArrayEquals(new String[]{"IN", "US", null, "IN", null, "UK"}, takenDict.toArray());
        }
    }
}