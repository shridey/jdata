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

class ChunkedBufferTest {

    @Test
    void testChunkedBufferBinarySearchLookupsAndFlatteningAcrossAllTypes() {
        try (Arena arena = Arena.ofConfined()) {
            // 1. Int32 chunks + appendChunk + nested flattening
            Int32Buffer i1 = Int32Buffer.allocate(arena, "ids", 3);
            i1.copyFrom(new int[]{10, 20, 30});
            Int32Buffer i2 = Int32Buffer.allocate(arena, "ids", 2);
            i2.copyFrom(new int[]{40, 50});
            Int32Buffer i3 = Int32Buffer.allocate(arena, "ids", 4);
            i3.copyFrom(new int[]{60, 70, 80, 90});

            ChunkedBuffer nested = ChunkedBuffer.of(i1, i2);
            ChunkedBuffer chunkedInt = nested.appendChunk(i3);

            assertEquals("ids", chunkedInt.name());
            assertEquals(DataType.INT32, chunkedInt.type());
            assertEquals(arena, chunkedInt.arena());
            assertEquals(0L, chunkedInt.offset());
            assertEquals(3, chunkedInt.chunkCount(), "Nested ChunkedBuffers must be flattened");
            assertEquals(3, chunkedInt.chunks().size());
            assertSame(i2, chunkedInt.chunk(1));
            assertEquals(9L, chunkedInt.length());

            for (int idx = 0; idx < 9; idx++) {
                assertEquals((idx + 1) * 10, chunkedInt.getInt(idx));
            }

            // Discontiguous multi-chunk buffer must reject direct single-segment buffer() access
            assertThrows(UnsupportedOperationException.class, chunkedInt::bufferCount);
            assertThrows(UnsupportedOperationException.class, () -> chunkedInt.buffer(0));

            // 2. Int64, Float32, Float64, Boolean, UTF8, and Dictionary typed getters
            Int64Buffer l1 = Int64Buffer.allocate(arena, "l", 2);
            l1.copyFrom(new long[]{100L, 200L});
            ChunkedBuffer chunkedLong = ChunkedBuffer.of(l1);
            assertEquals(2, chunkedLong.bufferCount());
            assertNotNull(chunkedLong.buffer(1));
            assertEquals(200L, chunkedLong.getLong(1));
            assertSame(l1, chunkedLong.compact(arena), "Single unsliced chunk must compact in zero-copy");

            Float32Buffer f1 = Float32Buffer.allocate(arena, "f", 2);
            f1.copyFrom(new float[]{1.5f, 2.5f});
            assertEquals(2.5f, ChunkedBuffer.of(f1).getFloat(1));

            Float64Buffer d1 = Float64Buffer.allocate(arena, "d", 2);
            d1.copyFrom(new double[]{3.5, 4.5});
            assertEquals(4.5, ChunkedBuffer.of(d1).getDouble(1));

            BooleanBuffer b1 = BooleanBuffer.allocate(arena, "b", 2);
            b1.copyFrom(new boolean[]{false, true});
            assertTrue(ChunkedBuffer.of(b1).getBoolean(1));
        }
    }

    @Test
    void testCrossChunkZeroCopySlicingAndNullManagement() {
        try (Arena arena = Arena.ofConfined()) {
            UTF8Buffer c0 = UTF8Buffer.allocate(arena, "words", new String[]{"w0", "w1", "w2"});
            UTF8Buffer c1 = UTF8Buffer.allocate(arena, "words", new String[]{"w3", null, "w5", "w6"});
            UTF8Buffer c2 = UTF8Buffer.allocate(arena, "words", new String[]{"w7", "w8", "w9"});

            ChunkedBuffer full = ChunkedBuffer.of(c0, c1, c2);
            assertEquals(10L, full.length());
            assertEquals(1L, full.nullCount());
            assertTrue(full.isNull(4));

            // Mutate null state across chunk boundaries
            full.setNull(8); // Index 8 -> c2 local index 1 ("w8")
            assertEquals(2L, full.nullCount());
            assertTrue(full.isNull(8));

            // Zero-copy slice spanning from middle of c0 (index 1) to middle of c2 (index 8, length 8 -> [1..9))
            ChunkedBuffer crossSlice = full.slice(1, 8);
            assertEquals(8L, crossSlice.length());
            assertEquals(3, crossSlice.chunkCount());
            assertEquals(2L, crossSlice.nullCount());

            assertEquals("w1", crossSlice.getString(0));
            assertEquals("w2", crossSlice.getString(1));
            assertEquals("w3", crossSlice.getString(2));
            assertNull(crossSlice.getString(3)); // was index 4
            assertEquals("w6", crossSlice.getString(5));
            assertEquals("w7", crossSlice.getString(6));
            assertNull(crossSlice.getString(7)); // was index 8

            // Compact the cross-chunk slice into a single contiguous UTF8Buffer
            UTF8Buffer compacted = (UTF8Buffer) crossSlice.compact(arena);
            assertEquals(8L, compacted.length());
            assertEquals(2L, compacted.nullCount());
            assertArrayEquals(
                    new String[]{"w1", "w2", "w3", null, "w5", "w6", "w7", null},
                    compacted.toArray()
            );

            // Healing all nulls across chunks
            full.setAllValid();
            assertEquals(0L, full.nullCount());
            assertFalse(full.isNull(4));
        }
    }

    @Test
    void testZeroCopyTableConcatenationCompactionAndFFIRoundtrip() {
        try (Arena arena = Arena.ofConfined()) {
            Schema schema = new Schema(List.of(
                    new Field("id", DataType.INT32, false),
                    new Field("region", DataType.DICTIONARY, true)
            ));

            Int32Buffer idBatch1 = Int32Buffer.allocate(arena, "id", 2);
            idBatch1.copyFrom(new int[]{1, 2});
            DictionaryBuffer regBatch1 = DictionaryBuffer.allocate(arena, "region", new String[]{"APAC", "EMEA"});
            Table t1 = new Table(schema, List.of(idBatch1, regBatch1));

            Int32Buffer idBatch2 = Int32Buffer.allocate(arena, "id", 3);
            idBatch2.copyFrom(new int[]{3, 4, 5});
            DictionaryBuffer regBatch2 = DictionaryBuffer.allocate(arena, "region", new String[]{"AMER", "APAC", null});
            Table t2 = new Table(schema, List.of(idBatch2, regBatch2));

            // 1. O(1) Zero-Copy Table.concat
            Table concatenated = t1.concat(t2);
            assertEquals(5L, concatenated.rowCount());
            assertInstanceOf(ChunkedBuffer.class, concatenated.getColumn("id"));
            assertInstanceOf(ChunkedBuffer.class, concatenated.getColumn("region"));

            ChunkedBuffer chunkedRegion = (ChunkedBuffer) concatenated.getColumn("region");
            assertEquals("APAC", chunkedRegion.getString(0));
            assertEquals("AMER", chunkedRegion.getString(2));
            assertNull(chunkedRegion.getString(4));

            // 2. Export chunked Table directly over FFI (ArrowCExporter auto-compacts into structArena)
            MemorySegment cArr = ArrowCExporter.exportTableArray(concatenated, arena).reinterpret(0);
            MemorySegment cSch = ArrowCExporter.exportTableSchema(concatenated, arena).reinterpret(0);

            Table imported = ArrowCImporter.importTable(cArr, cSch, arena);
            assertEquals(5L, imported.rowCount());
            ImportedDataBuffer impId = (ImportedDataBuffer) imported.getColumn("id");
            ImportedDataBuffer impReg = (ImportedDataBuffer) imported.getColumn("region");
            assertEquals(5, impId.getInt(4));
            assertEquals("AMER", impReg.getString(2));
            assertTrue(impReg.isNull(4));

            // 3. Explicit Table.compact(arena)
            Table compactedTable = concatenated.compact(arena);
            assertInstanceOf(Int32Buffer.class, compactedTable.getColumn("id"));
            assertInstanceOf(DictionaryBuffer.class, compactedTable.getColumn("region"));
            DictionaryBuffer unifiedDict = (DictionaryBuffer) compactedTable.getColumn("region");
            assertEquals(3L, unifiedDict.cardinality()); // "APAC", "EMEA", "AMER"
            assertArrayEquals(new String[]{"APAC", "EMEA", "AMER", "APAC", null}, unifiedDict.toArray());
        }
    }
}