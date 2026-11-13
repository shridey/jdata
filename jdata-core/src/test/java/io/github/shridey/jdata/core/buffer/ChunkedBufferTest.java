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
            assertEquals(3, chunkedInt.chunkCount());
            assertEquals(3, chunkedInt.chunks().size());
            assertSame(i2, chunkedInt.chunk(1));
            assertEquals(9L, chunkedInt.length());

            for (int idx = 0; idx < 9; idx++) {
                assertEquals((idx + 1) * 10, chunkedInt.getInt(idx));
            }

            Int32Buffer appendedFromChunked = new Int32BufferBuilder(arena, "from_chunked", 4)
                    .appendImportedBlock(chunkedInt, 2, 5)
                    .build();
            assertArrayEquals(new int[]{30, 40, 50, 60, 70}, appendedFromChunked.toArray());

            assertThrows(UnsupportedOperationException.class, chunkedInt::bufferCount);
            assertThrows(UnsupportedOperationException.class, () -> chunkedInt.buffer(0));

            Int64Buffer l1 = Int64Buffer.allocate(arena, "l", 2);
            l1.copyFrom(new long[]{100L, 200L});
            ChunkedBuffer chunkedLong = ChunkedBuffer.of(l1);
            assertEquals(2, chunkedLong.bufferCount());
            assertNotNull(chunkedLong.buffer(1));
            assertEquals(200L, chunkedLong.getLong(1));
            assertSame(l1, chunkedLong.compact(arena));

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
    void testZeroCopyWrapFactoriesAcrossAllPhysicalBuffers() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer i32 = Int32Buffer.allocate(arena, "i32", 4);
            i32.copyFrom(new int[]{1, 2, 3, 4});
            Int32Buffer wrappedI32 = Int32Buffer.wrap("i32_w", arena, 3, 1, i32.buffer(0), i32.buffer(1));
            assertArrayEquals(new int[]{2, 3, 4}, wrappedI32.toArray());

            Int64Buffer i64 = Int64Buffer.allocate(arena, "i64", 3);
            i64.copyFrom(new long[]{10L, 20L, 30L});
            Int64Buffer wrappedI64 = Int64Buffer.wrap("i64_w", arena, 2, 1, i64.buffer(0), i64.buffer(1));
            assertArrayEquals(new long[]{20L, 30L}, wrappedI64.toArray());

            Float32Buffer f32 = Float32Buffer.allocate(arena, "f32", 3);
            f32.copyFrom(new float[]{1.0f, 2.0f, 3.0f});
            Float32Buffer wrappedF32 = Float32Buffer.wrap("f32_w", arena, 2, 1, f32.buffer(0), f32.buffer(1));
            assertArrayEquals(new float[]{2.0f, 3.0f}, wrappedF32.toArray());

            Float64Buffer f64 = Float64Buffer.allocate(arena, "f64", 3);
            f64.copyFrom(new double[]{10.5, 20.5, 30.5});
            Float64Buffer wrappedF64 = Float64Buffer.wrap("f64_w", arena, 2, 1, f64.buffer(0), f64.buffer(1));
            assertArrayEquals(new double[]{20.5, 30.5}, wrappedF64.toArray());

            BooleanBuffer bool = BooleanBuffer.allocate(arena, "bool", 4);
            bool.copyFrom(new boolean[]{false, true, false, true});
            BooleanBuffer wrappedBool = BooleanBuffer.wrap("bool_w", arena, 3, 1, bool.buffer(0), bool.buffer(1));
            assertArrayEquals(new boolean[]{true, false, true}, wrappedBool.toArray());

            UTF8Buffer utf8 = UTF8Buffer.allocate(arena, "utf8", new String[]{"A", "B", "C"});
            UTF8Buffer wrappedUtf8 = UTF8Buffer.wrap("utf8_w", arena, 2, 1, utf8.buffer(0), utf8.buffer(1), utf8.buffer(2));
            assertArrayEquals(new String[]{"B", "C"}, wrappedUtf8.toArray());

            DictionaryBuffer dict = DictionaryBuffer.allocate(arena, "dict", new String[]{"X", "Y", "X"});
            DictionaryBuffer wrappedDict = DictionaryBuffer.wrap(
                    "dict_w", arena, 2, 1, dict.buffer(0), dict.buffer(1), dict.dictionary()
            );
            assertArrayEquals(new String[]{"Y", "X"}, wrappedDict.toArray());
        }
    }

    @Test
    void testChunkedBufferBuilderAutoRolloverForAllTypesAndUtf8ByteLimits() {
        try (Arena arena = Arena.ofConfined()) {
            ChunkedBufferBuilder defaultBuilder = new ChunkedBufferBuilder(arena, "def", DataType.INT32);
            assertEquals("def", defaultBuilder.name());
            assertEquals(DataType.INT32, defaultBuilder.type());
            assertEquals(ChunkedBufferBuilder.DEFAULT_MAX_CHUNK_ROWS, defaultBuilder.maxChunkRows());
            assertEquals(ChunkedBufferBuilder.DEFAULT_MAX_UTF8_CHUNK_BYTES, defaultBuilder.maxUtf8ChunkBytes());

            // Chain flushChunk() explicitly to verify manual chunk boundary flushing
            ChunkedBuffer manuallyFlushed = defaultBuilder.append(10).flushChunk().append(20).build();
            assertEquals(2, manuallyFlushed.chunkCount());
            assertEquals(20, manuallyFlushed.getInt(1));

            ChunkedBuffer i32Chunked = new ChunkedBufferBuilder(arena, "i32", DataType.INT32, 3)
                    .append(1)
                    .appendAll(new int[]{2, 3, 4, 5, 6, 7})
                    .appendNull()
                    .build();
            assertEquals(8L, i32Chunked.length());
            assertEquals(3, i32Chunked.chunkCount());
            assertEquals(7, i32Chunked.getInt(6));
            assertTrue(i32Chunked.isNull(7));

            ChunkedBuffer i64Chunked = new ChunkedBufferBuilder(arena, "i64", DataType.INT64, 2)
                    .append(10L)
                    .appendAll(new long[]{20L, 30L, 40L})
                    .build();
            assertEquals(2, i64Chunked.chunkCount());
            assertEquals(40L, i64Chunked.getLong(3));

            ChunkedBuffer f32Chunked = new ChunkedBufferBuilder(arena, "f32", DataType.FLOAT32, 2)
                    .append(1.0f)
                    .appendAll(new float[]{2.0f, 3.0f})
                    .build();
            assertEquals(2, f32Chunked.chunkCount());
            assertEquals(3.0f, f32Chunked.getFloat(2));

            ChunkedBuffer f64Chunked = new ChunkedBufferBuilder(arena, "f64", DataType.FLOAT64, 2)
                    .append(1.5)
                    .appendAll(new double[]{2.5, 3.5})
                    .build();
            assertEquals(2, f64Chunked.chunkCount());
            assertEquals(3.5, f64Chunked.getDouble(2));

            ChunkedBuffer boolChunked = new ChunkedBufferBuilder(arena, "bool", DataType.BOOLEAN, 2)
                    .append(true)
                    .appendAll(new boolean[]{false, true})
                    .build();
            assertEquals(2, boolChunked.chunkCount());
            assertTrue(boolChunked.getBoolean(2));

            ChunkedBufferBuilder utf8Stream = new ChunkedBufferBuilder(arena, "utf8_stream", DataType.UTF8, 100, 10);
            utf8Stream.appendAll(new String[]{"123456", "7890", "ABCD", "EFGHIJ"});
            UTF8Buffer blockSrc = UTF8Buffer.allocate(arena, "src", new String[]{"12345", "67890", "XYZ"});
            utf8Stream.appendBlock(blockSrc, 0, blockSrc.length());
            assertEquals(7L, utf8Stream.length());

            ChunkedBuffer utf8Chunked = utf8Stream.build();
            assertEquals(4, utf8Chunked.chunkCount());
            assertEquals("123456", utf8Chunked.getString(0));
            assertEquals("EFGHIJ", utf8Chunked.getString(3));
            assertEquals("XYZ", utf8Chunked.getString(6));
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

            full.setNull(8);
            assertEquals(2L, full.nullCount());
            assertTrue(full.isNull(8));

            ChunkedBuffer crossSlice = full.slice(1, 8);
            assertEquals(8L, crossSlice.length());
            assertEquals(3, crossSlice.chunkCount());
            assertEquals(2L, crossSlice.nullCount());

            UTF8Buffer compacted = (UTF8Buffer) crossSlice.compact(arena);
            assertArrayEquals(
                    new String[]{"w1", "w2", "w3", null, "w5", "w6", "w7", null},
                    compacted.toArray()
            );

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

            Table concatenated = t1.concat(t2);
            assertEquals(5L, concatenated.rowCount());

            MemorySegment cArr = ArrowCExporter.exportTableArray(concatenated, arena).reinterpret(0);
            MemorySegment cSch = ArrowCExporter.exportTableSchema(concatenated, arena).reinterpret(0);

            Table imported = ArrowCImporter.importTable(cArr, cSch, arena);
            assertEquals(5L, imported.rowCount());
            ImportedDataBuffer impId = (ImportedDataBuffer) imported.getColumn("id");
            ImportedDataBuffer impReg = (ImportedDataBuffer) imported.getColumn("region");
            assertEquals(5, impId.getInt(4));
            assertEquals("AMER", impReg.getString(2));
            assertTrue(impReg.isNull(4));

            Table emptySliced = concatenated.slice(0, 0);
            MemorySegment emptyArr = ArrowCExporter.exportTableArray(emptySliced, arena);
            MemorySegment emptySch = ArrowCExporter.exportTableSchema(emptySliced, arena);
            Table importedEmpty = ArrowCImporter.importTable(emptyArr, emptySch, arena);
            assertEquals(0L, importedEmpty.rowCount());
            assertEquals(DataType.DICTIONARY, importedEmpty.schema().getField(1).type());

            Table compactedTable = concatenated.compact(arena);
            DictionaryBuffer unifiedDict = (DictionaryBuffer) compactedTable.getColumn("region");
            assertEquals(3L, unifiedDict.cardinality());
            assertArrayEquals(new String[]{"APAC", "EMEA", "AMER", "APAC", null}, unifiedDict.toArray());
        }
    }
}