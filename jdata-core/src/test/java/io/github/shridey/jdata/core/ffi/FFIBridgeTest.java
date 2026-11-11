package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;
import io.github.shridey.jdata.core.internal.ArrowCStructs;
import io.github.shridey.jdata.core.layout.Field;
import io.github.shridey.jdata.core.layout.Schema;
import io.github.shridey.jdata.core.layout.Table;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FFIBridgeTest {

    @Test
    void testAllDataTypesRoundtripAndRawPointerReinterpretation() {
        try (Arena arena = Arena.ofConfined()) {
            // 1. Boolean with unaligned slice
            BooleanBuffer boolBuf = BooleanBuffer.allocate(arena, "bool_col", 12);
            boolBuf.copyFrom(new boolean[]{true, false, true, true, false, true, false, false, true, true, false, true});
            boolBuf.setNull(4);
            BooleanBuffer boolSlice = boolBuf.slice(3, 7);

            MemorySegment rawBoolArr = ArrowCExporter.exportArray(boolSlice, arena).reinterpret(0);
            MemorySegment rawBoolSch = ArrowCExporter.exportSchema(boolSlice, arena).reinterpret(0);

            ImportedDataBuffer impBool = ArrowCImporter.importBuffer(rawBoolArr, rawBoolSch, arena);
            assertEquals(DataType.BOOLEAN, impBool.type());
            assertEquals(7L, impBool.length());
            assertEquals(3L, impBool.offset());
            assertEquals(1L, impBool.nullCount());
            assertTrue(impBool.getBoolean(0));
            assertTrue(impBool.isNull(1));
            assertTrue(impBool.getBoolean(2));

            BooleanBuffer rebuiltBool = ((BooleanBufferBuilder) impBool.toBuilder(arena)).build();
            assertEquals(7L, rebuiltBool.length());
            assertEquals(1L, rebuiltBool.nullCount());
            assertTrue(rebuiltBool.isNull(1));

            // 2. UTF8 with unicode, nulls, and slice
            UTF8Buffer utf8Buf = UTF8Buffer.allocate(arena, "utf8_col",
                    new String[]{"skip", "Привет", null, "🚀Rocket", "tail"});
            UTF8Buffer utf8Slice = utf8Buf.slice(1, 3);

            MemorySegment rawUtf8Arr = ArrowCExporter.exportArray(utf8Slice, arena).reinterpret(0);
            MemorySegment rawUtf8Sch = ArrowCExporter.exportSchema(utf8Slice, arena).reinterpret(0);

            ImportedDataBuffer impUtf8 = ArrowCImporter.importBuffer(rawUtf8Arr, rawUtf8Sch, arena);
            assertEquals(DataType.UTF8, impUtf8.type());
            assertEquals(3L, impUtf8.length());
            assertEquals(1L, impUtf8.offset());
            assertEquals("Привет", impUtf8.getString(0));
            assertTrue(impUtf8.isNull(1));
            assertNull(impUtf8.getString(1));
            assertEquals("🚀Rocket", impUtf8.getString(2));

            UTF8Buffer rebuiltUtf8 = ((UTF8BufferBuilder) impUtf8.toBuilder(arena)).build();
            assertArrayEquals(new String[]{"Привет", null, "🚀Rocket"}, rebuiltUtf8.toArray());
        }
    }

    @Test
    void testImportForeignBufferWithNullValidityBitmapPointer() {
        try (Arena arena = Arena.ofConfined()) {
            Int64Buffer source = Int64Buffer.allocate(arena, "non_null_foreign", 5);
            source.copyFrom(new long[]{10L, 20L, 30L, 40L, 50L});

            MemorySegment cArray = ArrowCExporter.exportArray(source, arena);
            MemorySegment cSchema = ArrowCExporter.exportSchema(source, arena);

            // Simulate C++/Rust/DuckDB setting buffers[0] = NULL when null_count == 0
            MemorySegment buffersPtr = cArray.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_BUFFERS)
                    .reinterpret(2 * ValueLayout.ADDRESS.byteSize());
            buffersPtr.setAtIndex(ValueLayout.ADDRESS, 0, MemorySegment.NULL);

            ImportedDataBuffer imported = ArrowCImporter.importBuffer(cArray, cSchema, arena);
            assertEquals(0L, imported.nullCount());
            for (int i = 0; i < 5; i++) {
                assertFalse(imported.isNull(i));
                assertEquals((i + 1) * 10L, imported.getLong(i));
            }

            imported.setAllValid();
            assertThrows(UnsupportedOperationException.class, () -> imported.setNull(0));

            Int64Buffer materialized = ((Int64BufferBuilder) imported.toBuilder(arena)).build();
            assertArrayEquals(new long[]{10L, 20L, 30L, 40L, 50L}, materialized.toArray());
        }
    }

    @Test
    void testLongColumnNamesAndParameterizedFormatsAcrossFFI() {
        try (Arena arena = Arena.ofConfined()) {
            String longName = "col_" + "x".repeat(400);
            Int64Buffer tsCol = Int64Buffer.allocate(arena, longName, 2);
            tsCol.copyFrom(new long[]{1700000000000L, 1700000001000L});

            MemorySegment cArray = ArrowCExporter.exportArray(tsCol, arena);
            MemorySegment cSchema = ArrowCExporter.exportSchema(tsCol, arena);

            cSchema.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT,
                    arena.allocateFrom("tsm:America/Indiana/Indianapolis"));

            ImportedDataBuffer imported = ArrowCImporter.importBuffer(cArray, cSchema, arena);
            assertEquals(longName, imported.name());
            assertEquals(DataType.TIMESTAMP_MS, imported.type());
        }
    }

    @Test
    void testArenaClosureTriggersNativeReleaseAndPreventsUseAfterFree() {
        try (Arena producerArena = Arena.ofConfined()) {
            Int32Buffer col = Int32Buffer.allocate(producerArena, "lifecycle", 6);
            col.copyFrom(new int[]{1, 2, 3, 4, 5, 6});

            MemorySegment cArray = ArrowCExporter.exportArray(col, producerArena);
            MemorySegment cSchema = ArrowCExporter.exportSchema(col, producerArena);

            ImportedDataBuffer imported;
            try (Arena consumerArena = Arena.ofConfined()) {
                imported = ArrowCImporter.importBuffer(cArray, cSchema, consumerArena);

                // 1. Importer immediately releases the ArrowSchema struct
                assertEquals(MemorySegment.NULL, cSchema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE));

                // 2. Slices work while consumerArena is open
                ImportedDataBuffer slice = imported.slice(1, 3);
                assertEquals(2, slice.getInt(0));
                assertNotEquals(MemorySegment.NULL, cArray.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE));
            }

            // 3. Closing consumerArena automatically invokes the native ArrowArray release callback
            assertEquals(MemorySegment.NULL, cArray.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE));

            // 4. Attempting to read from the imported buffer after its Arena closed throws IllegalStateException instead of segfaulting
            assertThrows(IllegalStateException.class, () -> imported.getInt(0));
        }
    }

    @Test
    void testImportedDataBufferFloat32AndStrictBoundsChecks() {
        try (Arena arena = Arena.ofConfined()) {
            Float32Buffer f32 = Float32Buffer.allocate(arena, "f32", 3);
            f32.copyFrom(new float[]{1.25f, 2.5f, 3.75f});

            MemorySegment cArray = ArrowCExporter.exportArray(f32, arena);
            MemorySegment cSchema = ArrowCExporter.exportSchema(f32, arena);

            ImportedDataBuffer imported = ArrowCImporter.importBuffer(cArray, cSchema, arena);
            assertEquals(1.25f, imported.getFloat(0));
            assertEquals(2.5f, imported.getFloat(1));
            assertEquals(3.75f, imported.getFloat(2));

            Float32Buffer rebuilt = ((Float32BufferBuilder) imported.toBuilder(arena)).build();
            assertArrayEquals(new float[]{1.25f, 2.5f, 3.75f}, rebuilt.toArray());

            assertThrows(IndexOutOfBoundsException.class, () -> imported.getFloat(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> imported.getFloat(3));
            assertThrows(IllegalStateException.class, () -> imported.getInt(0));
            assertThrows(IllegalStateException.class, () -> imported.getDouble(0));
        }
    }

    @Test
    void testTableExportImportPreservesFieldNullabilityAndData() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer ids = Int32Buffer.allocate(arena, "id", 3);
            ids.copyFrom(new int[]{101, 102, 103});

            Float64Buffer scores = Float64Buffer.allocate(arena, "score", 3);
            scores.copyFrom(new double[]{98.5, 87.25, 92.0});

            UTF8Buffer names = UTF8Buffer.allocate(arena, "user", new String[]{"Alice", null, "Charlie"});

            Schema schema = new Schema(List.of(
                    new Field("id", DataType.INT32, false),
                    new Field("score", DataType.FLOAT64, false),
                    new Field("user", DataType.UTF8, true)
            ));

            Table table = new Table(schema, List.of(ids, scores, names));
            MemorySegment cTableArray = ArrowCExporter.exportTableArray(table, arena).reinterpret(0);
            MemorySegment cTableSchema = ArrowCExporter.exportTableSchema(table, arena).reinterpret(0);

            Table importedTable = ArrowCImporter.importTable(cTableArray, cTableSchema, arena);
            assertEquals(3L, importedTable.rowCount());
            assertEquals(3, importedTable.columnCount());

            assertFalse(importedTable.schema().getField(0).nullable(), "Non-nullable flag must survive FFI");
            assertFalse(importedTable.schema().getField(1).nullable(), "Non-nullable flag must survive FFI");
            assertTrue(importedTable.schema().getField(2).nullable(), "Nullable flag must survive FFI");

            ImportedDataBuffer impIds = (ImportedDataBuffer) importedTable.getColumn("id");
            ImportedDataBuffer impScores = (ImportedDataBuffer) importedTable.getColumn("score");
            ImportedDataBuffer impUsers = (ImportedDataBuffer) importedTable.getColumn("user");

            assertEquals(101, impIds.getInt(0));
            assertEquals(98.5, impScores.getDouble(0));
            assertEquals(87.25, impScores.getDouble(1));
            assertEquals(92.0, impScores.getDouble(2));
            assertEquals("Alice", impUsers.getString(0));
            assertTrue(impUsers.isNull(1));
            assertEquals("Charlie", impUsers.getString(2));
        }
    }
}