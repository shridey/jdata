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

            // Simulate raw zero-length pointers coming from a native C downcall (.reinterpret(0))
            MemorySegment rawBoolArr = ArrowCExporter.exportArray(boolSlice, arena).reinterpret(0);
            MemorySegment rawBoolSch = ArrowCExporter.exportSchema(boolSlice, arena).reinterpret(0);

            try (ImportedDataBuffer impBool = ArrowCImporter.importBuffer(rawBoolArr, rawBoolSch)) {
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
            }

            // 2. UTF8 with unicode, nulls, and slice
            UTF8Buffer utf8Buf = UTF8Buffer.allocate(arena, "utf8_col",
                    new String[]{"skip", "Привет", null, "🚀Rocket", "tail"});
            UTF8Buffer utf8Slice = utf8Buf.slice(1, 3);

            MemorySegment rawUtf8Arr = ArrowCExporter.exportArray(utf8Slice, arena).reinterpret(0);
            MemorySegment rawUtf8Sch = ArrowCExporter.exportSchema(utf8Slice, arena).reinterpret(0);

            try (ImportedDataBuffer impUtf8 = ArrowCImporter.importBuffer(rawUtf8Arr, rawUtf8Sch)) {
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

            try (ImportedDataBuffer imported = ArrowCImporter.importBuffer(cArray, cSchema)) {
                assertEquals(0L, imported.nullCount());
                for (int i = 0; i < 5; i++) {
                    assertFalse(imported.isNull(i));
                    assertEquals((i + 1) * 10L, imported.getLong(i));
                }

                // setAllValid() should succeed as a no-op, while setNull() must reject missing bitmap
                imported.setAllValid();
                assertThrows(UnsupportedOperationException.class, () -> imported.setNull(0));

                // toBuilder must cleanly materialize into a standard Int64Buffer with a fresh bitmap
                Int64Buffer materialized = ((Int64BufferBuilder) imported.toBuilder(arena)).build();
                assertArrayEquals(new long[]{10L, 20L, 30L, 40L, 50L}, materialized.toArray());
            }
        }
    }

    @Test
    void testLongColumnNamesAndParameterizedFormatsAcrossFFI() {
        try (Arena arena = Arena.ofConfined()) {
            String longName = "col_" + "x".repeat(400); // Exceeds legacy 256-byte limit
            Int64Buffer tsCol = Int64Buffer.allocate(arena, longName, 2);
            tsCol.copyFrom(new long[]{1700000000000L, 1700000001000L});

            MemorySegment cArray = ArrowCExporter.exportArray(tsCol, arena);
            MemorySegment cSchema = ArrowCExporter.exportSchema(tsCol, arena);

            // Override format with a >16-byte parameterized Arrow timestamp string
            cSchema.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT,
                    arena.allocateFrom("tsm:America/Indiana/Indianapolis"));

            try (ImportedDataBuffer imported = ArrowCImporter.importBuffer(cArray, cSchema)) {
                assertEquals(longName, imported.name());
                assertEquals(DataType.TIMESTAMP_MS, imported.type());
            }
        }
    }

    @Test
    void testIdempotentCloseSchemaReleaseAndNonOwningSliceLifecycle() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer col = Int32Buffer.allocate(arena, "lifecycle", 6);
            col.copyFrom(new int[]{1, 2, 3, 4, 5, 6});

            MemorySegment cArray = ArrowCExporter.exportArray(col, arena);
            MemorySegment cSchema = ArrowCExporter.exportSchema(col, arena);

            ImportedDataBuffer imported = ArrowCImporter.importBuffer(cArray, cSchema);

            // 1. Importer must have immediately released the ArrowSchema struct
            assertEquals(MemorySegment.NULL, cSchema.get(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE));

            // 2. Closing a slice must NOT release the parent ArrowArray struct
            ImportedDataBuffer slice = imported.slice(1, 3);
            assertEquals(2, slice.getInt(0));
            slice.close();
            assertNotEquals(MemorySegment.NULL, cArray.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE),
                    "Closing a non-owning slice must not release the underlying native ArrowArray");

            // 3. Closing the owning buffer releases the native struct; subsequent close() calls are safe no-ops
            imported.close();
            assertEquals(MemorySegment.NULL, cArray.get(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE));
            assertDoesNotThrow(imported::close);
        }
    }

    @Test
    void testImportedDataBufferStrictBoundsAndTypeChecks() {
        try (Arena arena = Arena.ofConfined()) {
            Float32Buffer f32 = Float32Buffer.allocate(arena, "f32", 3);
            f32.copyFrom(new float[]{1.0f, 2.0f, 3.0f});

            MemorySegment cArray = ArrowCExporter.exportArray(f32, arena);
            MemorySegment cSchema = ArrowCExporter.exportSchema(f32, arena);

            try (ImportedDataBuffer imported = ArrowCImporter.importBuffer(cArray, cSchema)) {
                assertThrows(IndexOutOfBoundsException.class, () -> imported.getFloat(-1));
                assertThrows(IndexOutOfBoundsException.class, () -> imported.getFloat(3));
                assertThrows(IllegalStateException.class, () -> imported.getInt(0));
                assertThrows(IllegalStateException.class, () -> imported.getDouble(0));
            }
        }
    }

    @Test
    void testTableExportImportPreservesFieldNullabilityAndData() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer ids = Int32Buffer.allocate(arena, "id", 3);
            ids.copyFrom(new int[]{101, 102, 103});

            UTF8Buffer names = UTF8Buffer.allocate(arena, "user", new String[]{"Alice", null, "Charlie"});

            Schema schema = new Schema(List.of(
                    new Field("id", DataType.INT32, false),   // Non-nullable
                    new Field("user", DataType.UTF8, true)    // Nullable
            ));

            try (Table table = new Table(schema, List.of(ids, names))) {
                MemorySegment cTableArray = ArrowCExporter.exportTableArray(table, arena).reinterpret(0);
                MemorySegment cTableSchema = ArrowCExporter.exportTableSchema(table, arena).reinterpret(0);

                try (Table importedTable = ArrowCImporter.importTable(cTableArray, cTableSchema)) {
                    assertEquals(3L, importedTable.rowCount());
                    assertEquals(2, importedTable.columnCount());

                    assertFalse(importedTable.schema().getField(0).nullable(), "Non-nullable flag must survive FFI");
                    assertTrue(importedTable.schema().getField(1).nullable(), "Nullable flag must survive FFI");

                    ImportedDataBuffer impIds = (ImportedDataBuffer) importedTable.getColumn("id");
                    ImportedDataBuffer impUsers = (ImportedDataBuffer) importedTable.getColumn("user");

                    assertEquals(101, impIds.getInt(0));
                    assertEquals("Alice", impUsers.getString(0));
                    assertTrue(impUsers.isNull(1));
                    assertEquals("Charlie", impUsers.getString(2));
                }
            }
        }
    }
}