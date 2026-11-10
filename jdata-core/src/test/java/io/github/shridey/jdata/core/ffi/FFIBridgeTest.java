package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;
import io.github.shridey.jdata.core.layout.Field;
import io.github.shridey.jdata.core.layout.Schema;
import io.github.shridey.jdata.core.layout.Table;
import org.junit.jupiter.api.Test;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class FFIBridgeTest {

    @Test
    void testZeroCopyExportAndImport() {
        try (Arena arena = Arena.ofConfined()) {

            // 1. Create native Java memory
            Float64Buffer originalCol = Float64Buffer.allocate(arena, "sensor_data", 50);
            originalCol.set(0, 3.1415);
            originalCol.setNull(1);

            // 2. EXPORT: Bind the Java memory to C-Structs
            MemorySegment cArrayPtr = ArrowCExporter.exportArray(originalCol, arena);
            MemorySegment cSchemaPtr = ArrowCExporter.exportSchema(originalCol, arena);

            // 3. IMPORT: Read the C-Structs back into a generic ArrowColumn
            double value;
            try (ImportedDataBuffer importedCol = ArrowCImporter.importBuffer(cArrayPtr, cSchemaPtr)) {

                // 4. VERIFY: Ensure the Importer dynamically resolved everything correctly
                assertEquals("sensor_data", importedCol.name());
                assertEquals(DataType.FLOAT64, importedCol.type());
                assertEquals(50, importedCol.length());

                // 5. VERIFY ZERO-COPY: Read from the imported column's reinterpreted MemorySegment
                assertFalse(importedCol.isNull(0));
                assertTrue(importedCol.isNull(1));

                // We cast down to Float64Column just to test the specific .get() method,
                // proving the raw memory address matches the original.
                // In a real scenario, jdata-num uses the Vector API on importedCol.buffer(1).
                value = importedCol.buffer(1).get(java.lang.foreign.ValueLayout.JAVA_DOUBLE, 0);
            }
            assertEquals(3.1415, value);
        }
    }

    @Test
    void testImportedDataBufferToBuilder() {
        try (Arena arena = Arena.ofConfined()) {
            Float64Buffer original = Float64Buffer.allocate(arena, "sensor", 2);
            original.set(0, 1.5);
            original.setNull(1);

            MemorySegment arrayPtr = ArrowCExporter.exportArray(original, arena);
            MemorySegment schemaPtr = ArrowCExporter.exportSchema(original, arena);

            Float64Buffer rebuilt;

            // Try-with-resources triggers ImportedDataBuffer.close() to release the C-memory
            try (ImportedDataBuffer imported = ArrowCImporter.importBuffer(arrayPtr, schemaPtr)) {

                // Route through the switch statement.
                // We cast to the expected builder type since we know the schema.
                Float64BufferBuilder builder = (Float64BufferBuilder) imported.toBuilder(arena);

                builder.append(2.5); // Prove it is safely mutable Java memory now
                rebuilt = builder.build();

            } // Foreign memory is safely released natively here!

            assertEquals(3, rebuilt.length());
            assertEquals(1.5, rebuilt.get(0));
            assertTrue(rebuilt.isNull(1));
            assertEquals(2.5, rebuilt.get(2));
        }
    }

    @Test
    void testExportImportSlicedBuffer() {
        try (Arena arena = Arena.ofConfined()) {
            Float64Buffer original = Float64Buffer.allocate(arena, "sensor", 5);
            original.copyFrom(new double[]{1.1, 2.2, 3.3, 4.4, 5.5});

            // Slice out middle elements: [2.2, 3.3, 4.4]
            Float64Buffer slice = original.slice(1, 3);

            // EXPORT: The ArrowCExporter must write offset=1 into the C-Struct
            MemorySegment arrayPtr = ArrowCExporter.exportArray(slice, arena);
            MemorySegment schemaPtr = ArrowCExporter.exportSchema(slice, arena);

            Float64Buffer rebuilt;

            // IMPORT: The ArrowCImporter must read offset=1 and apply it to ImportedDataBuffer
            try (ImportedDataBuffer imported = ArrowCImporter.importBuffer(arrayPtr, schemaPtr)) {

                // 1. Verify the C-Struct successfully passed the offset over FFI
                assertEquals(1, imported.offset());
                assertEquals(3, imported.length());

                // 2. Route through the switch statement to materialize it back to Java memory
                Float64BufferBuilder builder = (Float64BufferBuilder) imported.toBuilder(arena);
                rebuilt = builder.build();
            } // Foreign memory released

            // 3. Verify the finalized buffer
            assertEquals(3, rebuilt.length());
            assertEquals(0, rebuilt.offset()); // Fresh memory allocation = 0 offset
            assertEquals(2.2, rebuilt.get(0));
            assertEquals(3.3, rebuilt.get(1));
            assertEquals(4.4, rebuilt.get(2));
        }
    }

    @Test
    void testExportImportTableRoundtrip() throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            // 1. Allocate sample columns with data
            Int32Buffer idCol = Int32Buffer.allocate(arena, "id", 3);
            idCol.set(0, 10);
            idCol.set(1, 20);
            idCol.set(2, 30);

            Float64Buffer valCol = Float64Buffer.allocate(arena, "value", 3);
            valCol.set(0, 100.5);
            valCol.set(1, 200.0);
            valCol.set(2, 300.25);

            // 2. Define Schema and build the Table
            Schema schema = new Schema(List.of(
                    new Field("id", DataType.INT32, true),
                    new Field("value", DataType.FLOAT64, true)
            ));

            try (Table originalTable = new Table(schema, List.of(idCol, valCol))) {

                // 3. Export Table into Arrow C-Struct Envelopes (+s struct format)
                MemorySegment arrayPtr = ArrowCExporter.exportTableArray(originalTable, arena);
                MemorySegment schemaPtr = ArrowCExporter.exportTableSchema(originalTable, arena);

                // 4. Import the Table back from the C-Struct pointers
                try (Table importedTable = ArrowCImporter.importTable(arrayPtr, schemaPtr)) {

                    // 5. Verify structural metadata alignment
                    assertEquals(3, importedTable.rowCount());
                    assertEquals(2, importedTable.columnCount());

                    assertEquals("id", importedTable.schema().getField(0).name());
                    assertEquals(DataType.INT32, importedTable.schema().getField(0).type());

                    assertEquals("value", importedTable.schema().getField(1).name());
                    assertEquals(DataType.FLOAT64, importedTable.schema().getField(1).type());

                    // 6. Verify data integrity across imported columns using ImportedDataBuffer getters
                    ImportedDataBuffer importedIdCol = (ImportedDataBuffer) importedTable.getColumn(0);
                    ImportedDataBuffer importedValCol = (ImportedDataBuffer) importedTable.getColumn(1);

                    assertEquals(10, importedIdCol.getInt(0));
                    assertEquals(20, importedIdCol.getInt(1));
                    assertEquals(30, importedIdCol.getInt(2));

                    assertEquals(100.5, importedValCol.getDouble(0));
                    assertEquals(200.0, importedValCol.getDouble(1));
                    assertEquals(300.25, importedValCol.getDouble(2));
                }
            }
        }
    }
}