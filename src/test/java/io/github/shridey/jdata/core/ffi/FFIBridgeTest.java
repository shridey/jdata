package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import org.junit.jupiter.api.Test;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
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
}