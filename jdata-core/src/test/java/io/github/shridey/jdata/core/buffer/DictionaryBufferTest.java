package io.github.shridey.jdata.core.buffer;

import io.github.shridey.jdata.core.memory.ArrowMemoryPool;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.memory.PooledArena;
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

class DictionaryBufferTest {

    @Test
    void testDictionaryDeduplicationAlignmentAndZeroCopyIndicesView() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(64 * 1024, 2);
             PooledArena arena = pool.acquireArena()) {

            String[] countries = {"IN", "US", "IN", null, "UK", "US", "IN", "JP", null, "UK"};
            DictionaryBuffer dictCol = DictionaryBuffer.allocate(arena, "country", countries);

            assertEquals(DataType.DICTIONARY, dictCol.type());
            assertEquals(10L, dictCol.length());
            assertEquals(2L, dictCol.nullCount());
            assertEquals(4L, dictCol.cardinality(), "Should contain exactly 4 unique country strings");
            assertEquals(2, dictCol.bufferCount());

            assertEquals(0L, dictCol.buffer(0).address() % 64);
            assertEquals(0L, dictCol.buffer(1).address() % 64);
            assertEquals(64L, dictCol.buffer(0).byteSize());
            assertEquals(64L, dictCol.buffer(1).byteSize());

            assertArrayEquals(new String[]{"IN", "US", "UK", "JP"}, dictCol.dictionary().toArray());

            assertEquals(0, dictCol.getIndex(0));
            assertEquals(1, dictCol.getIndex(1));
            assertEquals(0, dictCol.getIndex(2));
            assertTrue(dictCol.isNull(3));
            assertNull(dictCol.getString(3));
            assertEquals("UK", dictCol.getString(4));
            assertEquals("JP", dictCol.getString(7));
            assertArrayEquals(countries, dictCol.toArray());

            Int32Buffer indicesView = dictCol.indices();
            assertEquals(dictCol.buffer(1).address(), indicesView.buffer(1).address());
            assertArrayEquals(dictCol.toIndexArray(), indicesView.toArray());
        }
    }

    @Test
    void testOffHeapFromUTF8AndOffHeapDecodeRoundtrip() {
        try (Arena arena = Arena.ofConfined()) {
            String[] raw = new String[500];
            for (int i = 0; i < raw.length; i++) {
                if (i % 25 == 0) {
                    raw[i] = null;
                } else {
                    raw[i] = "category_🚀_" + (i % 120);
                }
            }

            UTF8Buffer utf8Col = UTF8Buffer.allocate(arena, "categories", raw);
            DictionaryBuffer encoded = DictionaryBuffer.fromUTF8(arena, utf8Col);

            assertEquals(500L, encoded.length());
            assertEquals(20L, encoded.nullCount());
            assertEquals(120L, encoded.cardinality());
            assertArrayEquals(raw, encoded.toArray());

            UTF8Buffer decoded = encoded.decode(arena);
            assertEquals(500L, decoded.length());
            assertEquals(20L, decoded.nullCount());
            assertArrayEquals(raw, decoded.toArray());
        }
    }

    @Test
    void testSlicingAndBlockRemappingAcrossDictionaries() {
        try (Arena arena = Arena.ofConfined()) {
            DictionaryBuffer batch1 = DictionaryBuffer.allocate(arena, "dept",
                    new String[]{"Eng", "Sales", "Eng", null});
            DictionaryBuffer batch2 = DictionaryBuffer.allocate(arena, "dept",
                    new String[]{"HR", "Sales", "Legal", "Eng", "HR"});

            DictionaryBuffer slice2 = batch2.slice(1, 3);
            assertEquals(3L, slice2.length());
            assertEquals(1L, slice2.offset());
            assertArrayEquals(new String[]{"Sales", "Legal", "Eng"}, slice2.toArray());

            UTF8Buffer extraUtf8 = UTF8Buffer.allocate(arena, "dept", new String[]{"Ops", "Eng"});

            DictionaryBufferBuilder mergeBuilder = batch1.toBuilder(arena)
                    .appendBlock(slice2, 0, slice2.length())
                    .appendUTF8Block(extraUtf8, 0, extraUtf8.length())
                    .append("Finance")
                    .appendNull();

            assertEquals(5, mergeBuilder.cardinality());
            DictionaryBuffer merged = mergeBuilder.build();

            assertEquals(11L, merged.length());
            assertEquals(2L, merged.nullCount());
            assertEquals(5L, merged.cardinality());
            assertArrayEquals(
                    new String[]{"Eng", "Sales", "Eng", null, "Sales", "Legal", "Eng", "Ops", "Eng", "Finance", null},
                    merged.toArray()
            );
        }
    }

    @Test
    void testOfIndicesValidation() {
        try (Arena arena = Arena.ofConfined()) {
            UTF8Buffer dict = UTF8Buffer.allocate(arena, "dict", new String[]{"Red", "Green", "Blue"});
            Int32Buffer validIndices = Int32Buffer.allocate(arena, "color", 4);
            validIndices.copyFrom(new int[]{2, 0, 999, 1});
            validIndices.setNull(2);

            DictionaryBuffer wrapped = DictionaryBuffer.ofIndices("color", validIndices, dict);
            assertEquals("Blue", wrapped.getString(0));
            assertEquals("Red", wrapped.getString(1));
            assertNull(wrapped.getString(2));
            assertEquals("Green", wrapped.getString(3));

            Int32Buffer badIndices = Int32Buffer.allocate(arena, "bad", 2);
            badIndices.copyFrom(new int[]{0, 3});
            assertThrows(IndexOutOfBoundsException.class, () -> DictionaryBuffer.ofIndices("bad", badIndices, dict));
        }
    }

    @Test
    void testFFIDictionaryExportAndImportRoundtrip() {
        try (Arena arena = Arena.ofConfined()) {
            DictionaryBuffer orig = DictionaryBuffer.allocate(arena, "status",
                    new String[]{"PENDING", "ACTIVE", null, "ACTIVE", "CLOSED", "PENDING"});
            DictionaryBuffer slicedOrig = orig.slice(1, 4);

            Table table = new Table(
                    new Schema(List.of(new Field("status", DataType.DICTIONARY, true))),
                    List.of(slicedOrig)
            );

            MemorySegment cTableArr = ArrowCExporter.exportTableArray(table, arena).reinterpret(0);
            MemorySegment cTableSch = ArrowCExporter.exportTableSchema(table, arena).reinterpret(0);

            Table importedTable = ArrowCImporter.importTable(cTableArr, cTableSch, arena);
            assertEquals(4L, importedTable.rowCount());
            assertEquals(DataType.DICTIONARY, importedTable.schema().getField(0).type());

            ImportedDataBuffer impDictCol = (ImportedDataBuffer) importedTable.getColumn("status");
            assertNotNull(impDictCol.dictionary());
            assertEquals(3L, impDictCol.dictionary().length());

            assertEquals("ACTIVE", impDictCol.getString(0));
            assertTrue(impDictCol.isNull(1));
            assertNull(impDictCol.getString(1));
            assertEquals("ACTIVE", impDictCol.getString(2));
            assertEquals("CLOSED", impDictCol.getString(3));
            assertEquals(1, impDictCol.getIndex(0));

            DictionaryBuffer rebuilt = ((DictionaryBufferBuilder) impDictCol.toBuilder(arena)).build();
            assertArrayEquals(new String[]{"ACTIVE", null, "ACTIVE", "CLOSED"}, rebuilt.toArray());
        }
    }
}