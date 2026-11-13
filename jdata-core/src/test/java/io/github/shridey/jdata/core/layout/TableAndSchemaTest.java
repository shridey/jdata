package io.github.shridey.jdata.core.layout;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import io.github.shridey.jdata.core.buffer.Int32Buffer;
import io.github.shridey.jdata.core.buffer.Int64Buffer;
import io.github.shridey.jdata.core.buffer.UTF8Buffer;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

class TableAndSchemaTest {

    @Test
    void testDataTypeFormatResolutionRejectsPrefixCollisions() {
        assertEquals(DataType.INT16, DataType.fromFormat("s"));
        assertEquals(DataType.INT64, DataType.fromFormat("l"));
        assertEquals(DataType.UTF8, DataType.fromFormat("u"));
        assertEquals(DataType.STRUCT, DataType.fromFormat("+s"));
        assertEquals(DataType.TIMESTAMP_MS, DataType.fromFormat("tsm:UTC"));
        assertEquals(DataType.TIMESTAMP_US, DataType.fromFormat("tsu:Asia/Kolkata"));

        assertEquals(4, DataType.INT32.byteSize());
        assertEquals("i", DataType.INT32.getFormat());
        assertEquals(-1, DataType.UTF8.byteSize());

        assertThrows(IllegalArgumentException.class, () -> DataType.fromFormat("string"));
        assertThrows(IllegalArgumentException.class, () -> DataType.fromFormat("list"));
        assertThrows(IllegalArgumentException.class, () -> DataType.fromFormat("uint8"));
        assertThrows(IllegalArgumentException.class, () -> DataType.fromFormat("int32"));
        assertThrows(IllegalArgumentException.class, () -> DataType.fromFormat("DICTIONARY"));
        assertThrows(IllegalArgumentException.class, () -> DataType.fromFormat(""));
        assertThrows(IllegalArgumentException.class, () -> DataType.fromFormat(null));
    }

    @Test
    void testSchemaDefensiveImmutabilityAndLookup() {
        List<Field> mutableList = new ArrayList<>();
        mutableList.add(new Field("id", DataType.INT64, false));
        mutableList.add(new Field("score", DataType.FLOAT64));

        Schema schema = new Schema(mutableList);
        mutableList.clear();

        assertEquals(2, schema.columnCount());
        assertEquals(0, schema.getFieldIndex("id"));
        assertEquals(1, schema.getFieldIndex("score"));
        assertEquals(-1, schema.getFieldIndex("unknown"));

        Schema differentSchema = new Schema(List.of(new Field("id", DataType.INT32)));
        assertNotEquals(differentSchema, schema);
        Object nonSchemaObject = List.of();
        assertNotEquals(nonSchemaObject, schema);

        assertThrows(IllegalArgumentException.class, () -> schema.getField("unknown"));
        assertThrows(IllegalArgumentException.class, () -> schema.dropField("unknown"));
        assertThrows(IllegalArgumentException.class, () -> schema.renameField("unknown", "new_name"));
        assertThrows(UnsupportedOperationException.class,
                () -> attemptAdd(schema.fields(), new Field("x", DataType.INT32)));

        assertThrows(IllegalArgumentException.class,
                () -> new Schema(List.of(new Field("id", DataType.INT32), new Field("id", DataType.INT64))));
        NullPointerException npe = assertThrows(NullPointerException.class,
                () -> new Schema(Arrays.asList(new Field("id", DataType.INT32), null)));
        assertTrue(npe.getMessage().contains("Field at index 1"));
    }

    private static <T> void attemptAdd(List<T> list, T element) {
        list.add(element);
    }

    @Test
    void testTableStrictValidationInvariantsAndEmptyTable() {
        try (Arena arena = Arena.ofConfined()) {
            Table emptyTable = new Table(new Schema(List.of()), List.of());
            assertEquals(0L, emptyTable.rowCount());
            assertEquals(0, emptyTable.columnCount());
            assertTrue(emptyTable.columns().isEmpty());
            assertTrue(emptyTable.toString().contains("rows=0"));

            Int32Buffer ids = Int32Buffer.allocate(arena, "id", 3);
            Int32Buffer wrongLength = Int32Buffer.allocate(arena, "val", 4);
            Int32Buffer wrongName = Int32Buffer.allocate(arena, "other_name", 3);
            Float64Buffer valsWithNull = Float64Buffer.allocate(arena, "val", 3);
            valsWithNull.setNull(1);

            Schema s1 = new Schema(List.of(new Field("id", DataType.INT32), new Field("val", DataType.INT32)));
            assertThrows(IllegalArgumentException.class, () -> new Table(s1, List.of(ids)));
            assertThrows(IllegalArgumentException.class, () -> new Table(s1, List.of(ids, wrongLength)));
            assertThrows(IllegalArgumentException.class, () -> new Table(s1, List.of(ids, wrongName)));
            assertThrows(IllegalArgumentException.class, () -> new Table(s1, List.of(ids, valsWithNull)));

            Schema nonNullSchema = new Schema(List.of(
                    new Field("id", DataType.INT32, false),
                    new Field("val", DataType.FLOAT64, false)
            ));
            assertThrows(IllegalArgumentException.class, () -> new Table(nonNullSchema, List.of(ids, valsWithNull)));

            Table validTable = new Table(
                    new Schema(List.of(new Field("id", DataType.INT32, false))),
                    List.of(ids)
            );
            assertEquals(1, validTable.columns().size());
            assertSame(validTable, Table.concat(List.of(validTable)));
            assertThrows(IllegalArgumentException.class, () -> Table.concat(List.of()));
            assertThrows(IllegalArgumentException.class, () -> validTable.concat(emptyTable));
            assertThrows(IllegalArgumentException.class, () -> validTable.getColumn("missing"));
            assertThrows(IllegalArgumentException.class, () -> validTable.select("missing"));
            assertThrows(IllegalArgumentException.class, () -> validTable.dropColumn("missing"));
            assertThrows(IllegalArgumentException.class, () -> validTable.renameColumn("missing", "x"));
            assertThrows(IllegalArgumentException.class, () -> validTable.head(-1));
            assertThrows(IllegalArgumentException.class, () -> validTable.tail(-1));

            Int32Buffer selWithNull = Int32Buffer.allocate(arena, "sel", 2);
            selWithNull.set(0, 1);
            selWithNull.setNull(1);
            Table gathered = validTable.take(selWithNull, arena);
            assertTrue(gathered.schema().getField(0).nullable());
            assertTrue(gathered.getColumn(0).isNull(1));
        }
    }

    @Test
    void testParallelZeroCopyTableSlicingWithSharedArena() throws Exception {
        try (Arena sharedArena = Arena.ofShared()) {
            int totalRows = 10_000;
            Int64Buffer idCol = Int64Buffer.allocate(sharedArena, "id", totalRows);

            String[] tags = new String[totalRows];
            for (int i = 0; i < totalRows; i++) {
                idCol.set(i, i);
                tags[i] = "row_" + i;
            }
            UTF8Buffer populatedTags = UTF8Buffer.allocate(sharedArena, "tag", tags);

            Schema schema = new Schema(List.of(
                    new Field("id", DataType.INT64, false),
                    new Field("tag", DataType.UTF8, false)
            ));

            Table table = new Table(schema, List.of(idCol, populatedTags));

            try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
                List<Callable<Long>> tasks = new ArrayList<>();
                int chunkSize = 2_500;

                for (int chunk = 0; chunk < 4; chunk++) {
                    long start = (long) chunk * chunkSize;
                    Table slice = table.slice(start, chunkSize);

                    tasks.add(() -> {
                        Int64Buffer sliceIds = (Int64Buffer) slice.getColumn("id");
                        UTF8Buffer sliceTags = (UTF8Buffer) slice.getColumn("tag");
                        long sum = 0;
                        for (int i = 0; i < slice.rowCount(); i++) {
                            long id = sliceIds.get(i);
                            assertEquals("row_" + id, sliceTags.getString(i));
                            sum += id;
                        }
                        return sum;
                    });
                }

                long totalSum = 0;
                for (Future<Long> f : pool.invokeAll(tasks)) {
                    totalSum += f.get();
                }

                long expectedSum = (long) (totalRows - 1) * totalRows / 2L;
                assertEquals(expectedSum, totalSum);
            }
        }
    }
}