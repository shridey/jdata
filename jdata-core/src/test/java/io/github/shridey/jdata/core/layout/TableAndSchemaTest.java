package io.github.shridey.jdata.core.layout;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import io.github.shridey.jdata.core.buffer.Int32Buffer;
import io.github.shridey.jdata.core.buffer.Int64Buffer;
import io.github.shridey.jdata.core.buffer.UTF8Buffer;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.ArrayList;
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

        // Must reject arbitrary strings that happen to start with a valid 1-char format prefix
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
        mutableList.clear(); // Must not affect schema

        assertEquals(2, schema.columnCount());
        assertEquals(0, schema.getFieldIndex("id"));
        assertEquals(1, schema.getFieldIndex("score"));
        assertEquals(-1, schema.getFieldIndex("unknown"));
        assertThrows(IllegalArgumentException.class, () -> schema.getField("unknown"));
        assertThrows(UnsupportedOperationException.class, () -> schema.fields().add(new Field("x", DataType.INT32)));
    }

    @Test
    void testTableStrictValidationInvariants() {
        try (Arena arena = Arena.ofConfined()) {
            Int32Buffer ids = Int32Buffer.allocate(arena, "id", 3);
            Int32Buffer wrongLength = Int32Buffer.allocate(arena, "val", 4);
            Int32Buffer wrongName = Int32Buffer.allocate(arena, "other_name", 3);
            Float64Buffer valsWithNull = Float64Buffer.allocate(arena, "val", 3);
            valsWithNull.setNull(1);

            // 1. Row count mismatch
            Schema s1 = new Schema(List.of(new Field("id", DataType.INT32), new Field("val", DataType.INT32)));
            assertThrows(IllegalArgumentException.class, () -> {
                try (Table ignored = new Table(s1, List.of(ids, wrongLength))) {
                    fail("Should have thrown on row count mismatch");
                }
            });

            // 2. Column name mismatch
            assertThrows(IllegalArgumentException.class, () -> {
                try (Table ignored = new Table(s1, List.of(ids, wrongName))) {
                    fail("Should have thrown on column name mismatch");
                }
            });

            // 3. DataType mismatch
            assertThrows(IllegalArgumentException.class, () -> {
                try (Table ignored = new Table(s1, List.of(ids, valsWithNull))) {
                    fail("Should have thrown on DataType mismatch");
                }
            });

            // 4. Non-nullable Field violation
            Schema nonNullSchema = new Schema(List.of(
                    new Field("id", DataType.INT32, false),
                    new Field("val", DataType.FLOAT64, false)
            ));
            assertThrows(IllegalArgumentException.class, () -> {
                try (Table ignored = new Table(nonNullSchema, List.of(ids, valsWithNull))) {
                    fail("Should have thrown on non-nullable violation");
                }
            });
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

            try (Table table = new Table(schema, List.of(idCol, populatedTags));
                 ExecutorService pool = Executors.newFixedThreadPool(4)) {

                List<Callable<Long>> tasks = createTasks(table);

                long totalSum = 0;
                for (Future<Long> f : pool.invokeAll(tasks)) {
                    totalSum += f.get();
                }

                long expectedSum = (long) (totalRows - 1) * totalRows / 2L;
                assertEquals(expectedSum, totalSum);
            }
        }
    }

    private static @NonNull List<Callable<Long>> createTasks(Table table) {
        List<Callable<Long>> tasks = new ArrayList<>();
        int chunkSize = 2_500;

        for (int chunk = 0; chunk < 4; chunk++) {
            long start = (long) chunk * chunkSize;

            tasks.add(() -> {
                try (Table slice = table.slice(start, chunkSize)) {
                    Int64Buffer sliceIds = (Int64Buffer) slice.getColumn("id");
                    UTF8Buffer sliceTags = (UTF8Buffer) slice.getColumn("tag");
                    long sum = 0;
                    for (int i = 0; i < slice.rowCount(); i++) {
                        long id = sliceIds.get(i);
                        assertEquals("row_" + id, sliceTags.getString(i));
                        sum += id;
                    }
                    return sum;
                }
            });
        }
        return tasks;
    }
}