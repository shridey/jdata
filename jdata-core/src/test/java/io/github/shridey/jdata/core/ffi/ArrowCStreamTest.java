package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.ArrowMemoryPool;
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.PooledArena;
import io.github.shridey.jdata.core.buffer.DictionaryBuffer;
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import io.github.shridey.jdata.core.buffer.Int32Buffer;
import io.github.shridey.jdata.core.layout.Field;
import io.github.shridey.jdata.core.layout.Schema;
import io.github.shridey.jdata.core.layout.Table;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ArrowCStreamTest {

    @Test
    void testTableStreamPerBatchPooledArenaAndReadAllConcatenation() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(64 * 1024, 2);
             Arena producerArena = Arena.ofConfined();
             Arena streamArena = Arena.ofConfined()) {

            Int32Buffer ids = Int32Buffer.allocate(producerArena, "id", 7);
            ids.copyFrom(new int[]{10, 20, 30, 40, 50, 60, 70});

            Float64Buffer amounts = Float64Buffer.allocate(producerArena, "amount", 7);
            amounts.copyFrom(new double[]{1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5});

            DictionaryBuffer regions = DictionaryBuffer.allocate(producerArena, "region",
                    new String[]{"IN", "US", "UK", "IN", null, "US", "UK"});

            Schema schema = new Schema(List.of(
                    new Field("id", DataType.INT32, false),
                    new Field("amount", DataType.FLOAT64, false),
                    new Field("region", DataType.DICTIONARY, true)
            ));
            Table fullTable = new Table(schema, List.of(ids, amounts, regions));

            // 1. Stream in batches of 3 rows (batches of size 3, 3, 1) using per-batch PooledArena
            MemorySegment cStream1 = ArrowCExporter.exportTableStream(fullTable, 3, producerArena).reinterpret(0);
            try (ArrowTableStream stream1 = ArrowCImporter.importTableStream(cStream1, streamArena)) {
                assertFalse(stream1.isClosed());
                assertEquals(schema, stream1.schema());
                assertNull(stream1.lastError());

                int batchCount = 0;
                long totalRows = 0;

                while (true) {
                    try (PooledArena batchArena = pool.acquireArena()) {
                        Table batch = stream1.nextBatch(batchArena);
                        if (batch == null) {
                            break;
                        }
                        batchCount++;
                        totalRows += batch.rowCount();
                        ImportedDataBuffer batchIds = (ImportedDataBuffer) batch.getColumn("id");
                        assertTrue(batchIds.getInt(0) >= 10);
                    }
                }

                assertEquals(3, batchCount);
                assertEquals(7L, totalRows);
                assertNull(stream1.nextBatch(), "Subsequent nextBatch() calls after EOF must return null");
            }

            // 2. Stream via readAll(arena) into a single zero-copy concatenated Table
            MemorySegment cStream2 = ArrowCExporter.exportTableStream(fullTable, 2, producerArena).reinterpret(0);
            try (ArrowTableStream stream2 = ArrowCImporter.importTableStream(cStream2, streamArena)) {
                Table combined = stream2.readAll(streamArena).compact(streamArena);
                assertEquals(7L, combined.rowCount());
                assertArrayEquals(new int[]{10, 20, 30, 40, 50, 60, 70}, ((Int32Buffer) combined.getColumn("id")).toArray());
                assertArrayEquals(
                        new String[]{"IN", "US", "UK", "IN", null, "US", "UK"},
                        ((DictionaryBuffer) combined.getColumn("region")).toArray()
                );
            }
        }
    }

    @Test
    void testEmptyStreamAndErrorPropagationAcrossFFI() {
        try (Arena arena = Arena.ofConfined()) {
            Schema schema = new Schema(List.of(
                    new Field("id", DataType.INT32, false),
                    new Field("tag", DataType.DICTIONARY, true)
            ));

            // 1. Empty 0-batch stream
            MemorySegment emptyCStream = ArrowCExporter.exportTableStream(schema, Collections.emptyIterator(), arena);
            try (ArrowTableStream emptyStream = ArrowCImporter.importTableStream(emptyCStream, arena)) {
                Table emptyTable = emptyStream.readAll(arena);
                assertEquals(0L, emptyTable.rowCount());
                assertEquals(schema, emptyTable.schema());
            }
            // Importing an already-released stream must throw IllegalStateException
            assertThrows(IllegalStateException.class, () -> ArrowCImporter.importTableStream(emptyCStream, arena));

            // 2. Failing iterator must propagate error message via get_last_error
            Iterator<Table> failingIterator = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return true;
                }

                @Override
                public Table next() {
                    throw new IllegalStateException("Simulated disk I/O read failure at batch 1");
                }
            };

            MemorySegment failingCStream = ArrowCExporter.exportTableStream(schema, failingIterator, arena);
            try (ArrowTableStream errStream = ArrowCImporter.importTableStream(failingCStream, arena)) {
                IllegalStateException ex = assertThrows(IllegalStateException.class, errStream::nextBatch);
                assertTrue(ex.getMessage().contains("Simulated disk I/O read failure at batch 1"));
                assertEquals("Simulated disk I/O read failure at batch 1", errStream.lastError());
            }
        }
    }
}