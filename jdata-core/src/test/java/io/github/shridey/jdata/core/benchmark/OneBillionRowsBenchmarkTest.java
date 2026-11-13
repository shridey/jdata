package io.github.shridey.jdata.core.benchmark;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.Bitmaps;
import io.github.shridey.jdata.core.buffer.BooleanBuffer;
import io.github.shridey.jdata.core.buffer.ChunkedBuffer;
import io.github.shridey.jdata.core.buffer.DictionaryBuffer;
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import io.github.shridey.jdata.core.buffer.Int32Buffer;
import io.github.shridey.jdata.core.buffer.Int64Buffer;
import io.github.shridey.jdata.core.ffi.ArrowCExporter;
import io.github.shridey.jdata.core.ffi.ArrowCImporter;
import io.github.shridey.jdata.core.ffi.ArrowTableStream;
import io.github.shridey.jdata.core.ffi.ImportedDataBuffer;
import io.github.shridey.jdata.core.layout.Field;
import io.github.shridey.jdata.core.layout.Schema;
import io.github.shridey.jdata.core.layout.Table;
import io.github.shridey.jdata.core.memory.ArrowMemoryPool;
import io.github.shridey.jdata.core.memory.PooledArena;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OneBillionRowsBenchmarkTest {

    private static final long ONE_BILLION_ROWS = 1_000_000_000L;

    @Test
    void runOneBillionRowsBenchmarkSuite() {
        List<BillionRowResult> results = new ArrayList<>();

        results.add(benchmarkOneBillionRowsPooledSlabAllocation());
        results.add(benchmarkOneBillionRowsBitmapsAnd());
        results.add(benchmarkOneBillionRowsFfiArrowArrayStream());
        results.add(benchmarkOneBillionRowsChunkedTableSlicing());

        printReport(results);
        assertEquals(4, results.size());
    }

    private static BillionRowResult benchmarkOneBillionRowsPooledSlabAllocation() {
        int batches = 1_000;
        int batchRows = 1_000_000; // 1,000 * 1,000,000 = 1,000,000,000 rows (8.0 GB logical Int64 data)

        try (ArrowMemoryPool pool = new ArrowMemoryPool(16L * 1024L * 1024L, 2)) {
            // Warmup pass (50 batches)
            for (int w = 0; w < 50; w++) {
                try (PooledArena arena = pool.acquireArena()) {
                    Int64Buffer buf = Int64Buffer.allocate(arena, "id", batchRows);
                    buf.set(0, w);
                }
            }

            long t0 = System.nanoTime();
            long totalRows = 0L;
            long checksum = 0L;

            for (int b = 0; b < batches; b++) {
                try (PooledArena batchArena = pool.acquireArena()) {
                    Int64Buffer col = Int64Buffer.allocate(batchArena, "id", batchRows);
                    col.set(0, b);
                    col.set(batchRows - 1, b + 1L);
                    checksum += col.get(0) + col.get(batchRows - 1);
                    totalRows += col.length();
                }
            }

            double elapsedMs = (System.nanoTime() - t0) / 1_000_000.0;
            assertEquals(ONE_BILLION_ROWS, totalRows);
            assertEquals(1_000_000L, checksum);
            assertEquals(1, pool.totalAllocatedSlabs(), "Must recycle a single 16 MB physical slab");

            return new BillionRowResult(
                    "1B-Row Pooled Int64 Allocation (8 GB Logical)",
                    totalRows,
                    elapsedMs,
                    pool.totalAllocatedSlabBytes() / (1024.0 * 1024.0)
            );
        }
    }

    private static BillionRowResult benchmarkOneBillionRowsBitmapsAnd() {
        int batches = 100;
        int batchRows = 10_000_000; // 100 * 10,000,000 = 1,000,000,000 boolean rows

        try (ArrowMemoryPool pool = new ArrowMemoryPool(16L * 1024L * 1024L, 2);
             Arena setupArena = Arena.ofConfined()) {

            BooleanBuffer left = BooleanBuffer.allocate(setupArena, "left", batchRows);
            BooleanBuffer right = BooleanBuffer.allocate(setupArena, "right", batchRows);
            left.set(0, true);
            right.set(0, true);
            left.set(batchRows - 1, true);
            right.set(batchRows - 1, true);

            // Warmup pass
            for (int w = 0; w < 5; w++) {
                try (PooledArena arena = pool.acquireArena()) {
                    BooleanBuffer out = Bitmaps.and(left, right, "and", arena);
                    assertEquals(batchRows, out.length());
                }
            }

            long t0 = System.nanoTime();
            long totalRows = 0L;

            for (int b = 0; b < batches; b++) {
                try (PooledArena batchArena = pool.acquireArena()) {
                    BooleanBuffer res = Bitmaps.and(left, right, "and", batchArena);
                    totalRows += res.length();
                }
            }

            double elapsedMs = (System.nanoTime() - t0) / 1_000_000.0;
            assertEquals(ONE_BILLION_ROWS, totalRows);
            assertEquals(1, pool.totalAllocatedSlabs());

            return new BillionRowResult(
                    "1B-Row 64-Bit Word Bitmaps.and() Intersection",
                    totalRows,
                    elapsedMs,
                    pool.totalAllocatedSlabBytes() / (1024.0 * 1024.0)
            );
        }
    }

    private static BillionRowResult benchmarkOneBillionRowsFfiArrowArrayStream() {
        int batches = 1_000;
        int batchRows = 1_000_000; // 1,000 * 1,000,000 = 1,000,000,000 rows (20.0 GB logical Table data)

        try (ArrowMemoryPool pool = new ArrowMemoryPool(16L * 1024L * 1024L, 2);
             Arena setupArena = Arena.ofConfined();
             Arena streamArena = Arena.ofConfined()) {

            Int32Buffer col1 = Int32Buffer.allocate(setupArena, "c1", batchRows);
            Int64Buffer col2 = Int64Buffer.allocate(setupArena, "c2", batchRows);
            Float64Buffer col3 = Float64Buffer.allocate(setupArena, "c3", batchRows);
            col1.set(0, 42);
            col2.set(batchRows - 1, 999L);
            col3.set(0, 3.14);

            Schema schema = new Schema(List.of(
                    new Field("c1", DataType.INT32, false),
                    new Field("c2", DataType.INT64, false),
                    new Field("c3", DataType.FLOAT64, false)
            ));
            Table templateBatch = new Table(schema, List.of(col1, col2, col3));

            Iterator<Table> billionRowIterator = new Iterator<>() {
                private int emitted = 0;

                @Override
                public boolean hasNext() {
                    return emitted < batches;
                }

                @Override
                public Table next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    emitted++;
                    return templateBatch;
                }
            };

            long t0 = System.nanoTime();
            long streamedRows = 0L;
            long checksum = 0L;

            MemorySegment cStream = ArrowCExporter.exportTableStream(schema, billionRowIterator, streamArena);
            try (ArrowTableStream stream = ArrowCImporter.importTableStream(cStream, streamArena)) {
                while (true) {
                    try (PooledArena batchArena = pool.acquireArena()) {
                        Table batch = stream.nextBatch(batchArena);
                        if (batch == null) {
                            break;
                        }
                        ImportedDataBuffer impCol2 = (ImportedDataBuffer) batch.getColumn(1);
                        checksum += impCol2.getLong(batchRows - 1);
                        streamedRows += batch.rowCount();
                    }
                }
            }

            double elapsedMs = (System.nanoTime() - t0) / 1_000_000.0;
            assertEquals(ONE_BILLION_ROWS, streamedRows);
            assertEquals(999_000L, checksum);
            assertEquals(1, pool.totalAllocatedSlabs());

            return new BillionRowResult(
                    "1B-Row Zero-Copy FFI Stream (20 GB Logical)",
                    streamedRows,
                    elapsedMs,
                    pool.totalAllocatedSlabBytes() / (1024.0 * 1024.0)
            );
        }
    }

    private static BillionRowResult benchmarkOneBillionRowsChunkedTableSlicing() {
        int chunks = 1_000;
        int chunkRows = 1_000_000; // 1,000 * 1,000,000 = 1,000,000,000 rows

        try (ArrowMemoryPool pool = new ArrowMemoryPool(32L * 1024L * 1024L, 2);
             PooledArena arena = pool.acquireArena()) {

            Int64Buffer idChunk = Int64Buffer.allocate(arena, "id", chunkRows);
            Float64Buffer valChunk = Float64Buffer.allocate(arena, "val", chunkRows);
            Int32Buffer codeChunk = Int32Buffer.allocate(arena, "region", chunkRows);
            idChunk.set(500_000, 777L);
            valChunk.set(500_000, 123.45);
            DictionaryBuffer dictChunk = DictionaryBuffer.ofIndices(
                    "region",
                    codeChunk,
                    DictionaryBuffer.allocate(arena, "dict", new String[]{"APAC", "EMEA", "AMER"}).dictionary()
            );

            Schema schema = new Schema(List.of(
                    new Field("id", DataType.INT64, false),
                    new Field("val", DataType.FLOAT64, false),
                    new Field("region", DataType.DICTIONARY, false)
            ));
            Table singleChunkTable = new Table(schema, List.of(idChunk, valChunk, dictChunk));

            List<Table> thousandTables = new ArrayList<>(chunks);
            for (int i = 0; i < chunks; i++) {
                thousandTables.add(singleChunkTable);
            }

            long t0 = System.nanoTime();

            // 1. O(1) Zero-Copy Concatenation into a 1,000,000,000-row Table
            Table oneBillionRowTable = Table.concat(thousandTables);
            assertEquals(ONE_BILLION_ROWS, oneBillionRowTable.rowCount());

            // 2. Zero-Copy Cross-Chunk Slicing of 500,000,000 rows from the middle of the 1B-row Table
            Table halfBillionSlice = oneBillionRowTable.slice(250_000_000L, 500_000_000L);
            assertEquals(500_000_000L, halfBillionSlice.rowCount());

            // 3. O(log C) binary-searched random lookups across the 1-billion-row virtual space
            ChunkedBuffer chunkedIds = (ChunkedBuffer) oneBillionRowTable.getColumn("id");
            ChunkedBuffer chunkedRegions = (ChunkedBuffer) oneBillionRowTable.getColumn("region");
            long lookupChecksum = 0L;
            for (long idx = 500_000L; idx < ONE_BILLION_ROWS; idx += 1_000_000L) {
                lookupChecksum += chunkedIds.getLong(idx);
            }
            assertEquals("APAC", chunkedRegions.getString(999_999_999L));

            double elapsedMs = (System.nanoTime() - t0) / 1_000_000.0;
            assertEquals(777_000L, lookupChecksum);

            return new BillionRowResult(
                    "1B-Row Chunked Table Concat + Slice + Lookup",
                    ONE_BILLION_ROWS,
                    elapsedMs,
                    pool.totalAllocatedSlabBytes() / (1024.0 * 1024.0)
            );
        }
    }

    private static void printReport(List<BillionRowResult> results) {
        String border = "+-----------------------------------------------+----------------+------------+----------------+------------------+";
        System.out.println("\n" + border);
        System.out.printf(Locale.ROOT, "| %-45s | %14s | %10s | %14s | %16s |%n",
                "jdata-core 1-Billion-Row Benchmark", "Rows", "Time", "Peak Pool RAM", "Throughput");
        System.out.println(border);
        for (BillionRowResult r : results) {
            double billionRowsPerSec = (r.rows / (r.elapsedMillis / 1000.0)) / 1_000_000_000.0;
            System.out.printf(Locale.ROOT, "| %-45s | %,14d | %7.2f ms | %11.1f MB | %10.2f B/sec |%n",
                    r.name, r.rows, r.elapsedMillis, r.peakPoolMb, billionRowsPerSec);
        }
        System.out.println(border + "\n");
    }

    private record BillionRowResult(String name, long rows, double elapsedMillis, double peakPoolMb) {}
}