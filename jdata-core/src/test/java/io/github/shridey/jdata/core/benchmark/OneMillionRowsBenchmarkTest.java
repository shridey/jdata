package io.github.shridey.jdata.core.benchmark;

import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.Bitmaps;
import io.github.shridey.jdata.core.buffer.BooleanBuffer;
import io.github.shridey.jdata.core.buffer.ChunkedBuffer;
import io.github.shridey.jdata.core.buffer.ChunkedBufferBuilder;
import io.github.shridey.jdata.core.buffer.DictionaryBuffer;
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import io.github.shridey.jdata.core.buffer.Int32Buffer;
import io.github.shridey.jdata.core.buffer.Int64Buffer;
import io.github.shridey.jdata.core.buffer.Int64BufferBuilder;
import io.github.shridey.jdata.core.buffer.UTF8Buffer;
import io.github.shridey.jdata.core.ffi.ArrowCExporter;
import io.github.shridey.jdata.core.ffi.ArrowCImporter;
import io.github.shridey.jdata.core.ffi.ArrowTableStream;
import io.github.shridey.jdata.core.layout.Field;
import io.github.shridey.jdata.core.layout.Schema;
import io.github.shridey.jdata.core.layout.Table;
import io.github.shridey.jdata.core.memory.ArrowMemoryPool;
import io.github.shridey.jdata.core.memory.PooledArena;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OneMillionRowsBenchmarkTest {

    private static final int ROWS = 1_000_000;
    private static final int WARMUP_ITERS = 3;
    private static final int MEASURE_ITERS = 5;

    @Test
    void runFullBenchmarkSuite() {
        List<BenchmarkResult> results = new ArrayList<>();

        results.add(benchmarkPooledVsOsArena());
        results.add(benchmarkPrimitiveBuilderIngest());
        results.add(benchmarkChunkedStreamingIngest());
        results.add(benchmarkUtf8ZeroAllocSimdScan());
        results.add(benchmarkOffHeapDictionaryEncodeAndDecode());
        results.add(benchmarkBitmapAndTableFilter());
        results.add(benchmarkZeroCopyFfiStreamRoundtrip());

        printReport(results);
        assertEquals(7, results.size());
    }

    private static BenchmarkResult benchmarkPooledVsOsArena() {
        int cycles = 500;
        int rowsPerCycle = 16_384;

        try (ArrowMemoryPool pool = new ArrowMemoryPool(4L * 1024L * 1024L, 4)) {
            for (int w = 0; w < WARMUP_ITERS; w++) {
                runPooledAllocationCycles(pool, cycles, rowsPerCycle);
                runOsAllocationCycles(cycles, rowsPerCycle);
            }

            long pooledNanos = 0L;
            long osNanos = 0L;

            for (int i = 0; i < MEASURE_ITERS; i++) {
                long t0 = System.nanoTime();
                long sum1 = runPooledAllocationCycles(pool, cycles, rowsPerCycle);
                pooledNanos += (System.nanoTime() - t0);

                long t1 = System.nanoTime();
                long sum2 = runOsAllocationCycles(cycles, rowsPerCycle);
                osNanos += (System.nanoTime() - t1);

                assertEquals(sum1, sum2);
            }

            double avgPooledMs = (pooledNanos / (double) MEASURE_ITERS) / 1_000_000.0;
            double avgOsMs = (osNanos / (double) MEASURE_ITERS) / 1_000_000.0;
            double speedup = avgOsMs / Math.max(avgPooledMs, 0.001);
            long totalOps = (long) cycles * rowsPerCycle;

            return new BenchmarkResult(
                    String.format(Locale.ROOT, "PooledSlab vs OS Arena (%.1fx speedup)", speedup),
                    totalOps,
                    avgPooledMs
            );
        }
    }

    private static long runPooledAllocationCycles(ArrowMemoryPool pool, int cycles, int rowsPerCycle) {
        long checksum = 0L;
        for (int c = 0; c < cycles; c++) {
            try (PooledArena arena = pool.acquireArena()) {
                Int64Buffer buf = Int64Buffer.allocate(arena, "col", rowsPerCycle);
                buf.set(0, c);
                checksum += buf.get(0);
            }
        }
        return checksum;
    }

    private static long runOsAllocationCycles(int cycles, int rowsPerCycle) {
        long checksum = 0L;
        for (int c = 0; c < cycles; c++) {
            try (Arena arena = Arena.ofConfined()) {
                Int64Buffer buf = Int64Buffer.allocate(arena, "col", rowsPerCycle);
                buf.set(0, c);
                checksum += buf.get(0);
            }
        }
        return checksum;
    }

    private static BenchmarkResult benchmarkPrimitiveBuilderIngest() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(16L * 1024L * 1024L, 4)) {
            for (int w = 0; w < WARMUP_ITERS; w++) {
                try (PooledArena arena = pool.acquireArena()) {
                    Int64BufferBuilder b = new Int64BufferBuilder(arena, "ids", 64);
                    for (int i = 0; i < ROWS; i++) {
                        b.append(i);
                    }
                    assertEquals(ROWS, b.build().length());
                }
            }

            long totalNanos = 0L;
            for (int m = 0; m < MEASURE_ITERS; m++) {
                try (PooledArena arena = pool.acquireArena()) {
                    long t0 = System.nanoTime();
                    Int64BufferBuilder b = new Int64BufferBuilder(arena, "ids", 64);
                    for (int i = 0; i < ROWS; i++) {
                        b.append(i);
                    }
                    Int64Buffer built = b.build();
                    totalNanos += (System.nanoTime() - t0);
                    assertEquals(ROWS - 1L, built.get(ROWS - 1));
                }
            }

            double avgMs = (totalNanos / (double) MEASURE_ITERS) / 1_000_000.0;
            return new BenchmarkResult("Int64BufferBuilder (In-Place Slab Append)", ROWS, avgMs);
        }
    }

    private static BenchmarkResult benchmarkChunkedStreamingIngest() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(16L * 1024L * 1024L, 4)) {
            for (int w = 0; w < WARMUP_ITERS; w++) {
                try (PooledArena arena = pool.acquireArena()) {
                    ChunkedBufferBuilder cb = new ChunkedBufferBuilder(arena, "stream", DataType.FLOAT64, 65_536);
                    for (int i = 0; i < ROWS; i++) {
                        cb.append(i * 0.5);
                    }
                    assertEquals(ROWS, cb.build().length());
                }
            }

            long totalNanos = 0L;
            for (int m = 0; m < MEASURE_ITERS; m++) {
                try (PooledArena arena = pool.acquireArena()) {
                    long t0 = System.nanoTime();
                    ChunkedBufferBuilder cb = new ChunkedBufferBuilder(arena, "stream", DataType.FLOAT64, 65_536);
                    for (int i = 0; i < ROWS; i++) {
                        cb.append(i * 0.5);
                    }
                    ChunkedBuffer built = cb.build();
                    totalNanos += (System.nanoTime() - t0);
                    assertTrue(built.chunkCount() >= 15);
                }
            }

            double avgMs = (totalNanos / (double) MEASURE_ITERS) / 1_000_000.0;
            return new BenchmarkResult("ChunkedBufferBuilder (Zero-Copy Rollover)", ROWS, avgMs);
        }
    }

    private static BenchmarkResult benchmarkUtf8ZeroAllocSimdScan() {
        String[] categories = new String[16];
        for (int i = 0; i < categories.length; i++) {
            categories[i] = "REGION_CODE_" + i;
        }

        try (Arena setupArena = Arena.ofConfined()) {
            String[] rawStrings = new String[ROWS];
            for (int i = 0; i < ROWS; i++) {
                rawStrings[i] = categories[i & 15];
            }
            UTF8Buffer utf8Col = UTF8Buffer.allocate(setupArena, "region", rawStrings);
            MemorySegment target = MemorySegment.ofArray("REGION_CODE_7".getBytes(StandardCharsets.UTF_8));

            for (int w = 0; w < WARMUP_ITERS; w++) {
                long matches = 0;
                for (int i = 0; i < ROWS; i++) {
                    if (utf8Col.equalsBytes(i, target)) {
                        matches++;
                    }
                }
                assertEquals(ROWS / 16, matches);
            }

            long totalNanos = 0L;
            for (int m = 0; m < MEASURE_ITERS; m++) {
                long t0 = System.nanoTime();
                long matches = 0;
                for (int i = 0; i < ROWS; i++) {
                    if (utf8Col.equalsBytes(i, target)) {
                        matches++;
                    }
                }
                totalNanos += (System.nanoTime() - t0);
                assertEquals(ROWS / 16, matches);
            }

            double avgMs = (totalNanos / (double) MEASURE_ITERS) / 1_000_000.0;
            return new BenchmarkResult("UTF8Buffer Zero-Alloc SIMD equalsBytes()", ROWS, avgMs);
        }
    }

    private static BenchmarkResult benchmarkOffHeapDictionaryEncodeAndDecode() {
        String[] categories = new String[32];
        for (int i = 0; i < categories.length; i++) {
            categories[i] = "CATEGORY_REGION_" + i;
        }

        try (ArrowMemoryPool pool = new ArrowMemoryPool(32L * 1024L * 1024L, 4);
             Arena setupArena = Arena.ofConfined()) {

            String[] rawStrings = new String[ROWS];
            for (int i = 0; i < ROWS; i++) {
                rawStrings[i] = categories[i & 31];
            }
            UTF8Buffer sourceUtf8 = UTF8Buffer.allocate(setupArena, "cat", rawStrings);

            for (int w = 0; w < WARMUP_ITERS; w++) {
                try (PooledArena arena = pool.acquireArena()) {
                    DictionaryBuffer dict = DictionaryBuffer.fromUTF8(arena, sourceUtf8);
                    UTF8Buffer decoded = dict.decode(arena);
                    assertEquals(ROWS, decoded.length());
                }
            }

            long totalNanos = 0L;
            for (int m = 0; m < MEASURE_ITERS; m++) {
                try (PooledArena arena = pool.acquireArena()) {
                    long t0 = System.nanoTime();
                    DictionaryBuffer dict = DictionaryBuffer.fromUTF8(arena, sourceUtf8);
                    UTF8Buffer decoded = dict.decode(arena);
                    totalNanos += (System.nanoTime() - t0);
                    assertEquals(32L, dict.cardinality());
                    assertEquals(ROWS, decoded.length());
                }
            }

            double avgMs = (totalNanos / (double) MEASURE_ITERS) / 1_000_000.0;
            return new BenchmarkResult("Dictionary Off-Heap Encode + Decode", ROWS, avgMs);
        }
    }

    private static BenchmarkResult benchmarkBitmapAndTableFilter() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(32L * 1024L * 1024L, 4);
             Arena setupArena = Arena.ofConfined()) {

            Int64Buffer ids = Int64Buffer.allocate(setupArena, "id", ROWS);
            Float64Buffer metrics = Float64Buffer.allocate(setupArena, "metric", ROWS);
            BooleanBuffer maskA = BooleanBuffer.allocate(setupArena, "mA", ROWS);
            BooleanBuffer maskB = BooleanBuffer.allocate(setupArena, "mB", ROWS);

            for (int i = 0; i < ROWS; i++) {
                ids.set(i, i);
                metrics.set(i, i * 1.25);
                maskA.set(i, (i & 1) == 0);
                maskB.set(i, (i & 3) == 0);
            }

            Schema schema = new Schema(List.of(
                    new Field("id", DataType.INT64, false),
                    new Field("metric", DataType.FLOAT64, false)
            ));
            Table table = new Table(schema, List.of(ids, metrics));

            for (int w = 0; w < WARMUP_ITERS; w++) {
                try (PooledArena arena = pool.acquireArena()) {
                    BooleanBuffer combined = Bitmaps.and(maskA, maskB, "mask", arena);
                    Table filtered = table.filter(combined, arena);
                    assertEquals(ROWS / 4, filtered.rowCount());
                }
            }

            long totalNanos = 0L;
            for (int m = 0; m < MEASURE_ITERS; m++) {
                try (PooledArena arena = pool.acquireArena()) {
                    long t0 = System.nanoTime();
                    BooleanBuffer combined = Bitmaps.and(maskA, maskB, "mask", arena);
                    Table filtered = table.filter(combined, arena);
                    totalNanos += (System.nanoTime() - t0);
                    assertEquals(ROWS / 4, filtered.rowCount());
                }
            }

            double avgMs = (totalNanos / (double) MEASURE_ITERS) / 1_000_000.0;
            return new BenchmarkResult("64-Bit Bitmap AND + POPCNT Table.filter()", ROWS, avgMs);
        }
    }

    private static BenchmarkResult benchmarkZeroCopyFfiStreamRoundtrip() {
        try (ArrowMemoryPool pool = new ArrowMemoryPool(16L * 1024L * 1024L, 4);
             Arena setupArena = Arena.ofConfined()) {

            Int32Buffer col1 = Int32Buffer.allocate(setupArena, "c1", ROWS);
            Int64Buffer col2 = Int64Buffer.allocate(setupArena, "c2", ROWS);
            Float64Buffer col3 = Float64Buffer.allocate(setupArena, "c3", ROWS);

            Schema schema = new Schema(List.of(
                    new Field("c1", DataType.INT32, false),
                    new Field("c2", DataType.INT64, false),
                    new Field("c3", DataType.FLOAT64, false)
            ));
            Table table = new Table(schema, List.of(col1, col2, col3));

            for (int w = 0; w < WARMUP_ITERS; w++) {
                try (PooledArena arena = pool.acquireArena()) {
                    MemorySegment cStream = ArrowCExporter.exportTableStream(table, 100_000, arena);
                    try (ArrowTableStream stream = ArrowCImporter.importTableStream(cStream, arena)) {
                        Table roundtrip = stream.readAll(arena);
                        assertEquals(ROWS, roundtrip.rowCount());
                    }
                }
            }

            long totalNanos = 0L;
            for (int m = 0; m < MEASURE_ITERS; m++) {
                try (PooledArena arena = pool.acquireArena()) {
                    long t0 = System.nanoTime();
                    MemorySegment cStream = ArrowCExporter.exportTableStream(table, 100_000, arena);
                    try (ArrowTableStream stream = ArrowCImporter.importTableStream(cStream, arena)) {
                        Table roundtrip = stream.readAll(arena);
                        assertEquals(ROWS, roundtrip.rowCount());
                    }
                    totalNanos += (System.nanoTime() - t0);
                }
            }

            double avgMs = (totalNanos / (double) MEASURE_ITERS) / 1_000_000.0;
            return new BenchmarkResult("Zero-Copy FFI ArrowArrayStream (10 Batches)", ROWS, avgMs);
        }
    }

    private static void printReport(List<BenchmarkResult> results) {
        String border = "+-----------------------------------------------+--------------+--------------+------------------+";
        System.out.println("\n" + border);
        System.out.printf(Locale.ROOT, "| %-45s | %12s | %12s | %16s |%n",
                "jdata-core 1-Million-Row Benchmark", "Rows", "Avg Time", "Throughput");
        System.out.println(border);
        for (BenchmarkResult r : results) {
            double millionRowsPerSec = (r.rows / (r.avgMillis / 1000.0)) / 1_000_000.0;
            System.out.printf(Locale.ROOT, "| %-45s | %,12d | %9.2f ms | %10.1f M/sec |%n",
                    r.name, r.rows, r.avgMillis, millionRowsPerSec);
        }
        System.out.println(border + "\n");
    }

    private record BenchmarkResult(String name, long rows, double avgMillis) {}
}