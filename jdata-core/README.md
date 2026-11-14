# jdata-core

[![Maven Central](https://img.shields.io/maven-central/v/io.github.shridey/jdata-core.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.shridey/jdata-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-25%2B-orange.svg)](https://openjdk.org/projects/jdk/25/)

**`jdata-core`** is a high-performance, zero-dependency, pure-Java implementation of the **Apache Arrow Columnar Memory Format (v1.4)** and the **Apache Arrow C Data & C Stream Interfaces**, built natively on the Java 25 Foreign Function & Memory (FFM) API (`java.lang.foreign`).

It delivers deterministic off-heap memory management, $O(1)$ pooled slab allocation, strict 64-byte SIMD alignment and tail padding, zero-heap string and dictionary encoding, discontiguous chunked columns, 64-bit word-accelerated bitmap operations, and zero-copy FFI interoperability with C++, Rust, DuckDB, Polars, and PyArrow—**without Netty, `sun.misc.Unsafe`, JNI, or reflective `--add-opens` hacks**.

---

## Installation

### Maven

```xml
<dependency>
    <groupId>io.github.shridey</groupId>
    <artifactId>jdata-core</artifactId>
    <version>0.3.1</version>
</dependency>
```

### Gradle (Kotlin DSL)

```kotlin
implementation("io.github.shridey:jdata-core:0.3.1")
```

### Runtime JVM Flag (When Using FFI)
Standard buffer, builder, and table operations require zero JVM flags. When invoking native downcalls or upcalls via `io.github.shridey.jdata.core.ffi` (`ArrowCExporter`, `ArrowCImporter`, `ArrowTableStream`), pass `--enable-native-access`:

```bash
# Running on the module path (JPMS)
java --enable-native-access=jdata.core -p your-app.jar -m your.module/your.Main

# Running on the classpath
java --enable-native-access=ALL-UNNAMED -jar your-app.jar
```

---

## Package Architecture

| Package | Description | Key Classes & Interfaces |
| :--- | :--- | :--- |
| `io.github.shridey.jdata.core` | Root column contract and Arrow type definitions | `DataBuffer`, `DataType` |
| `io.github.shridey.jdata.core.memory` | Thread-safe off-heap slab pool and scoped arenas | `ArrowMemoryPool`, `PooledArena` |
| `io.github.shridey.jdata.core.buffer` | Contiguous buffers, builders, dictionaries, chunks, and bitmaps | `Int32Buffer`, `Int64Buffer`, `Float32Buffer`, `Float64Buffer`, `BooleanBuffer`, `UTF8Buffer`, `DictionaryBuffer`, `ChunkedBuffer`, `Bitmaps` |
| `io.github.shridey.jdata.core.layout` | Relational schema metadata and columnar tables | `Field`, `Schema`, `Table` |
| `io.github.shridey.jdata.core.ffi` | Zero-copy Apache Arrow C Data & C Stream FFI bridge | `ArrowCExporter`, `ArrowCImporter`, `ArrowTableStream`, `ImportedDataBuffer` |

---

## Real-World Developer Guide & Use Cases

### 1. High-Throughput Memory Pooling (`ArrowMemoryPool` & `PooledArena`)
Instantiate a single `ArrowMemoryPool` at application startup and check out a `PooledArena` per query or batch. When the `try`-with-resources block exits, only the dirty prefix of each slab is zeroed and returned to the pool in $O(1)$ time.

```java
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import io.github.shridey.jdata.core.buffer.Int64Buffer;
import io.github.shridey.jdata.core.memory.ArrowMemoryPool;
import io.github.shridey.jdata.core.memory.PooledArena;

try (ArrowMemoryPool pool = new ArrowMemoryPool()) {
    try (PooledArena arena = pool.acquireArena()) {
        Int64Buffer orderIds = Int64Buffer.allocate(arena, "order_id", 10_000);
        Float64Buffer totals = Float64Buffer.allocate(arena, "total", 10_000);

        orderIds.set(0, 90001L);
        totals.set(0, 249.95);
        totals.setNull(1);
    } // <-- Slabs are wiped and returned to the pool here in O(1) time
}
```

---

### 2. Building Columns, Zero-Alloc UTF-8 Inspection & Tables (`BufferBuilder`, `UTF8Buffer`, `Table`)

```java
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;
import io.github.shridey.jdata.core.layout.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.List;

try (Arena arena = Arena.ofConfined()) {
    Int64Buffer timestamps = new Int64BufferBuilder(arena, "ts", 128)
            .appendAll(new long[]{1700000001000L, 1700000002000L, 1700000003000L})
            .build();

    Float64Buffer prices = new Float64BufferBuilder(arena, "price", 128)
            .append(185.25)
            .appendNull()
            .append(186.00)
            .build();

    UTF8Buffer symbols = new UTF8BufferBuilder(arena, "symbol", 128)
            .appendAll(new String[]{"AAPL", "MSFT", "NVDA"})
            .build();

    // Zero-allocation off-heap UTF-8 inspection and SIMD comparison
    int byteLen = symbols.getByteLength(0);                 // 4
    MemorySegment slice = symbols.getByteSlice(0);          // Zero-copy 4-byte MemorySegment
    MemorySegment target = MemorySegment.ofArray("AAPL".getBytes(StandardCharsets.UTF_8));
    boolean isAapl = symbols.equalsBytes(0, target);        // true (SIMD MemorySegment.mismatch)
    int cmp = symbols.compareBytes(0, symbols, 1);          // < 0 ("AAPL" < "MSFT")

    Schema schema = new Schema(List.of(
            new Field("ts", DataType.INT64, false),
            new Field("price", DataType.FLOAT64, true),
            new Field("symbol", DataType.UTF8, false)
    ));

    Table trades = new Table(schema, List.of(timestamps, prices, symbols));
    Table sliceView = trades.slice(0, 2); // O(1) zero-copy horizontal slice
}
```

---

### 3. Categorical Compression with Dictionary Encoding (`DictionaryBuffer`)

```java
import io.github.shridey.jdata.core.buffer.*;
import java.lang.foreign.Arena;

try (Arena arena = Arena.ofConfined()) {
    String[] regions = {"APAC", "EMEA", "APAC", null, "AMER", "EMEA", "APAC"};
    DictionaryBuffer regionCol = DictionaryBuffer.allocate(arena, "region", regions);

    long uniqueCount = regionCol.cardinality();   // 3 ("APAC", "EMEA", "AMER")
    String val = regionCol.getString(2);          // "APAC"
    Int32Buffer codeVector = regionCol.indices(); // Zero-copy Int32Buffer view for SIMD kernels
    UTF8Buffer decoded = regionCol.decode(arena); // Off-heap decode to flat UTF8Buffer
}
```

---

### 4. Unbounded Streaming & Multi-Gigabyte Strings (`ChunkedBuffer` & `ChunkedBufferBuilder`)

```java
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;
import java.lang.foreign.Arena;

try (Arena arena = Arena.ofConfined()) {
    ChunkedBufferBuilder logBuilder = new ChunkedBufferBuilder(arena, "message", DataType.UTF8, 4096);
    for (int i = 0; i < 15_000; i++) {
        logBuilder.append("log_entry_" + i);
    }

    ChunkedBuffer chunkedLogs = logBuilder.build();
    String msg = chunkedLogs.getString(12_345);           // O(log C) binary-searched lookup
    ChunkedBuffer window = chunkedLogs.slice(3_000, 500); // Cross-chunk zero-copy slice
}
```

---

### 5. 64-Bit Word Bitmap Logic & Late-Materialization Filtering (`Bitmaps` & `Table.filter`)

```java
import io.github.shridey.jdata.core.buffer.*;
import io.github.shridey.jdata.core.layout.Table;
import java.lang.foreign.Arena;

try (Arena arena = Arena.ofConfined()) {
    BooleanBuffer isPaid = BooleanBuffer.allocate(arena, "paid", 5);
    isPaid.copyFrom(new boolean[]{false, true, true, true, true});

    BooleanBuffer isActive = BooleanBuffer.allocate(arena, "active", 5);
    isActive.copyFrom(new boolean[]{true, true, false, true, true});

    // 64-bit word-level boolean algebra + hardware POPCNT / TZCNT Table filtering
    BooleanBuffer mask = Bitmaps.and(isPaid, isActive, "mask", arena);
    // Table filtered = table.filter(mask, arena);
}
```

---

### 6. Zero-Copy Native FFI Interop (`ArrowCExporter`, `ArrowCImporter`, `ArrowTableStream`)

```java
import io.github.shridey.jdata.core.buffer.Int64Buffer;
import io.github.shridey.jdata.core.ffi.*;
import io.github.shridey.jdata.core.layout.Table;
import io.github.shridey.jdata.core.memory.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

try (ArrowMemoryPool pool = new ArrowMemoryPool();
     Arena streamArena = Arena.ofConfined()) {

    MemorySegment cStreamPtr = ArrowCExporter.exportTableStream(largeTable, 50_000, streamArena);

    try (ArrowTableStream stream = ArrowCImporter.importTableStream(cStreamPtr, streamArena)) {
        while (true) {
            try (PooledArena batchArena = pool.acquireArena()) {
                Table batch = stream.nextBatch(batchArena);
                if (batch == null) break;

                // Obtain a zero-copy typed native view over any imported column
                Int64Buffer nativeCol = ((ImportedDataBuffer) batch.getColumn(0)).asInt64Buffer();
            }
        }
    }
}
```

---

## Benchmarks (`v0.3.1`)

### 1. In-Memory Single-Batch Throughput (1,000,000 Rows)
Executed on Java 25 (`OneMillionRowsBenchmarkTest`):

| Benchmark Operation | Rows | Avg Time | Throughput |
| :--- | ---: | ---: | ---: |
| **PooledSlab Allocation & Checkout (`PooledArena`)** | 8,192,000 | 4.83 ms | **1,695.5 M rows/sec** |
| **Zero-Copy FFI `ArrowArrayStream` (10 Batches)** | 1,000,000 | 2.00 ms | **498.8 M rows/sec** |
| **64-Bit `Bitmaps.and()` + `POPCNT` `Table.filter()`** | 1,000,000 | 5.55 ms | **180.3 M rows/sec** |
| **`UTF8Buffer` Zero-Alloc SIMD `equalsBytes()` Scan** | 1,000,000 | 7.47 ms | **133.9 M rows/sec** |
| **`Int64BufferBuilder` (In-Place Slab Append)** | 1,000,000 | 9.74 ms | **102.7 M rows/sec** |
| **`ChunkedBufferBuilder` (Zero-Copy Rollover)** | 1,000,000 | 10.71 ms | **93.4 M rows/sec** |
| **Off-Heap `DictionaryBuffer` Encode + Decode** | 1,000,000 | 33.30 ms | **30.0 M rows/sec** |

### 2. Out-of-Core & Chunked Scale (1,000,000,000 Rows)
Executed on Java 25 (`OneBillionRowsBenchmarkTest`) on a 16 GB RAM machine using `ArrowMemoryPool` slab recycling to process up to 20 GB of logical columnar data within a 16–32 MB physical off-heap footprint:

| 1-Billion-Row Benchmark Operation | Rows | Time | Peak Pool RAM | Throughput |
| :--- | ---: | ---: | ---: | ---: |
| **1B-Row Chunked Table Concat + Slice + Lookup** | 1,000,000,000 | 4.03 ms | 32.0 MB | **247.91 B rows/sec** |
| **1B-Row Chunked `UTF8`/`Dict` `take()` + SIMD Scan** | 1,000,000,000 | 43.44 ms | 32.0 MB | **23.02 B rows/sec** |
| **1B-Row Zero-Copy FFI Stream (20 GB Logical)** | 1,000,000,000 | 98.48 ms | 16.0 MB | **10.15 B rows/sec** |
| **1B-Row 64-Bit Word `Bitmaps.and()` Intersection** | 1,000,000,000 | 113.74 ms | 16.0 MB | **8.79 B rows/sec** |
| **1B-Row Pooled `Int64` Allocation (8 GB Logical)** | 1,000,000,000 | 250.63 ms | 16.0 MB | **3.99 B rows/sec** |

To run the benchmark suites locally:

```bash
mvn test -Dtest=OneMillionRowsBenchmarkTest,OneBillionRowsBenchmarkTest
```

---

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).