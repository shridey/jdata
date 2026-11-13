# jdata-core

[![Maven Central](https://img.shields.io/maven-central/v/io.github.shridey/jdata-core.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.shridey/jdata-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-25%2B-orange.svg)](https://openjdk.org/projects/jdk/25/)

**`jdata-core`** is a high-performance, zero-dependency, pure-Java implementation of the **Apache Arrow Columnar Memory Format (v1.4)** and the **Apache Arrow C Data & C Stream Interfaces**, built natively on the Java 25 Foreign Function & Memory (FFM) API (`java.lang.foreign`).

It delivers deterministic off-heap memory management, $O(1)$ pooled slab allocation, strict 64-byte SIMD alignment and tail padding, zero-heap string and dictionary encoding, discontiguous chunked columns, 64-bit word-accelerated bitmap operations, and zero-copy FFI interoperability with C++, Rust, DuckDB, Polars, and PyArrow—**without Netty, `sun.misc.Unsafe`, JNI, or reflective `--add-opens` hacks**.

---

## Why `jdata-core`?

* **Zero External Dependencies:** Built 100% on standard `java.lang.foreign` (`Arena`, `MemorySegment`, `ValueLayout`, `Linker`).
* **64-Byte SIMD & AVX-512 Ready:** Every physical buffer is allocated on a 64-byte aligned address and padded to a 64-byte multiple so 512-bit vector kernels can execute unmasked loads across column tails without out-of-bounds faults.
* **Deterministic `Arena` Lifecycles:** Columns and tables borrow an `Arena` rather than requiring manual per-column `.close()` calls and reference-counting boilerplate. Closing an `Arena` immediately invalidates all associated memory views and prevents use-after-free segfaults.
* **Slab Pooling & In-Place Tip Growth:** `ArrowMemoryPool` and `PooledArena` sub-allocate from reusable 4 MB off-heap slabs in $O(1)$ pointer-bump time, expand active builder buffers in-place without copying, and recycle intermediate resize blocks across columns.
* **Zero-Heap String & Dictionary Pipelines:** UTF-8 validation, surrogate-safe encoding, and 64-bit word-hashed dictionary deduplication (`MemorySegment.mismatch()`) run directly in off-heap memory.

---

## Installation

### Maven

```xml
<dependency>
    <groupId>io.github.shridey</groupId>
    <artifactId>jdata-core</artifactId>
    <version>0.3.0</version>
</dependency>
```

### Gradle (Kotlin DSL)

```kotlin
implementation("io.github.shridey:jdata-core:0.3.0")
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
**Scenario:** You are building a low-latency analytics service or query engine that processes thousands of requests per second. Allocating and freeing OS memory via `Arena.ofConfined()` on every request triggers kernel `mmap`/`munmap` syscalls and page faults.

**Solution:** Instantiate a single `ArrowMemoryPool` at application startup and check out a `PooledArena` per query or worker thread. When the `try`-with-resources block exits, only the dirty prefix of each slab is zeroed and returned to the pool in $O(1)$ time.

```java
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import io.github.shridey.jdata.core.buffer.Int64Buffer;
import io.github.shridey.jdata.core.memory.ArrowMemoryPool;
import io.github.shridey.jdata.core.memory.PooledArena;

// 1. Create one long-lived pool (default: 4 MB slabs, up to 16 cached idle slabs = 64 MB)
try (ArrowMemoryPool pool = new ArrowMemoryPool()) {

    // 2. Per-query or per-batch: acquire a fast thread-confined PooledArena
    try (PooledArena arena = pool.acquireArena()) {
        Int64Buffer orderIds = Int64Buffer.allocate(arena, "order_id", 10_000);
        Float64Buffer totals = Float64Buffer.allocate(arena, "total", 10_000);

        orderIds.set(0, 90001L);
        totals.set(0, 249.95);

        // Null handling is O(1) and bit-packed in an off-heap validity bitmap
        totals.setNull(1);
        boolean isRow1Null = totals.isNull(1); // true

        // Healing a null slot back to valid happens automatically on set() or setValid()
        totals.set(1, 99.00); // isNull(1) is now false, nullCount() is 0
    } // <-- All slabs are automatically wiped and returned to the pool here

    // 3. For multi-threaded parallel query stages, acquire a thread-safe shared arena:
    try (PooledArena sharedArena = pool.acquireSharedArena()) {
        // Safe for concurrent allocations across worker threads
    }
}
```

---

### 2. Building Financial & Telemetry Tables (`BufferBuilder`, `Schema`, `Table`)
**Scenario:** You are ingesting a stream of market trades where the final row count is unknown upfront, and you need to construct a validated columnar `Table`, project specific columns, and partition the table across worker threads with zero memory copying.

```java
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;
import io.github.shridey.jdata.core.layout.*;
import java.lang.foreign.Arena;
import java.util.List;

try (Arena arena = Arena.ofConfined()) {
    // 1. Dynamically build primitive, boolean, and UTF-8 columns
    Int64Buffer timestamps = new Int64BufferBuilder(arena, "ts", 128)
            .append(1700000001000L)
            .appendAll(new long[]{1700000002000L, 1700000003000L, 1700000004000L})
            .build();

    Float64Buffer prices = new Float64BufferBuilder(arena, "price", 128)
            .append(185.25)
            .append(185.50)
            .appendNull() // Missing tick price
            .append(186.00)
            .build();

    BooleanBuffer isBuy = new BooleanBufferBuilder(arena, "is_buy", 128)
            .appendAll(new boolean[]{true, false, true, true})
            .build();

    UTF8Buffer symbols = new UTF8BufferBuilder(arena, "symbol", 128)
            .appendAll(new String[]{"AAPL", "MSFT", "NVDA", "AAPL"})
            .build();

    // 2. Define a strict Schema (enforces column names, types, row counts, and nullability)
    Schema schema = new Schema(List.of(
            new Field("ts", DataType.INT64, false),
            new Field("price", DataType.FLOAT64, true),
            new Field("is_buy", DataType.BOOLEAN, false),
            new Field("symbol", DataType.UTF8, false)
    ));

    Table trades = new Table(schema, List.of(timestamps, prices, isBuy, symbols));

    // 3. O(1) Zero-Copy Table Slicing & Structural Transformations
    Table firstTwo = trades.head(2);
    Table lastTwo = trades.tail(2);
    Table window = trades.slice(1, 2); // Rows [1, 2] — zero bytes copied!

    // Project, rename, add, or drop columns in O(1) metadata time
    Table summary = trades
            .select("ts", "symbol", "price")
            .renameColumn("symbol", "ticker")
            .dropColumn("ts");
}
```

#### Wrapping Pre-Allocated or Memory-Mapped Segments (`wrap()`)
Every physical buffer provides a zero-copy `wrap()` factory for wrapping `FileChannel.map()` segments or pre-computed SIMD output segments:

```java
Int64Buffer mappedCol = Int64Buffer.wrap("id", arena, rowCount, 0L, validitySegment, dataSegment);
UTF8Buffer mappedStr = UTF8Buffer.wrap("name", arena, rowCount, 0L, validitySeg, offsetsSeg, utf8BytesSeg);
```

---

### 3. Categorical Compression with Dictionary Encoding (`DictionaryBuffer`)
**Scenario:** Your dataset contains millions of rows with repeated categorical strings (`country`, `currency`, `status`, `department`). Storing raw UTF-8 strings wastes gigabytes of RAM and slows down equality filters and group-bys.

**Solution:** `DictionaryBuffer` compresses repeated strings into 32-bit integer codes (`INT32` indices $\rightarrow$ deduplicated `UTF8Buffer` dictionary). `DictionaryBufferBuilder` hashes and deduplicates UTF-8 bytes directly off-heap.

```java
import io.github.shridey.jdata.core.buffer.*;
import java.lang.foreign.Arena;

try (Arena arena = Arena.ofConfined()) {
    // 1. Encode from a Java String[]
    String[] regions = {"APAC", "EMEA", "APAC", null, "AMER", "EMEA", "APAC"};
    DictionaryBuffer regionCol = DictionaryBuffer.allocate(arena, "region", regions);

    System.out.println("Total rows: " + regionCol.length());          // 7
    System.out.println("Unique values: " + regionCol.cardinality());  // 3 ("APAC", "EMEA", "AMER")
    System.out.println("Row 2 string: " + regionCol.getString(2));    // "APAC"
    System.out.println("Row 2 dict code: " + regionCol.getIndex(2));  // 0

    // 2. Encode an existing off-heap UTF8Buffer into a DictionaryBuffer with ZERO heap String allocations
    UTF8Buffer rawUtf8 = UTF8Buffer.allocate(arena, "status", new String[]{"OPEN", "CLOSED", "OPEN"});
    DictionaryBuffer encodedOffHeap = DictionaryBuffer.fromUTF8(arena, rawUtf8);

    // 3. Zero-copy Int32Buffer view over the dictionary codes (for vectorized SIMD kernels)
    Int32Buffer codeVector = regionCol.indices();

    // 4. Lazy dictionary compaction when rebuilding a slice
    DictionaryBuffer slice = regionCol.slice(4, 3); // ["AMER", "EMEA", "APAC"]
    DictionaryBuffer compactedSlice = slice.toBuilder(arena).build();

    // 5. Decode back into a flat UTF8Buffer completely off-heap
    UTF8Buffer flatUtf8 = regionCol.decode(arena);
}
```

---

### 4. Unbounded Streaming & Multi-Gigabyte Strings (`ChunkedBuffer` & `ChunkedBufferBuilder`)
**Scenario:** You are streaming millions of log lines from Kafka or reading a multi-gigabyte CSV/Parquet file. Standard single-array buffers must reallocate and copy memory when doubling capacity, and standard Arrow `UTF8` buffers hit a hard 2 GB (`Integer.MAX_VALUE`) offset limit.

**Solution:** `ChunkedBufferBuilder` writes into fixed-capacity physical chunks (default: 65,536 rows) that fit cleanly inside `ArrowMemoryPool` slabs with **zero resize copying**, and automatically rolls over `UTF8` chunks before reaching 2 GB.

```java
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;
import io.github.shridey.jdata.core.layout.Table;
import java.lang.foreign.Arena;
import java.util.List;

try (Arena arena = Arena.ofConfined()) {
    // 1. Stream rows into fixed 4,096-row chunks with zero power-of-two resize copies
    ChunkedBufferBuilder logBuilder = new ChunkedBufferBuilder(arena, "message", DataType.UTF8, 4096);
    for (int i = 0; i < 15_000; i++) {
        logBuilder.append("log_entry_" + i);
    }

    ChunkedBuffer chunkedLogs = logBuilder.build();
    System.out.println("Total rows: " + chunkedLogs.length());     // 15,000
    System.out.println("Physical chunks: " + chunkedLogs.chunkCount()); // 4

    // 2. O(log C) binary-searched random access across chunks
    String msg = chunkedLogs.getString(12_345); // "log_entry_12345"

    // 3. Cross-chunk zero-copy slicing (only slices the boundary chunks)
    ChunkedBuffer crossChunkSlice = chunkedLogs.slice(3_000, 6_000);

    // 4. O(1) Zero-Copy Table Concatenation
    Table combined = batchTable1.concat(batchTable2);
    Table manyCombined = Table.concat(List.of(batchTable1, batchTable2, batchTable3));

    // 5. Coalesce into a single contiguous physical buffer when needed
    UTF8Buffer contiguous = (UTF8Buffer) crossChunkSlice.compact(arena);
}
```

---

### 5. 64-Bit Word Bitmap Logic & Late-Materialization Filtering (`Bitmaps` & `Table.filter`)
**Scenario:** You want to evaluate compound boolean predicates (`(price > 100.0) AND (is_active == true)`) and filter a multi-column `Table` without branching row-by-row or materializing intermediate columns prematurely.

**Solution:** `Bitmaps` processes 64 boolean and validity bits per CPU cycle using unaligned `long` word loads, and extracts a 32-bit `SelectionVector` (`Int32Buffer`) via hardware `POPCNT` (`Long.bitCount`) and `TZCNT` (`Long.numberOfTrailingZeros`) instructions.

```java
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;
import io.github.shridey.jdata.core.layout.*;
import java.lang.foreign.Arena;
import java.util.List;

try (Arena arena = Arena.ofConfined()) {
    Int32Buffer userIds = Int32Buffer.allocate(arena, "user_id", 5);
    userIds.copyFrom(new int[]{10, 20, 30, 40, 50});

    DictionaryBuffer tiers = DictionaryBuffer.allocate(arena, "tier",
            new String[]{"Free", "Pro", "Enterprise", "Pro", "Enterprise"});

    Schema schema = new Schema(List.of(
            new Field("user_id", DataType.INT32, false),
            new Field("tier", DataType.DICTIONARY, false)
    ));
    Table users = new Table(schema, List.of(userIds, tiers));

    // Predicate masks produced by comparison kernels
    BooleanBuffer isPaidMask = BooleanBuffer.allocate(arena, "paid", 5);
    isPaidMask.copyFrom(new boolean[]{false, true, true, true, true});

    BooleanBuffer isActiveMask = BooleanBuffer.allocate(arena, "active", 5);
    isActiveMask.copyFrom(new boolean[]{true, true, false, true, true});

    // 1. 64-bit word-level boolean algebra (and, or, xor, not) with automatic null propagation
    BooleanBuffer finalMask = Bitmaps.and(isPaidMask, isActiveMask, "paid_and_active", arena);

    // 2. Filter the Table in one call (uses POPCNT + TZCNT SelectionVector under the hood)
    Table activePaidUsers = users.filter(finalMask, arena);
    // Result rows: user_id = [20, 40, 50], tier = ["Pro", "Pro", "Enterprise"]

    // 3. Or extract the SelectionVector explicitly for custom late materialization / sorting
    Int32Buffer selectionIndices = Bitmaps.toSelectionIndices(finalMask, arena); // [1, 3, 4]
    Table gathered = users.take(selectionIndices, arena);
}
```

---

### 6. Zero-Copy Native FFI Interop (`ArrowCExporter`, `ArrowCImporter`, `ArrowTableStream`)
**Scenario:** You want to pass a `Table` directly to DuckDB, Polars, PyArrow, or a custom C++/Rust library in the same process without serializing to Protobuf, Parquet, or JSON—or stream a 100 GB dataset across FFI in bounded memory.

#### Single-Batch Zero-Copy Export & Import (`ArrowArray` & `ArrowSchema`)

```java
import io.github.shridey.jdata.core.ffi.*;
import io.github.shridey.jdata.core.layout.Table;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

try (Arena exportArena = Arena.ofConfined();
     Arena importArena = Arena.ofConfined()) {

    // Export a Table to native ArrowArray and ArrowSchema C structs
    MemorySegment cArrayPtr = ArrowCExporter.exportTableArray(table, exportArena);
    MemorySegment cSchemaPtr = ArrowCExporter.exportTableSchema(table, exportArena);

    // Pass cArrayPtr.address() and cSchemaPtr.address() to C++/Rust/DuckDB/PyArrow,
    // or import foreign C pointers directly into a jdata Table:
    Table importedTable = ArrowCImporter.importTable(cArrayPtr, cSchemaPtr, importArena);

    ImportedDataBuffer idCol = (ImportedDataBuffer) importedTable.getColumn("user_id");
    int firstUserId = idCol.getInt(0);
} // <-- Closing importArena automatically invokes the native ArrowArray.release callback!
```

#### Out-of-Core Multi-Batch Streaming (`ArrowArrayStream`)

```java
import io.github.shridey.jdata.core.ffi.*;
import io.github.shridey.jdata.core.layout.Table;
import io.github.shridey.jdata.core.memory.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

try (ArrowMemoryPool pool = new ArrowMemoryPool();
     Arena streamArena = Arena.ofConfined()) {

    // Export a Table (or any lazy Iterator<Table>) as an ArrowArrayStream in 50,000-row batches
    MemorySegment cStreamPtr = ArrowCExporter.exportTableStream(largeTable, 50_000, streamArena);

    // Pull batches on demand using ArrowTableStream
    try (ArrowTableStream stream = ArrowCImporter.importTableStream(cStreamPtr, streamArena)) {
        System.out.println("Stream schema: " + stream.schema());

        while (true) {
            // Bind each batch to a short-lived PooledArena so memory is recycled every iteration
            try (PooledArena batchArena = pool.acquireArena()) {
                Table batch = stream.nextBatch(batchArena);
                if (batch == null) {
                    break; // End of stream reached
                }
                // Process batch in constant RAM...
            }
        }
    }
}
```

---

## Choosing Your Execution Pattern: In-Memory vs. Out-of-Core (Larger-Than-RAM)

1. **In-Memory Mode (Dataset fits in RAM):** Allocate columns inside a single `Arena` or `PooledArena` and operate on a unified `Table` (or use `ChunkedBufferBuilder` to avoid resize copies and 2 GB string limits).
2. **Out-of-Core Streaming Mode (Processing 20+ GB on a 16 GB Machine):** Create a single `ArrowMemoryPool`, iterate over batches via `ArrowTableStream`, and open a `try (PooledArena batchArena = pool.acquireArena())` block *inside* your batch loop. Each batch reuses the exact same 16 MB off-heap slab, allowing you to process billions of rows with a flat 16 MB memory footprint.

## Benchmarks

### 1. In-Memory Single-Batch Throughput (1,000,000 Rows)
Executed on Java 25 (`OneMillionRowsBenchmarkTest`)

| `jdata-core` Benchmark Operation | Rows | Avg Time | Throughput |
| :--- | ---: | ---: | ---: |
| **PooledSlab Allocation & Checkout (`PooledArena`)** | 8,192,000 | 3.81 ms | **2,149.7 M rows/sec** |
| **Zero-Copy FFI `ArrowArrayStream` (10 Batches)** | 1,000,000 | 2.76 ms | **361.9 M rows/sec** |
| **`Int64BufferBuilder` (In-Place Slab Append)** | 1,000,000 | 7.23 ms | **138.3 M rows/sec** |
| **`ChunkedBufferBuilder` (Zero-Copy Rollover)** | 1,000,000 | 9.26 ms | **108.0 M rows/sec** |
| **Off-Heap `DictionaryBuffer` Encode + Decode** | 1,000,000 | 33.50 ms | **29.8 M rows/sec** |
| **64-Bit `Bitmaps.and()` + `POPCNT` `Table.filter()`** | 1,000,000 | 34.29 ms | **29.2 M rows/sec** |

### 2. Out-of-Core & Chunked Scale (1,000,000,000 Rows)
Executed on Java 25 (`OneBillionRowsBenchmarkTest`) using `ArrowMemoryPool` slab recycling to process multi-gigabyte logical datasets within a 16–32 MB physical off-heap footprint:

| 1-Billion-Row Benchmark Operation | Rows | Time | Peak Pool RAM | Throughput |
| :--- | ---: | ---: | ---: | ---: |
| **Pooled `Int64` Allocation (8 GB Logical)** | 1,000,000,000 | 203.02 ms | 16.0 MB | **4.93 B rows/sec** |
| **64-Bit Word `Bitmaps.and()` Intersection** | 1,000,000,000 | 111.47 ms | 16.0 MB | **8.97 B rows/sec** |
| **Zero-Copy FFI Stream (20 GB Logical)** | 1,000,000,000 | 85.71 ms | 16.0 MB | **11.67 B rows/sec** |
| **Chunked Table Concat + Slice + Lookup** | 1,000,000,000 | 5.39 ms | 32.0 MB | **185.37 B rows/sec** |

To run the benchmark suite locally:

```bash
mvn test -Dtest=OneMillionRowsBenchmarkTest,OneBillionRowsBenchmarkTest
```

---

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).