# jdata

[![Maven Central](https://img.shields.io/maven-central/v/io.github.shridey/jdata-core.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.shridey/jdata-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-25%2B-orange.svg)](https://openjdk.org/projects/jdk/25/)

**`jdata`** is a high-performance, off-heap Apache Arrow memory foundation and zero-allocation analytical compute ecosystem for modern Java (Java 25+).

Built from the ground up on the **Java Foreign Function & Memory (FFM) API** (`java.lang.foreign`), `jdata` brings native C++/Rust-class columnar memory layout, $O(1)$ slab pooling, 64-byte SIMD tail padding, zero-heap string and dictionary encoding, and zero-copy Foreign Function Interface (FFI) streaming to the JVM—**with zero external dependencies, zero JNI, zero Netty, and zero `sun.misc.Unsafe` hacks**.

---

## Ecosystem Architecture & Modules

`jdata` is organized as a modular, layered reactor where each module has a strict, focused responsibility:

| Module | Status | Description |
| :--- | :--- | :--- |
| **[`jdata-core`](./jdata-core)** | **Available (`v0.3.0`)** | Off-heap 64-byte-aligned Apache Arrow memory engine, $O(1)$ slab pooling (`ArrowMemoryPool`, `PooledArena`), contiguous & chunked columnar buffers, off-heap dictionary encoding, 64-bit word bitmap algebra, and zero-copy Arrow C Data & C Stream FFI. |
| **`jdata-num`** | *In Roadmap* | Hardware-accelerated 512-bit SIMD vectorized compute kernels (`jdk.incubator.vector`) for branchless arithmetic, reductions, comparisons, and dictionary-coded group-bys. |
| **`jdata-io`** | *In Roadmap* | Zero-copy memory-mapped Apache Arrow IPC/Feather readers, Parquet integration, and streaming SIMD CSV ingestion backed by `ChunkedBufferBuilder` and `ArrowTableStream`. |
| **`jdata-df`** | *In Roadmap* | Expressive, developer-friendly DataFrame API and lazy relational query planner with morsel-driven multi-threaded execution. |

---

## Core Architectural Pillars

1. **100% Pure Modern Java (`java.lang.foreign`):**
   Uses standard JDK `Arena`, `MemorySegment`, `ValueLayout`, and `Linker` APIs. No JNI C++ bridges to compile, no Netty allocator dependencies, and no `--add-opens=java.base/java.nio=ALL-UNNAMED` reflective workarounds.
2. **64-Byte Alignment & 64-Byte SIMD Tail Padding:**
   Every physical validity bitmap, offset buffer, and data buffer is aligned to a 64-byte memory address and padded to a 64-byte boundary. Downstream SIMD kernels (`AVX-512` / `SVE`) can execute full 512-bit vector loads across column tails without bounds faults or scalar cleanup loops.
3. **Conveyor-Belt Slab Pooling (`ArrowMemoryPool` & `PooledArena`):**
   Sub-allocates from reusable 4 MB off-heap slabs in $O(1)$ pointer-bump time, expands active builder buffers in-place at the slab tip without copying, recycles relocated resize blocks across columns, and wipes only the dirty prefix on close—allowing 20+ GB streaming workloads to execute inside a flat 16 MB physical RAM footprint.
4. **Zero-GC Off-Heap String & Dictionary Pipelines:**
   Surrogate-safe UTF-8 encoding writes directly to off-heap segments. `DictionaryBufferBuilder` hashes 64-bit off-heap words and deduplicates strings using hardware-intrinsified `MemorySegment.mismatch()` without allocating `java.lang.String` objects on the heap.
5. **Deterministic Temporal Safety:**
   Columns and `Table`s borrow an `Arena` rather than requiring manual per-vector `.close()` reference counting. Closing an `Arena` immediately invalidates all issued segments and triggers native FFI release callbacks automatically.
6. **Zero-Copy Cross-Language Interop (Arrow C Data & C Stream ABI):**
   Exchange single batches (`ArrowArray` / `ArrowSchema`) or unbounded multi-batch streams (`ArrowArrayStream`) at raw memory pointer speed with DuckDB, Polars, PyArrow, Velox, or custom C++/Rust runtimes in the same process.

---

## Quick Start

### 1. Add `jdata-core` to Your Project

#### Maven
```xml
<dependency>
    <groupId>io.github.shridey</groupId>
    <artifactId>jdata-core</artifactId>
    <version>0.3.0</version>
</dependency>
```

#### Gradle (Kotlin DSL)
```kotlin
implementation("io.github.shridey:jdata-core:0.3.0")
```

*(Note: Standard memory, buffer, and table operations require zero JVM flags. When using `io.github.shridey.jdata.core.ffi` for native C Data / C Stream downcalls and upcalls, pass `--enable-native-access=jdata.core` or `--enable-native-access=ALL-UNNAMED` to the JVM).*

---

### 2. In-Memory Table Construction, Bitmap Filtering & Zero-Copy Slicing

```java
import io.github.shridey.jdata.core.DataType;
import io.github.shridey.jdata.core.buffer.*;
import io.github.shridey.jdata.core.layout.*;
import io.github.shridey.jdata.core.memory.*;
import java.util.List;

try (ArrowMemoryPool pool = new ArrowMemoryPool();
     PooledArena arena = pool.acquireArena()) {

    // 1. Build 64-byte aligned & tail-padded off-heap columns
    Int64Buffer ids = new Int64BufferBuilder(arena, "id", 4)
            .appendAll(new long[]{1001L, 1002L, 1003L, 1004L})
            .build();

    Float64Buffer amounts = new Float64BufferBuilder(arena, "amount", 4)
            .append(150.00)
            .appendNull()
            .append(420.50)
            .append(89.99)
            .build();

    DictionaryBuffer regions = DictionaryBuffer.allocate(
            arena, "region", new String[]{"APAC", "EMEA", "APAC", "AMER"}
    );

    Schema schema = new Schema(List.of(
            new Field("id", DataType.INT64, false),
            new Field("amount", DataType.FLOAT64, true),
            new Field("region", DataType.DICTIONARY, false)
    ));

    Table orders = new Table(schema, List.of(ids, amounts, regions));

    // 2. O(1) zero-copy slicing and column projection
    Table recent = orders.tail(2).select("id", "region");

    // 3. 64-bit word bitmap filtering (POPCNT + TZCNT SelectionVector gather)
    BooleanBuffer mask = BooleanBuffer.allocate(arena, "high_value", 4);
    mask.copyFrom(new boolean[]{true, false, true, false});

    Table filtered = orders.filter(mask, arena);
    System.out.println("Filtered rows: " + filtered.rowCount()); // 2
}
```

---

### 3. Out-of-Core Streaming (Processing Larger-Than-RAM Datasets in Constant Memory)

By combining `ArrowTableStream` with a per-batch `PooledArena`, `jdata` processes datasets larger than physical RAM by recycling the same off-heap slab on every batch:

```java
import io.github.shridey.jdata.core.ffi.*;
import io.github.shridey.jdata.core.layout.Table;
import io.github.shridey.jdata.core.memory.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

try (ArrowMemoryPool pool = new ArrowMemoryPool();
     Arena streamArena = Arena.ofConfined()) {

    MemorySegment cStreamPtr = ArrowCExporter.exportTableStream(sourceTable, 100_000, streamArena);

    try (ArrowTableStream stream = ArrowCImporter.importTableStream(cStreamPtr, streamArena)) {
        while (true) {
            // Each batch checks out and recycles the exact same physical off-heap slab
            try (PooledArena batchArena = pool.acquireArena()) {
                Table batch = stream.nextBatch(batchArena);
                if (batch == null) {
                    break; // End of stream
                }
                // Process 100,000-row batch in constant RAM...
            }
        }
    }
}
```

---

## Performance Benchmarks

### 1. Single-Batch In-Memory Throughput (1,000,000 Rows)
Executed on Java 25 (`JDataCoreBenchmarkTest`):

| Benchmark Operation | Rows | Avg Time | Throughput |
| :--- | ---: | ---: | ---: |
| **PooledSlab Allocation & Checkout (`PooledArena`)** | 8,192,000 | 3.81 ms | **2,149.7 M rows/sec** |
| **Zero-Copy FFI `ArrowArrayStream` (10 Batches)** | 1,000,000 | 2.76 ms | **361.9 M rows/sec** |
| **`Int64BufferBuilder` (In-Place Slab Append)** | 1,000,000 | 7.23 ms | **138.3 M rows/sec** |
| **`ChunkedBufferBuilder` (Zero-Copy Rollover)** | 1,000,000 | 9.26 ms | **108.0 M rows/sec** |
| **Off-Heap `DictionaryBuffer` Encode + Decode** | 1,000,000 | 33.50 ms | **29.8 M rows/sec** |
| **64-Bit `Bitmaps.and()` + `POPCNT` `Table.filter()`** | 1,000,000 | 34.29 ms | **29.2 M rows/sec** |

### 2. Out-of-Core & Chunked Scale (1,000,000,000 Rows)
Executed on Java 25 (`OneBillionRowsBenchmarkTest`) on a 16 GB machine, processing up to 20 GB of logical columnar data within a **16–32 MB physical off-heap memory footprint**:

| 1-Billion-Row Benchmark Operation | Rows | Time | Peak Pool RAM | Throughput |
| :--- | ---: | ---: | ---: | ---: |
| **Pooled `Int64` Allocation (8 GB Logical)** | 1,000,000,000 | 203.02 ms | 16.0 MB | **4.93 B rows/sec** |
| **64-Bit Word `Bitmaps.and()` Intersection** | 1,000,000,000 | 111.47 ms | 16.0 MB | **8.97 B rows/sec** |
| **Zero-Copy FFI Stream (20 GB Logical)** | 1,000,000,000 | 85.71 ms | 16.0 MB | **11.67 B rows/sec** |
| **Chunked Table Concat + Slice + $O(\log C)$ Lookup** | 1,000,000,000 | 5.39 ms | 32.0 MB | **185.37 B rows/sec** |

---

## Building & Running Tests

### Requirements
* **JDK 25+**
* **Apache Maven 3.9+**

### Commands

```bash
# Run all unit tests and benchmarks
mvn clean test

# Run only the 1M-row and 1B-row benchmark suites
mvn test -Dtest=JDataCoreBenchmarkTest,OneBillionRowsBenchmarkTest

# Build JARs, source JARs, and verify Javadoc compliance
mvn clean package javadoc:javadoc
```

Detailed module documentation for `jdata-core` can be found in [`jdata-core/README.md`](./jdata-core/README.md).

---

## Author & License

* **Author:** [Shridhar Pandey](https://shridey.com) ([Shridey](https://github.com/shridey))
* **License:** Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).