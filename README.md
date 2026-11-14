# jdata

[![Maven Central](https://img.shields.io/maven-central/v/io.github.shridey/jdata-core.svg?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.shridey/jdata-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Java](https://img.shields.io/badge/Java-25%2B-orange.svg)](https://openjdk.org/projects/jdk/25/)

**`jdata`** is a next-generation, zero-dependency columnar data engineering and high-performance numerical computing ecosystem for **Java 25+**, built natively on the **Foreign Function & Memory (FFM) API (`java.lang.foreign`)** and the **Apache Arrow Columnar Specification**.

---

## Ecosystem Modules

| Module | Artifact ID | Status | Description |
| :--- | :--- | :---: | :--- |
| **[`jdata-core`](./jdata-core)** | `jdata-core` | **v0.3.1 (Live)** | Off-heap Apache Arrow memory engine, $O(1)$ slab pooling (`ArrowMemoryPool`), physical & chunked buffers, off-heap dictionary encoding, 64-bit bitmap logic, and zero-copy C Data / C Stream FFI bridge. |
| **`jdata-io`** | `jdata-io` | *In Development* | Zero-copy memory-mapped Apache Arrow IPC / Feather v2, high-speed SIMD CSV, and streaming readers/writers. |
| **`jdata-num`** | `jdata-num` | *Planned* | 512-bit SIMD (`jdk.incubator.vector`) mathematical, statistical, comparison, reduction, and group-by compute kernels. |
| **`jdata-df`** | `jdata-df` | *Planned* | Expressive, lazy-evaluated DataFrame and relational query planner built on `jdata-core`, `jdata-io`, and `jdata-num`. |

---

## Installation

### Maven (Direct Dependency on `jdata-core`)

```xml
<dependency>
    <groupId>io.github.shridey</groupId>
    <artifactId>jdata-core</artifactId>
    <version>0.3.1</version>
</dependency>
```

### Maven (Using the `jdata` BOM for Multi-Module Version Alignment)

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.github.shridey</groupId>
            <artifactId>jdata</artifactId>
            <version>0.3.1</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.github.shridey</groupId>
        <artifactId>jdata-core</artifactId>
    </dependency>
</dependencies>
```

### Gradle (Kotlin DSL)

```kotlin
implementation("io.github.shridey:jdata-core:0.3.1")
```

---

## Benchmark Highlights (`jdata-core v0.3.1` on Java 25)

### 1-Million-Row In-Memory Throughput
| Operation | Rows | Avg Time | Throughput |
| :--- | ---: | ---: | ---: |
| **PooledSlab Allocation & Checkout (`PooledArena`)** | 8,192,000 | 4.83 ms | **1,695.5 M rows/sec** |
| **Zero-Copy FFI `ArrowArrayStream` (10 Batches)** | 1,000,000 | 2.00 ms | **498.8 M rows/sec** |
| **64-Bit `Bitmaps.and()` + `POPCNT` `Table.filter()`** | 1,000,000 | 5.55 ms | **180.3 M rows/sec** |
| **`UTF8Buffer` Zero-Alloc SIMD `equalsBytes()` Scan** | 1,000,000 | 7.47 ms | **133.9 M rows/sec** |
| **`Int64BufferBuilder` (In-Place Slab Append)** | 1,000,000 | 9.74 ms | **102.7 M rows/sec** |

### 1-Billion-Row Out-of-Core & Chunked Scale (On a 16 GB RAM Machine)
| Operation | Rows | Time | Peak Pool RAM | Throughput |
| :--- | ---: | ---: | ---: | ---: |
| **1B-Row Chunked Table Concat + Slice + Lookup** | 1,000,000,000 | 4.03 ms | 32.0 MB | **247.91 B rows/sec** |
| **1B-Row Chunked `UTF8`/`Dict` `take()` + SIMD Scan** | 1,000,000,000 | 43.44 ms | 32.0 MB | **23.02 B rows/sec** |
| **1B-Row Zero-Copy FFI Stream (20 GB Logical)** | 1,000,000,000 | 98.48 ms | 16.0 MB | **10.15 B rows/sec** |
| **1B-Row 64-Bit Word `Bitmaps.and()` Intersection** | 1,000,000,000 | 113.74 ms | 16.0 MB | **8.79 B rows/sec** |
| **1B-Row Pooled `Int64` Allocation (8 GB Logical)** | 1,000,000,000 | 250.63 ms | 16.0 MB | **3.99 B rows/sec** |

See the [**`jdata-core` Documentation**](./jdata-core/README.md) for full API usage and examples.

---

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).