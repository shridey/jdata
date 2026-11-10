# jdata-core

[![Maven Central](https://img.shields.io/maven-central/v/io.github.shridey/jdata-core.svg)](https://central.sonatype.com/artifact/io.github.shridey/jdata-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java 25](https://img.shields.io/badge/Java-25-green.svg)](https://openjdk.org/projects/jdk/25/)

A modern, high-performance, zero-dependency off-heap memory foundation for Java. Built strictly on the **Foreign Function & Memory (FFM) API**, `jdata-core` is the foundational tier of the `jdata` ecosystem. It provides native interoperability with the **Apache Arrow C Data Interface** without the bloat of legacy buffers, third-party dependencies, or Garbage Collection overhead.

## Why jdata-core?

- **Zero Dependencies:** No Netty, no FlatBuffers, no SLF4J. Pure, modern Java 25.
- **Strict Ingestion vs. Execution Lifecycle:** Dynamically growing `BufferBuilder` classes handle messy data ingestion, while fixed-size, sealed `DataBuffer` classes lock down memory for vectorized processing.
- **O(1) Zero-Copy Partitioning:** Split multi-million row datasets for multi-threaded workloads instantly. Slices apply logical offsets over shared physical memory pointers without copying a single byte.
- **Zero-Copy Interoperability:** Implements the Arrow FFI natively. Pass heavy datasets directly to Rust, C++, Python (Polars/Pandas), or DuckDB instantly.
- **Project Panama Native:** Bypasses `sun.misc.Unsafe` entirely. Utilizes hardware-aligned `MemorySegment` architectures perfectly positioned for the Vector API and SIMD computations.

## Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.shridey</groupId>
    <artifactId>jdata-core</artifactId>
    <version>0.2.x</version>
</dependency>
```

**Important:** You must enable native memory access in your JVM at runtime and during testing:

```shell
--enable-native-access=ALL-UNNAMED
```

## Real-World Quick Start

### 1. Ingestion: Streaming Telemetry (The Builder Pattern)
When reading streaming data of unknown sizes—like a WebSocket feed of financial tick data or IoT sensor telemetry—use a `BufferBuilder`. It dynamically doubles its off-heap memory capacity automatically.

```java
import io.github.shridey.jdata.core.buffer.Float64BufferBuilder;
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import java.lang.foreign.Arena;

public class MarketDataIngestion {
    static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            
            // Initializing a builder for a high-frequency trading feed
            Float64BufferBuilder tradePrices = new Float64BufferBuilder(arena, "execution_price", 10_000);
            
            // Ingesting ticks (e.g., from an exchange parser)
            tradePrices.append(452.12)
                       .append(452.14)
                       .appendNull() // Packet drop / missing tick: safely managed in the validty bitmap
                       .append(452.11);

            // Seal the data into an immutable, hardware-aligned buffer for the query engine
            Float64Buffer executionPrices = tradePrices.build();
            
            System.out.println("Total ticks processed: " + executionPrices.length());
        }
    }
}
```

### 2. Execution: Parallel Processing (Zero-Copy Slicing)
Sealed buffers allow you to instantly partition massive datasets for concurrent thread pools. Slicing does not duplicate data; it creates lightweight views utilizing absolute physical offsets.

```java
import io.github.shridey.jdata.core.buffer.Int32Buffer;
import java.lang.foreign.Arena;

public class ParallelAdTechWorkload {
    static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            // Assume we loaded 10 million ad-click User IDs from a Parquet file
            Int32Buffer userIds = Int32Buffer.allocate(arena, "click_user_id", 10_000_000);
            
            // Distribute workloads across threads without copying memory or causing GC spikes
            Int32Buffer threadOneWorkload = userIds.slice(0, 2_500_000);
            Int32Buffer threadTwoWorkload = userIds.slice(2_500_000, 2_500_000);
            
            // Both slices share the exact same MemorySegment.
            // Thread 2's .get(0) automatically reads the physical memory at index 2,500,000.
            System.out.println("Thread 2 Logical Offset: " + threadTwoWorkload.offset());
        }
    }
}
```

### 3. FFI: Machine Learning Handoff (Arrow C Data Interface)
Export your Java columns to a native library without expensive serialization. For example, passing a sliced batch of image processing tensors directly to a Rust or C++ machine learning inference engine.

```java
import io.github.shridey.jdata.core.ffi.ArrowCExporter;
import io.github.shridey.jdata.core.ffi.ArrowCImporter;
import io.github.shridey.jdata.core.ffi.ImportedDataBuffer;
import io.github.shridey.jdata.core.buffer.Float32Buffer;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class NativeMLHandoff {
    static void main(String[] args) {
        try (Arena arena = Arena.ofConfined()) {
            Float32Buffer tensorBatch = Float32Buffer.allocate(arena, "normalized_pixels", 50_000);

            // 1. EXPORT: Bind the Java off-heap memory to standard C-Structs
            // The FFI exporter natively understands and preserves zero-copy slice offsets.
            MemorySegment arrayPtr = ArrowCExporter.exportArray(tensorBatch, arena);
            MemorySegment schemaPtr = ArrowCExporter.exportSchema(tensorBatch, arena);

            // [Pass arrayPtr and schemaPtr to your Rust/C++ inference backend via downcall...]
            
            // 2. IMPORT: Receive classification results back from the foreign language
            try (ImportedDataBuffer foreignResults = ArrowCImporter.importBuffer(arrayPtr, schemaPtr)) {
                System.out.println("Foreign engine returned: " + foreignResults.type());
            } // Foreign C++ memory is safely and natively released upon closing the block
        }
    }
}
```

## Architecture & Roadmap

`jdata-core` is Tier 1 of the broader `jdata` ecosystem:

- **(Complete) Tier 1: `jdata-core`** The raw, C-ABI compliant memory foundation and zero-copy FFI bridge.
- **(Next) Tier 2: `jdata-num`** A hardware-accelerated math engine utilizing the Java Vector API for SIMD computations directly on `jdata-core` memory segments.
- **(Planned) Tier 3: `jdata-df`** A full-featured, columnar DataFrame API for analytics, aggregations, and data engineering.

## License

This project is licensed under the Apache License, Version 2.0 - see the LICENSE file for details.