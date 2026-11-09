# jdata-core

[![Maven Central](https://img.shields.io/maven-central/v/io.github.shridey/jdata-core.svg)](https://central.sonatype.com/artifact/io.github.shridey/jdata-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java 22+](https://img.shields.io/badge/Java-22%2B-green.svg)](https://openjdk.org/projects/jdk/22/)

A modern, high-performance, and zero-dependency off-heap memory foundation for Java. Built entirely on the **Foreign Function & Memory (FFM) API** (Project Panama), `jdata-core` provides seamless interoperability with the **Apache Arrow C Data Interface** without the bloat of legacy buffers or third-party dependencies.

## Why jdata-core?

- **Zero Dependencies:** No Netty, no FlatBuffers, no SLF4J. Just pure, modern Java.
- **Zero-Copy Interoperability:** Implements the Apache Arrow C Data Interface natively. Instantly share memory with Rust, C++, Python, or DuckDB without copying a single byte.
- **Project Panama Native:** Bypasses `sun.misc.Unsafe` entirely. Uses `MemorySegment` and `Arena` for hardware-aligned, safe off-heap memory management.
- **SIMD & Valhalla Ready:** Architected specifically to feed off-heap memory directly into the Java Vector API for hardware-accelerated math, and perfectly positioned for Project Valhalla's upcoming value classes.

## Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.github.shridey</groupId>
    <artifactId>jdata-core</artifactId>
    <version>0.0.1</version>
</dependency>
```

**Important:** Because `jdata-core` utilizes the FFM API for native memory access, you must pass the following argument to your JVM at runtime (and during tests):

```shell
--enable-native-access=ALL-UNNAMED
```

## Quick Start

### 1. Allocating and Using Columns
Create heavily optimized off-heap columns with built-in null-bitmap management.

```java
import io.github.shridey.jdata.core.buffer.Float64Buffer;
import io.github.shridey.jdata.core.buffer.Float64Column;

public class CoreExample {
    static void main(String[] args) {
        // Allocate a column for 1,000 double-precision floats
        try (Float64Buffer col = Float64Buffer.allocate("prices", 1000)) {

            col.set(0, 199.99);
            col.setNull(1); // Modifies the native validity bitmap
            col.set(2, 250.50);

            System.out.println("Row 1 is null? " + col.isNull(1));
        } // Native memory is automatically freed here
    }
}
```


### 2. Zero-Copy FFI (Arrow C Data Interface)
Export your Java columns to a native library (like Rust or C++) or ingest pointers from foreign languages with zero overhead.

```java
import io.github.shridey.jdata.core.ffi.ArrowCExporter;
import io.github.shridey.jdata.core.ffi.ArrowCImporter;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

public class FFIExample {
    static void main(String[] args) {
        try (Arena arena = Arena.ofConfined();
            Float64Buffer col = Float64Buffer.allocate("data", 500)) {

            // 1. Allocate raw C-structs for ArrowArray and ArrowSchema
            MemorySegment arrayPtr = ArrowCExporter.allocateArray(arena);
            MemorySegment schemaPtr = ArrowCExporter.allocateSchema(arena);

            // 2. Export the Java column directly into the C-structs
            ArrowCExporter.exportColumn(col, arrayPtr, schemaPtr);

            // [Pass arrayPtr and schemaPtr to a native library via downcall...]
            
            // 3. Import a foreign Arrow column back into Java
            try (var importedCol = ArrowCImporter.importColumn(arrayPtr, schemaPtr)) {
                System.out.println("Imported column type: " + importedCol.type());
            }
        }
    }
}
```


## Architecture & Roadmap

`jdata-core` is Tier 1 of the broader `jdata` ecosystem:

- **[Complete] Tier 1: `jdata-core`** - The raw, C-ABI compliant memory foundation.
- **[Next] Tier 2: `jdata-num`** - A hardware-accelerated math engine utilizing the Java Vector API for SIMD computations directly on `jdata-core` memory segments.
- **[Planned] Tier 3: `jdata-df`** - A full-featured, columnar DataFrame API for data engineering and analytics.

## License

This project is licensed under the Apache License, Version 2.0 - see the LICENSE file for details.