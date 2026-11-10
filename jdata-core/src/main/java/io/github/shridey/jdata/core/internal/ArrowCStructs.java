package io.github.shridey.jdata.core.internal;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

/**
 * Defines the exact shape of C/C++ struct memory inside Java.
 * A "struct" in C is just a contiguous block of bytes. Because Java 22+ (FFM) can talk
 * to C directly, we have to tell Java exactly how many bytes each piece of a C struct takes,
 * and in what order they appear, so Java can write to those bytes correctly.
 */
public final class ArrowCStructs {

    private ArrowCStructs() {}

    /**
     * The blueprint for C++'s 'struct ArrowArray'.
     * This tells foreign languages WHERE our data lives in memory.
     */
    public static final StructLayout ARROW_ARRAY_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("length"),       // 8 bytes: Total rows
            ValueLayout.JAVA_LONG.withName("null_count"),   // 8 bytes: Total nulls
            ValueLayout.JAVA_LONG.withName("offset"),       // 8 bytes: Slicing offset
            ValueLayout.JAVA_LONG.withName("n_buffers"),    // 8 bytes: How many memory blocks we have
            ValueLayout.JAVA_LONG.withName("n_children"),   // 8 bytes: Nested types
            ValueLayout.ADDRESS.withName("buffers"),        // 8 bytes: The actual memory pointer
            ValueLayout.ADDRESS.withName("children"),       // 8 bytes: Pointer to children
            ValueLayout.ADDRESS.withName("dictionary"),     // 8 bytes: Pointer to dictionary
            ValueLayout.ADDRESS.withName("release"),        // 8 bytes: Function pointer to free memory
            ValueLayout.ADDRESS.withName("private_data")    // 8 bytes: Internal state
    ).withName("ArrowArray");

    /**
     * The blueprint for C++'s 'struct ArrowSchema'.
     * This tells foreign languages WHAT our data is (e.g., "it's a 64-bit integer").
     */
    public static final StructLayout ARROW_SCHEMA_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.ADDRESS.withName("format"),       // Pointer to the "g" or "l" format string
            ValueLayout.ADDRESS.withName("name"),         // Pointer to the column name string
            ValueLayout.ADDRESS.withName("metadata"),     // Pointer to custom JSON metadata
            ValueLayout.JAVA_LONG.withName("flags"),      // Bit flags (e.g., allows nulls)
            ValueLayout.JAVA_LONG.withName("n_children"),
            ValueLayout.ADDRESS.withName("children"),
            ValueLayout.ADDRESS.withName("dictionary"),
            ValueLayout.ADDRESS.withName("release"),
            ValueLayout.ADDRESS.withName("private_data")
    ).withName("ArrowSchema");

    // We pre-calculate exactly how many bytes into the struct each field starts.
    // For example, 'null_count' starts 8 bytes into the struct (right after 'length').
    // Hardcoding these byte offsets makes writing to the struct lightning fast.
    public static final long ARRAY_OFFSET_LENGTH = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("length"));
    public static final long ARRAY_OFFSET_NULL_COUNT = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("null_count"));
    public static final long ARRAY_OFFSET_OFFSET = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("offset"));
    public static final long ARRAY_OFFSET_N_BUFFERS = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("n_buffers"));
    public static final long ARRAY_OFFSET_N_CHILDREN = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("n_children"));
    public static final long ARRAY_OFFSET_BUFFERS = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("buffers"));
    public static final long ARRAY_OFFSET_CHILDREN = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("children"));
    public static final long ARRAY_OFFSET_DICTIONARY = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("dictionary"));
    public static final long ARRAY_OFFSET_RELEASE = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("release"));

    // Pre-computed byte offsets for ArrowSchema fields
    public static final long SCHEMA_OFFSET_FORMAT = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("format"));
    public static final long SCHEMA_OFFSET_NAME = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("name"));
    public static final long SCHEMA_OFFSET_FLAGS = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("flags"));
    public static final long SCHEMA_OFFSET_N_CHILDREN = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("n_children"));
    public static final long SCHEMA_OFFSET_CHILDREN = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("children"));
    public static final long SCHEMA_OFFSET_DICTIONARY = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("dictionary"));
    public static final long SCHEMA_OFFSET_RELEASE = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("release"));
}