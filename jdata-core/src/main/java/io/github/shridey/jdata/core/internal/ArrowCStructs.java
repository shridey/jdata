package io.github.shridey.jdata.core.internal;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

/**
 * MemoryLayout definitions for the Apache Arrow C Data Interface and C Stream Interface.
 */
public final class ArrowCStructs {

    private ArrowCStructs() {}

    public static final StructLayout ARROW_SCHEMA_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.ADDRESS.withName("format"),
            ValueLayout.ADDRESS.withName("name"),
            ValueLayout.ADDRESS.withName("metadata"),
            ValueLayout.JAVA_LONG.withName("flags"),
            ValueLayout.JAVA_LONG.withName("n_children"),
            ValueLayout.ADDRESS.withName("children"),
            ValueLayout.ADDRESS.withName("dictionary"),
            ValueLayout.ADDRESS.withName("release"),
            ValueLayout.ADDRESS.withName("private_data")
    );

    public static final long SCHEMA_OFFSET_FORMAT = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("format"));
    public static final long SCHEMA_OFFSET_NAME = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("name"));
    public static final long SCHEMA_OFFSET_METADATA = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("metadata"));
    public static final long SCHEMA_OFFSET_FLAGS = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("flags"));
    public static final long SCHEMA_OFFSET_N_CHILDREN = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("n_children"));
    public static final long SCHEMA_OFFSET_CHILDREN = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("children"));
    public static final long SCHEMA_OFFSET_DICTIONARY = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("dictionary"));
    public static final long SCHEMA_OFFSET_RELEASE = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("release"));
    public static final long SCHEMA_OFFSET_PRIVATE_DATA = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("private_data"));

    public static final StructLayout ARROW_ARRAY_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.JAVA_LONG.withName("length"),
            ValueLayout.JAVA_LONG.withName("null_count"),
            ValueLayout.JAVA_LONG.withName("offset"),
            ValueLayout.JAVA_LONG.withName("n_buffers"),
            ValueLayout.JAVA_LONG.withName("n_children"),
            ValueLayout.ADDRESS.withName("buffers"),
            ValueLayout.ADDRESS.withName("children"),
            ValueLayout.ADDRESS.withName("dictionary"),
            ValueLayout.ADDRESS.withName("release"),
            ValueLayout.ADDRESS.withName("private_data")
    );

    public static final long ARRAY_OFFSET_LENGTH = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("length"));
    public static final long ARRAY_OFFSET_NULL_COUNT = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("null_count"));
    public static final long ARRAY_OFFSET_OFFSET = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("offset"));
    public static final long ARRAY_OFFSET_N_BUFFERS = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("n_buffers"));
    public static final long ARRAY_OFFSET_N_CHILDREN = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("n_children"));
    public static final long ARRAY_OFFSET_BUFFERS = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("buffers"));
    public static final long ARRAY_OFFSET_CHILDREN = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("children"));
    public static final long ARRAY_OFFSET_DICTIONARY = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("dictionary"));
    public static final long ARRAY_OFFSET_RELEASE = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("release"));
    public static final long ARRAY_OFFSET_PRIVATE_DATA = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("private_data"));

    public static final StructLayout ARROW_ARRAY_STREAM_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.ADDRESS.withName("get_schema"),
            ValueLayout.ADDRESS.withName("get_next"),
            ValueLayout.ADDRESS.withName("get_last_error"),
            ValueLayout.ADDRESS.withName("release"),
            ValueLayout.ADDRESS.withName("private_data")
    );

    public static final long STREAM_OFFSET_GET_SCHEMA = ARROW_ARRAY_STREAM_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("get_schema"));
    public static final long STREAM_OFFSET_GET_NEXT = ARROW_ARRAY_STREAM_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("get_next"));
    public static final long STREAM_OFFSET_GET_LAST_ERROR = ARROW_ARRAY_STREAM_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("get_last_error"));
    public static final long STREAM_OFFSET_RELEASE = ARROW_ARRAY_STREAM_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("release"));
    public static final long STREAM_OFFSET_PRIVATE_DATA = ARROW_ARRAY_STREAM_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("private_data"));
}