package io.github.shridey.jdata.core.internal;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

/**
 * Native C struct {@link StructLayout} definitions and field byte offsets for the
 * Apache Arrow C Data Interface ({@code ArrowSchema}, {@code ArrowArray}) and
 * Apache Arrow C Stream Interface ({@code ArrowArrayStream}).
 */
public final class ArrowCStructs {

    private ArrowCStructs() {}

    /** Layout of the 72-byte C {@code ArrowSchema} struct on 64-bit platforms. */
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

    /** Byte offset of {@code ArrowSchema.format}. */
    public static final long SCHEMA_OFFSET_FORMAT = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("format"));
    /** Byte offset of {@code ArrowSchema.name}. */
    public static final long SCHEMA_OFFSET_NAME = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("name"));
    /** Byte offset of {@code ArrowSchema.metadata}. */
    public static final long SCHEMA_OFFSET_METADATA = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("metadata"));
    /** Byte offset of {@code ArrowSchema.flags}. */
    public static final long SCHEMA_OFFSET_FLAGS = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("flags"));
    /** Byte offset of {@code ArrowSchema.n_children}. */
    public static final long SCHEMA_OFFSET_N_CHILDREN = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("n_children"));
    /** Byte offset of {@code ArrowSchema.children}. */
    public static final long SCHEMA_OFFSET_CHILDREN = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("children"));
    /** Byte offset of {@code ArrowSchema.dictionary}. */
    public static final long SCHEMA_OFFSET_DICTIONARY = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("dictionary"));
    /** Byte offset of {@code ArrowSchema.release}. */
    public static final long SCHEMA_OFFSET_RELEASE = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("release"));
    /** Byte offset of {@code ArrowSchema.private_data}. */
    public static final long SCHEMA_OFFSET_PRIVATE_DATA = ARROW_SCHEMA_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("private_data"));

    /** Layout of the 80-byte C {@code ArrowArray} struct on 64-bit platforms. */
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

    /** Byte offset of {@code ArrowArray.length}. */
    public static final long ARRAY_OFFSET_LENGTH = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("length"));
    /** Byte offset of {@code ArrowArray.null_count}. */
    public static final long ARRAY_OFFSET_NULL_COUNT = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("null_count"));
    /** Byte offset of {@code ArrowArray.offset}. */
    public static final long ARRAY_OFFSET_OFFSET = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("offset"));
    /** Byte offset of {@code ArrowArray.n_buffers}. */
    public static final long ARRAY_OFFSET_N_BUFFERS = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("n_buffers"));
    /** Byte offset of {@code ArrowArray.n_children}. */
    public static final long ARRAY_OFFSET_N_CHILDREN = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("n_children"));
    /** Byte offset of {@code ArrowArray.buffers}. */
    public static final long ARRAY_OFFSET_BUFFERS = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("buffers"));
    /** Byte offset of {@code ArrowArray.children}. */
    public static final long ARRAY_OFFSET_CHILDREN = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("children"));
    /** Byte offset of {@code ArrowArray.dictionary}. */
    public static final long ARRAY_OFFSET_DICTIONARY = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("dictionary"));
    /** Byte offset of {@code ArrowArray.release}. */
    public static final long ARRAY_OFFSET_RELEASE = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("release"));
    /** Byte offset of {@code ArrowArray.private_data}. */
    public static final long ARRAY_OFFSET_PRIVATE_DATA = ARROW_ARRAY_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("private_data"));

    /** Layout of the 40-byte C {@code ArrowArrayStream} struct on 64-bit platforms. */
    public static final StructLayout ARROW_ARRAY_STREAM_LAYOUT = MemoryLayout.structLayout(
            ValueLayout.ADDRESS.withName("get_schema"),
            ValueLayout.ADDRESS.withName("get_next"),
            ValueLayout.ADDRESS.withName("get_last_error"),
            ValueLayout.ADDRESS.withName("release"),
            ValueLayout.ADDRESS.withName("private_data")
    );

    /** Byte offset of {@code ArrowArrayStream.get_schema}. */
    public static final long STREAM_OFFSET_GET_SCHEMA = ARROW_ARRAY_STREAM_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("get_schema"));
    /** Byte offset of {@code ArrowArrayStream.get_next}. */
    public static final long STREAM_OFFSET_GET_NEXT = ARROW_ARRAY_STREAM_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("get_next"));
    /** Byte offset of {@code ArrowArrayStream.get_last_error}. */
    public static final long STREAM_OFFSET_GET_LAST_ERROR = ARROW_ARRAY_STREAM_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("get_last_error"));
    /** Byte offset of {@code ArrowArrayStream.release}. */
    public static final long STREAM_OFFSET_RELEASE = ARROW_ARRAY_STREAM_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("release"));
    /** Byte offset of {@code ArrowArrayStream.private_data}. */
    public static final long STREAM_OFFSET_PRIVATE_DATA = ARROW_ARRAY_STREAM_LAYOUT.byteOffset(MemoryLayout.PathElement.groupElement("private_data"));
}