package io.github.shridey.jdata.core.ffi;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.internal.ArrowCStructs;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

/**
 * Bridges Java's off-heap memory to the outside world.
 * This utility takes an {@link DataBuffer} and populates standard C-structs,
 * allowing native languages (Python, Rust, C++) to read jdata's memory without copying bytes.
 *
 * @version 1.0
 * @since 1.0
 */
public final class ArrowCExporter {

    // Prevent instantiation of utility class
    private ArrowCExporter() {}

    /**
     * Exports an ArrowColumn's data buffers into an ArrowArray C-struct.
     *
     * @param column The Java column to export.
     * @param structArena The arena that will manage the lifecycle of the C-struct itself.
     * @return The memory address of the populated ArrowArray struct.
     */
    public static MemorySegment exportArray(DataBuffer column, Arena structArena) {
        // Allocate the C-struct in off-heap memory
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_ARRAY_LAYOUT);

        // Populate scalar metadata required by Arrow
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_LENGTH, column.length());
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_NULL_COUNT, column.nullCount());
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_OFFSET, 0L);
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_BUFFERS, column.bufferCount());
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.ARRAY_OFFSET_N_CHILDREN, 0L);

        // Allocate a C-array of pointers (void**) to hold our buffer addresses
        MemorySegment buffersPtrArray = structArena.allocate(ValueLayout.ADDRESS, column.bufferCount());
        for (int i = 0; i < column.bufferCount(); i++) {
            buffersPtrArray.setAtIndex(ValueLayout.ADDRESS, i, column.buffer(i));
        }

        // Link the pointer array into the main ArrowArray struct
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_BUFFERS, buffersPtrArray);

        // Null out unused fields (primitive columns do not have nested children or dictionaries)
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_CHILDREN, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_DICTIONARY, MemorySegment.NULL);

        // Bind the release callback (allowing the foreign language to signal it is done)
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, createArrayReleaseCallback(structArena));

        return struct;
    }

    /**
     * Exports the schema defining the data type of the column.
     *
     * @param column The Java column whose type we are describing.
     * @param structArena The arena that will manage the lifecycle of the C-struct.
     * @return The memory address of the populated ArrowSchema struct.
     */
    public static MemorySegment exportSchema(DataBuffer column, Arena structArena) {
        MemorySegment struct = structArena.allocate(ArrowCStructs.ARROW_SCHEMA_LAYOUT);

        // Fetch the exact C-Data format string defined in our ArrowType enum
        String formatString = column.type().getFormat();

        // Write the format string and column name into the off-heap struct
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_FORMAT, structArena.allocateFrom(formatString));
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_NAME, structArena.allocateFrom(column.name()));

        // Flag 2 indicates the column is "Nullable", standard for Arrow data
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_FLAGS, 2L);
        struct.set(ValueLayout.JAVA_LONG, ArrowCStructs.SCHEMA_OFFSET_N_CHILDREN, 0L);

        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_CHILDREN, MemorySegment.NULL);
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_DICTIONARY, MemorySegment.NULL);

        // Bind the schema-specific release callback
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE, createSchemaReleaseCallback(structArena));

        return struct;
    }

    /**
     * Creates a C function pointer (upcall stub) that triggers the Java Array release method.
     */
    private static MemorySegment createArrayReleaseCallback(Arena arena) {
        try {
            MethodHandle handle = MethodHandles.lookup().findStatic(
                    ArrowCExporter.class, "releaseArrayCallback",
                    java.lang.invoke.MethodType.methodType(void.class, MemorySegment.class)
            );
            return Linker.nativeLinker().upcallStub(handle, FunctionDescriptor.ofVoid(ValueLayout.ADDRESS), arena);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("Failed to bind Array release callback", e);
        }
    }

    /**
     * Creates a C function pointer (upcall stub) that triggers the Java Schema release method.
     */
    private static MemorySegment createSchemaReleaseCallback(Arena arena) {
        try {
            MethodHandle handle = MethodHandles.lookup().findStatic(
                    ArrowCExporter.class, "releaseSchemaCallback",
                    java.lang.invoke.MethodType.methodType(void.class, MemorySegment.class)
            );
            return Linker.nativeLinker().upcallStub(handle, FunctionDescriptor.ofVoid(ValueLayout.ADDRESS), arena);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("Failed to bind Schema release callback", e);
        }
    }

    /**
     * Invoked natively by C++/Rust/Python when they release the ArrowArray data.
     *
     * @param structPtr The raw, unbounded memory address of the struct ArrowArray.
     */
    public static void releaseArrayCallback(MemorySegment structPtr) {
        MemorySegment struct = structPtr.reinterpret(ArrowCStructs.ARROW_ARRAY_LAYOUT.byteSize());
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.ARRAY_OFFSET_RELEASE, MemorySegment.NULL);
    }

    /**
     * Invoked natively by C++/Rust/Python when they release the ArrowSchema data.
     *
     * @param structPtr The raw, unbounded memory address of the struct ArrowSchema.
     */
    public static void releaseSchemaCallback(MemorySegment structPtr) {
        MemorySegment struct = structPtr.reinterpret(ArrowCStructs.ARROW_SCHEMA_LAYOUT.byteSize());
        struct.set(ValueLayout.ADDRESS, ArrowCStructs.SCHEMA_OFFSET_RELEASE, MemorySegment.NULL);
    }
}