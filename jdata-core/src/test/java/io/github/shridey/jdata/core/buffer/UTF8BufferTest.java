package io.github.shridey.jdata.core.buffer;

import org.junit.jupiter.api.Test;
import java.lang.foreign.Arena;
import static org.junit.jupiter.api.Assertions.*;

class UTF8BufferTest {

    @Test
    void testVariableLengthStringOffsets() {
        try (Arena arena = Arena.ofConfined()) {
            String[] input = {"Apple", null, "", "Banana"};
            UTF8Buffer col = UTF8Buffer.allocate(arena, "fruits", input);

            assertEquals(4, col.length());
            assertEquals(1, col.nullCount());

            // 1. Standard string extraction
            assertEquals("Apple", col.getString(0));

            // 2. Null extraction (should bypass offset math)
            assertTrue(col.isNull(1));
            assertNull(col.getString(1));

            // 3. Empty string extraction (start offset == end offset)
            assertFalse(col.isNull(2));
            assertEquals("", col.getString(2));

            // 4. Final string extraction
            assertEquals("Banana", col.getString(3));
        }
    }

    @Test
    void testBuilderSafelyCopiesBlocks() {
        try (Arena arena = Arena.ofConfined()) {
            // 1. Initialize a sealed source buffer with a long string
            UTF8Buffer source = UTF8Buffer.allocate(arena, "src", new String[]{"XYZ"});

            // 2. Use a Builder to construct the new data layout
            UTF8BufferBuilder builder = new UTF8BufferBuilder(arena, "dest", 10, 64);

            // 3. Chain the operations together to utilize the return values and silence IDE warnings
            UTF8Buffer finalBuffer = builder.append("A")             // Index 0
                    .appendBlock(source, 0, 1) // Index 1
                    .append("C")             // Index 2
                    .build();                // Seal it

            // 4. Assertions
            assertEquals(3, finalBuffer.length());
            assertEquals("A", finalBuffer.getString(0));
            assertEquals("XYZ", finalBuffer.getString(1));
            assertEquals("C", finalBuffer.getString(2), "Subsequent strings remain perfectly intact.");
        }
    }

    @Test
    void testToBuilderForUTF8() {
        try (Arena arena = Arena.ofConfined()) {
            UTF8Buffer original = UTF8Buffer.allocate(arena, "strings", new String[]{"A", null, "B"});

            UTF8BufferBuilder builder = original.toBuilder(arena);
            builder.append("C");
            UTF8Buffer rebuilt = builder.build();

            assertEquals(4, rebuilt.length());
            assertEquals("A", rebuilt.getString(0));
            assertTrue(rebuilt.isNull(1));
            assertEquals("B", rebuilt.getString(2));
            assertEquals("C", rebuilt.getString(3));
        }
    }

    @Test
    void testZeroCopySliceUTF8() {
        try (Arena arena = Arena.ofConfined()) {
            String[] words = {"Apple", "Banana", null, "Date", "Elderberry"};
            UTF8Buffer original = UTF8Buffer.allocate(arena, "words", words);

            // Slice out: ["Banana", null, "Date"]
            UTF8Buffer slice = original.slice(1, 3);

            assertEquals(3, slice.length());
            assertEquals(1, slice.offset());

            // Verify string extraction with offsets
            assertEquals("Banana", slice.getString(0));
            assertTrue(slice.isNull(1));
            assertNull(slice.getString(1));
            assertEquals("Date", slice.getString(2));

            assertArrayEquals(new String[]{"Banana", null, "Date"}, slice.toArray());

            // Prove the pipeline: Sliced Buffer -> Builder -> Clean Buffer
            UTF8Buffer rebuilt = slice.toBuilder(arena).build();
            assertEquals(3, rebuilt.length());
            assertEquals(0, rebuilt.offset()); // Rebuilt buffers should reset offset to 0
            assertEquals("Date", rebuilt.getString(2));
        }
    }
}