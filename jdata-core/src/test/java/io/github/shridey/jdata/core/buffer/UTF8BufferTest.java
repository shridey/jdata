package io.github.shridey.jdata.core.buffer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class UTF8BufferTest {

    @Test
    void testMultilingualUnicodeAndEmojiEncoding() {
        try (Arena arena = Arena.ofConfined()) {
            String[] samples = {
                    "ASCII_123",
                    "",
                    null,
                    "Café & Привет & αβγ",
                    "नमस्ते दुनिया | 日本語 | €",
                    "🚀🔥🎉Jw𝄞"
            };

            UTF8Buffer allocated = UTF8Buffer.allocate(arena, "i18n", samples);
            assertEquals(samples.length, allocated.length());
            assertEquals(1L, allocated.nullCount());
            assertArrayEquals(samples, allocated.toArray());

            // Exercise the 3-argument UTF8BufferBuilder(Arena, String, long) constructor
            UTF8BufferBuilder builder = new UTF8BufferBuilder(arena, "i18n_built", 2);
            for (String s : samples) {
                builder.append(s);
            }
            builder.appendAll(samples);

            UTF8Buffer built = builder.build();
            assertEquals(samples.length * 2L, built.length());
            assertEquals(2L, built.nullCount());
            for (int i = 0; i < samples.length; i++) {
                assertEquals(samples[i], built.getString(i));
                assertEquals(samples[i], built.getString(samples.length + i));
            }
        }
    }

    @Test
    void testMalformedUnpairedSurrogatesDoNotCrashOrCorruptOffsets() {
        try (Arena arena = Arena.ofConfined()) {
            String[] malformed = {
                    "trailHigh\uD83D",
                    "midHigh\uD83DASCII",
                    "\uD800\uD83D\uDE80",
                    "loneLow\uDE00end",
                    "clean_after_malformed"
            };

            UTF8Buffer allocated = UTF8Buffer.allocate(arena, "malformed", malformed);
            UTF8Buffer built = new UTF8BufferBuilder(arena, "malformed_builder", 2, 16)
                    .appendAll(malformed)
                    .build();

            for (int i = 0; i < malformed.length; i++) {
                String expected = new String(malformed[i].getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                assertEquals(expected, allocated.getString(i), "Mismatch in allocate() at index " + i);
                assertEquals(expected, built.getString(i), "Mismatch in builder at index " + i);
            }
        }
    }

    @Test
    void testSlicingAndBlockAppendsWithOffsetRebasing() {
        try (Arena arena = Arena.ofConfined()) {
            String[] words = {"Zero", "One", null, "Three_🚀", "Four", "Five"};
            UTF8Buffer base = UTF8Buffer.allocate(arena, "words", words);

            UTF8Buffer slice1 = base.slice(1, 5);
            UTF8Buffer slice2 = slice1.slice(1, 3);

            assertEquals(3L, slice2.length());
            assertEquals(2L, slice2.offset());
            assertEquals(1L, slice2.nullCount());
            assertArrayEquals(new String[]{null, "Three_🚀", "Four"}, slice2.toArray());

            UTF8BufferBuilder builder = new UTF8BufferBuilder(arena, "combined", 2, 8);
            builder.append("Header")
                    .appendBlock(slice2, 0, slice2.length())
                    .append("Footer");

            UTF8Buffer result = builder.build();
            assertEquals(5L, result.length());
            assertEquals(1L, result.nullCount());
            assertArrayEquals(new String[]{"Header", null, "Three_🚀", "Four", "Footer"}, result.toArray());

            UTF8Buffer nonNull = UTF8Buffer.allocate(arena, "clean", new String[]{"Alpha", "Beta", "Gamma"});
            UTF8Buffer fastCopy = nonNull.toBuilder(arena).build();
            assertEquals(0L, fastCopy.nullCount());
            assertArrayEquals(new String[]{"Alpha", "Beta", "Gamma"}, fastCopy.toArray());
        }
    }

    @Test
    void testEmptyAndAllNullStringBuffers() {
        try (Arena arena = Arena.ofConfined()) {
            UTF8Buffer empty = UTF8Buffer.allocate(arena, "empty", new String[0]);
            assertEquals(0L, empty.length());
            assertEquals(0L, empty.nullCount());
            assertEquals(0, empty.toArray().length);

            UTF8Buffer allNulls = UTF8Buffer.allocate(arena, "nulls", new String[]{null, null, null});
            assertEquals(3L, allNulls.length());
            assertEquals(3L, allNulls.nullCount());
            assertNull(allNulls.getString(0));
            assertNull(allNulls.getString(2));
        }
    }

    @Test
    void testUTF8BoundsAndCapacityValidation() {
        try (Arena arena = Arena.ofConfined()) {
            assertThrows(IllegalArgumentException.class,
                    () -> new UTF8BufferBuilder(arena, "bad_cap", 8, -1));
            assertThrows(IllegalArgumentException.class,
                    () -> new UTF8BufferBuilder(arena, "overflow_cap", 8, (long) Integer.MAX_VALUE + 1L));

            UTF8Buffer buf = UTF8Buffer.allocate(arena, "test", new String[]{"A", "B"});
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getString(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.getString(2));
            assertThrows(IndexOutOfBoundsException.class, () -> buf.slice(1, 2));

            UTF8BufferBuilder builder = new UTF8BufferBuilder(arena, "b", 4, 64);
            assertThrows(IndexOutOfBoundsException.class, () -> builder.appendBlock(buf, -1, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> builder.appendBlock(buf, 1, 2));
        }
    }
}