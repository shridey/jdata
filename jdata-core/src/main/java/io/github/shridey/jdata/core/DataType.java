package io.github.shridey.jdata.core;

/**
 * Maps standard Java concepts to strict C++ memory layouts.
 * When two different languages (like Java and C++) share memory, they need a universal
 * translator. Apache Arrow uses simple 1-character strings (like "l" for a 64-bit integer)
 * to tell the other language exactly what the memory bytes represent.
 */
public enum DataType {

    // --- Null & Boolean ---
    /** Null type. Contains no physical data. */
    NULL("n", 0),

    /**
     * "b" tells C++ that the memory is bit-packed (8 booleans per byte).
     */
    BOOLEAN("b", 1),

    // --- Signed Integers ---
    /** 8-bit signed integer. */
    INT8("c", 1),
    /** 16-bit signed integer. */
    INT16("s", 2),
    /** "i" means standard 32-bit Integer (4 bytes). */
    INT32("i", 4),
    /** "l" means 64-bit Long (8 bytes). */
    INT64("l", 8),

    // --- Unsigned Integers ---
    UINT8("C", 1),
    UINT16("S", 2),
    UINT32("I", 4),
    UINT64("L", 8),

    // --- Floating Point ---
    FLOAT16("e", 2),
    /** "f" means 32-bit Float (4 bytes). */
    FLOAT32("f", 4),
    /** "g" means 64-bit Double (8 bytes). */
    FLOAT64("g", 8),

    // --- Variable-Length Binary and Strings ---
    /**
     * "u" means variable-length UTF-8 String.
     * The byte size is -1 because we don't know the size of a string until we read it.
     */
    UTF8("u", -1),
    LARGE_UTF8("U", -1),
    BINARY("z", -1),
    LARGE_BINARY("Z", -1),

    // --- Date & Time ---
    DATE32("tdD", 4),
    DATE64("tdm", 8),
    TIME32_MS("ttm", 4),
    TIME64_US("ttu", 8),
    TIMESTAMP_MS("tsm:", 8),
    TIMESTAMP_US("tsu:", 8),

    // --- Complex Types ---
    LIST("+l", -1),
    LARGE_LIST("+L", -1),
    STRUCT("+s", -1),

    /** Dictionary encoding. Relies on a nested schema and is handled dynamically. */
    DICTIONARY("DICTIONARY", -1);

    private final String cDataFormat;
    private final int byteSize;

    DataType(String cDataFormat, int byteSize) {
        this.cDataFormat = cDataFormat;
        this.byteSize = byteSize;
    }

    public String getFormat() {
        return cDataFormat;
    }

    public int byteSize() {
        return byteSize;
    }

    /**
     * If a C++ or Rust engine hands us memory, they will hand us a format string (e.g., "g").
     * This method translates that foreign string back into our Java Enum (FLOAT64).
     */
    public static DataType fromFormat(String format) {
        if (format == null || format.isEmpty()) {
            throw new IllegalArgumentException("Format string cannot be null or empty.");
        }
        for (DataType type : values()) {
            if (type == DICTIONARY) {
                continue;
            }
            // Only parameterized formats (such as "tsm:<timezone>") use prefix matching.
            boolean matches = type.cDataFormat.endsWith(":")
                    ? format.startsWith(type.cDataFormat)
                    : format.equals(type.cDataFormat);
            if (matches) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported Arrow C-Data format: " + format);
    }
}