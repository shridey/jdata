package io.github.shridey.jdata.core;

/**
 * Defines logical and physical Apache Arrow data types and maps them to their
 * canonical Apache Arrow C Data Interface format strings and element byte widths.
 */
public enum DataType {

    // --- Null & Boolean ---
    /** Null type containing no physical storage buffers ({@code "n"}). */
    NULL("n", 0),

    /** Bit-packed boolean type storing 8 values per byte in LSB-0 order ({@code "b"}). */
    BOOLEAN("b", 1),

    // --- Signed Integers ---
    /** 8-bit signed two's-complement integer ({@code "c"}, 1 byte). */
    INT8("c", 1),
    /** 16-bit signed two's-complement integer ({@code "s"}, 2 bytes). */
    INT16("s", 2),
    /** 32-bit signed two's-complement integer ({@code "i"}, 4 bytes). */
    INT32("i", 4),
    /** 64-bit signed two's-complement integer ({@code "l"}, 8 bytes). */
    INT64("l", 8),

    // --- Unsigned Integers ---
    /** 8-bit unsigned integer ({@code "C"}, 1 byte). */
    UINT8("C", 1),
    /** 16-bit unsigned integer ({@code "S"}, 2 bytes). */
    UINT16("S", 2),
    /** 32-bit unsigned integer ({@code "I"}, 4 bytes). */
    UINT32("I", 4),
    /** 64-bit unsigned integer ({@code "L"}, 8 bytes). */
    UINT64("L", 8),

    // --- Floating Point ---
    /** 16-bit IEEE 754 half-precision floating-point number ({@code "e"}, 2 bytes). */
    FLOAT16("e", 2),
    /** 32-bit IEEE 754 single-precision floating-point number ({@code "f"}, 4 bytes). */
    FLOAT32("f", 4),
    /** 64-bit IEEE 754 double-precision floating-point number ({@code "g"}, 8 bytes). */
    FLOAT64("g", 8),

    // --- Variable-Length Binary and Strings ---
    /** Variable-length UTF-8 encoded string with 32-bit signed byte offsets ({@code "u"}). */
    UTF8("u", -1),
    /** Variable-length UTF-8 encoded string with 64-bit signed byte offsets ({@code "U"}). */
    LARGE_UTF8("U", -1),
    /** Variable-length raw binary payload with 32-bit signed byte offsets ({@code "z"}). */
    BINARY("z", -1),
    /** Variable-length raw binary payload with 64-bit signed byte offsets ({@code "Z"}). */
    LARGE_BINARY("Z", -1),

    // --- Date & Time ---
    /** 32-bit date representing days elapsed since the UNIX epoch ({@code "tdD"}, 4 bytes). */
    DATE32("tdD", 4),
    /** 64-bit date representing milliseconds elapsed since the UNIX epoch ({@code "tdm"}, 8 bytes). */
    DATE64("tdm", 8),
    /** 32-bit time of day in milliseconds since midnight ({@code "ttm"}, 4 bytes). */
    TIME32_MS("ttm", 4),
    /** 64-bit time of day in microseconds since midnight ({@code "ttu"}, 8 bytes). */
    TIME64_US("ttu", 8),
    /** 64-bit timestamp in milliseconds since the UNIX epoch with optional timezone ({@code "tsm:"}). */
    TIMESTAMP_MS("tsm:", 8),
    /** 64-bit timestamp in microseconds since the UNIX epoch with optional timezone ({@code "tsu:"}). */
    TIMESTAMP_US("tsu:", 8),

    // --- Complex Types ---
    /** Variable-size list type with 32-bit offsets ({@code "+l"}). */
    LIST("+l", -1),
    /** Variable-size list type with 64-bit offsets ({@code "+L"}). */
    LARGE_LIST("+L", -1),
    /** Composite struct / record-batch type containing ordered child fields ({@code "+s"}). */
    STRUCT("+s", -1),

    /**
     * Dictionary-encoded categorical column backed by 32-bit signed integer indices
     * referencing a deduplicated child dictionary buffer.
     */
    DICTIONARY("DICTIONARY", -1);

    private final String cDataFormat;
    private final int byteSize;

    DataType(String cDataFormat, int byteSize) {
        this.cDataFormat = cDataFormat;
        this.byteSize = byteSize;
    }

    /**
     * Returns the canonical Apache Arrow C Data Interface format string for this type.
     *
     * @return the Arrow C Data format string
     */
    public String getFormat() {
        return cDataFormat;
    }

    /**
     * Returns the physical byte width of a single element for fixed-width types,
     * {@code 1} for bit-packed {@link #BOOLEAN}, {@code 0} for {@link #NULL},
     * or {@code -1} for variable-width and composite types.
     *
     * @return the element byte size, or {@code -1} if variable-length
     */
    public int byteSize() {
        return byteSize;
    }

    /**
     * Resolves an Apache Arrow C Data Interface format string (e.g., {@code "i"}, {@code "g"},
     * {@code "u"}, or parameterized {@code "tsm:UTC"}) into its corresponding {@link DataType}.
     *
     * @param format the Arrow C Data format string
     * @return the matching {@link DataType}
     * @throws IllegalArgumentException if {@code format} is null, empty, or unsupported
     */
    public static DataType fromFormat(String format) {
        if (format == null || format.isEmpty()) {
            throw new IllegalArgumentException("Format string cannot be null or empty.");
        }
        for (DataType type : values()) {
            if (type == DICTIONARY) {
                continue;
            }
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