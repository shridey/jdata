package io.github.shridey.jdata.core.layout;

import io.github.shridey.jdata.core.DataType;
import java.util.Objects;

/**
 * Represents the immutable structural blueprint of a single column within a {@link Schema}[cite: 9, 10].
 * Maps directly to a child entry in an Apache Arrow {@code ArrowSchema} struct[cite: 9].
 *
 * @param name     the non-null column name[cite: 9]
 * @param type     the non-null logical {@link DataType} of the column[cite: 9]
 * @param nullable {@code true} if the column permits null values, {@code false} otherwise[cite: 9]
 */
public record Field(String name, DataType type, boolean nullable) {

    /**
     * Constructs a validated {@link Field}[cite: 9].
     *
     * @throws NullPointerException if {@code name} or {@code type} is {@code null}[cite: 9]
     */
    public Field {
        Objects.requireNonNull(name, "Field name cannot be null");
        Objects.requireNonNull(type, "Field type cannot be null");
    }

    /**
     * Constructs a nullable {@link Field} with the given column name and data type[cite: 9].
     *
     * @param name the non-null column name[cite: 9]
     * @param type the non-null logical {@link DataType}[cite: 9]
     */
    public Field(String name, DataType type) {
        this(name, type, true);
    }
}