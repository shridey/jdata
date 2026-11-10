package io.github.shridey.jdata.core.layout;

import io.github.shridey.jdata.core.DataType;
import java.util.Objects;

/**
 * Represents the structural blueprint of a single column.
 * Maps directly to a single entry in an ArrowSchema's children array.
 */
public record Field(String name, DataType type, boolean nullable) {

    public Field {
        Objects.requireNonNull(name, "Field name cannot be null");
        Objects.requireNonNull(type, "Field type cannot be null");
    }

    /**
     * Helper constructor defaulting to a nullable column (standard for analytical data).
     */
    public Field(String name, DataType type) {
        this(name, type, true);
    }
}