package io.github.shridey.jdata.core.layout;

import java.util.List;
import java.util.Objects;

/**
 * An immutable blueprint describing the layout of a Table or RecordBatch.
 */
public record Schema(List<Field> fields) {

    public Schema {
        // Defensive copy ensures the schema remains strictly immutable
        fields = List.copyOf(Objects.requireNonNull(fields, "Fields cannot be null"));
    }

    public int columnCount() {
        return fields.size();
    }

    public Field getField(int index) {
        return fields.get(index);
    }

    public int getFieldIndex(String name) {
        Objects.requireNonNull(name, "Column name cannot be null");
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    public Field getField(String name) {
        int index = getFieldIndex(name);
        if (index == -1) {
            throw new IllegalArgumentException("Column not found in schema: " + name);
        }
        return fields.get(index);
    }
}