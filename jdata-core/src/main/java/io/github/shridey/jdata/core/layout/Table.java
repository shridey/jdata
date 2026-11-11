package io.github.shridey.jdata.core.layout;

import io.github.shridey.jdata.core.DataBuffer;

import java.util.List;
import java.util.Objects;

/**
 * A sealed, columnar dataset ready for execution.
 * Aligns a Schema with a list of physically populated DataBuffers.
 */
public final class Table {

    private final Schema schema;
    private final List<DataBuffer> columns;
    private final long rowCount;

    public Table(Schema schema, List<DataBuffer> columns) {
        this.schema = Objects.requireNonNull(schema, "Schema cannot be null");
        this.columns = List.copyOf(Objects.requireNonNull(columns, "Columns cannot be null"));

        if (schema.columnCount() != this.columns.size()) {
            throw new IllegalArgumentException(String.format(
                    "Schema size (%d) does not match column count (%d)",
                    schema.columnCount(), this.columns.size()
            ));
        }

        if (this.columns.isEmpty()) {
            this.rowCount = 0;
        } else {
            this.rowCount = this.columns.getFirst().length();

            for (int i = 0; i < this.columns.size(); i++) {
                DataBuffer col = this.columns.get(i);
                Field field = schema.getField(i);

                if (!col.name().equals(field.name())) {
                    throw new IllegalArgumentException(String.format(
                            "Column name mismatch at index %d. Schema expects '%s', Buffer is '%s'.",
                            i, field.name(), col.name()
                    ));
                }

                if (col.length() != this.rowCount) {
                    throw new IllegalArgumentException(String.format(
                            "Column alignment fault at index %d ('%s'). Expected %d rows, got %d.",
                            i, field.name(), this.rowCount, col.length()
                    ));
                }

                if (col.type() != field.type()) {
                    throw new IllegalArgumentException(String.format(
                            "Type mismatch at index %d ('%s'). Schema expects %s, Buffer is %s.",
                            i, field.name(), field.type(), col.type()
                    ));
                }

                if (!field.nullable() && col.nullCount() > 0) {
                    throw new IllegalArgumentException(String.format(
                            "Nullability violation at index %d ('%s'). Field is non-nullable but buffer contains %d nulls.",
                            i, field.name(), col.nullCount()
                    ));
                }
            }
        }
    }

    public Schema schema() { return schema; }
    public long rowCount() { return rowCount; }
    public int columnCount() { return columns.size(); }

    public DataBuffer getColumn(int index) {
        return columns.get(index);
    }

    public DataBuffer getColumn(String name) {
        int index = schema.getFieldIndex(name);
        if (index == -1) {
            throw new IllegalArgumentException("Column not found: " + name);
        }
        return columns.get(index);
    }

    /**
     * O(1) Zero-Copy Table Slicing.
     * Partitions the entire table instantly for multithreaded workloads.
     */
    public Table slice(long offset, long length) {
        Objects.checkFromIndexSize(offset, length, this.rowCount);

        List<DataBuffer> slicedColumns = columns.stream()
                .map(col -> col.slice(offset, length))
                .toList();

        return new Table(this.schema, slicedColumns);
    }
}