package io.github.shridey.jdata.core.layout;

import io.github.shridey.jdata.core.DataBuffer;

import java.util.List;
import java.util.Objects;

/**
 * A sealed, columnar dataset ready for execution.
 * Aligns a Schema with a list of physically populated DataBuffers.
 */
public final class Table implements AutoCloseable {

    private final Schema schema;
    private final List<DataBuffer> columns;
    private final long rowCount;

    public Table(Schema schema, List<DataBuffer> columns) {
        this.schema = Objects.requireNonNull(schema, "Schema cannot be null");
        this.columns = List.copyOf(Objects.requireNonNull(columns, "Columns cannot be null"));

        if (schema.columnCount() != columns.size()) {
            throw new IllegalArgumentException(String.format(
                    "Schema size (%d) does not match column count (%d)",
                    schema.columnCount(), columns.size()
            ));
        }

        if (this.columns.isEmpty()) {
            this.rowCount = 0;
        } else {
            // Establish the required row count from the first column
            this.rowCount = this.columns.getFirst().length();

            // STRICT ALIGNMENT: An analytical table is invalid if columns have different lengths
            for (int i = 0; i < this.columns.size(); i++) {
                DataBuffer col = this.columns.get(i);
                Field field = schema.getField(i);

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
        if (offset < 0 || length < 0 || offset + length > this.rowCount) {
            throw new IndexOutOfBoundsException("Invalid slice parameters for Table");
        }

        // Delegate the zero-copy slicing down to every individual off-heap buffer
        List<DataBuffer> slicedColumns = columns.stream()
                .map(col -> col.slice(offset, length))
                .toList();

        return new Table(this.schema, slicedColumns);
    }

    /**
     * Safely releases all underlying FFM off-heap memory.
     */
    @Override
    public void close() throws Exception {
        for (DataBuffer col : columns) {
            if (col instanceof AutoCloseable closeable) {
                closeable.close();
            }
        }
    }
}