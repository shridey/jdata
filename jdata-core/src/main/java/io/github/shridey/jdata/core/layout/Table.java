package io.github.shridey.jdata.core.layout;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.buffer.ChunkedBuffer;

import java.lang.foreign.Arena;
import java.util.ArrayList;
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

    /**
     * O(1) Zero-Copy Table Concatenation.
     * Combines this Table with {@code other} using {@link ChunkedBuffer} columns without copying off-heap data.
     */
    public Table concat(Table other) {
        Objects.requireNonNull(other, "other Table cannot be null");
        return concat(List.of(this, other));
    }

    /**
     * O(1) Zero-Copy Concatenation of multiple Tables sharing an identical {@link Schema}.
     */
    public static Table concat(List<Table> tables) {
        Objects.requireNonNull(tables, "tables list cannot be null");
        if (tables.isEmpty()) {
            throw new IllegalArgumentException("Cannot concatenate an empty list of tables.");
        }
        if (tables.size() == 1) {
            return Objects.requireNonNull(tables.getFirst(), "Table cannot be null");
        }

        Table first = Objects.requireNonNull(tables.getFirst(), "First table cannot be null");
        Schema targetSchema = first.schema();

        for (int t = 1; t < tables.size(); t++) {
            Table candidate = Objects.requireNonNull(tables.get(t), "Table at index " + t + " cannot be null");
            if (!targetSchema.equals(candidate.schema())) {
                throw new IllegalArgumentException(
                        "Schema mismatch during Table.concat at table index " + t
                );
            }
        }

        int colCount = targetSchema.columnCount();
        List<DataBuffer> mergedColumns = new ArrayList<>(colCount);

        for (int c = 0; c < colCount; c++) {
            Field field = targetSchema.getField(c);
            DataBuffer firstCol = first.getColumn(c);
            List<DataBuffer> colChunks = new ArrayList<>(tables.size());
            for (Table table : tables) {
                colChunks.add(table.getColumn(c));
            }
            mergedColumns.add(new ChunkedBuffer(firstCol.arena(), field.name(), field.type(), colChunks));
        }

        return new Table(targetSchema, mergedColumns);
    }

    /**
     * Compacts any {@link ChunkedBuffer} columns in this Table into single contiguous buffers in {@code targetArena}.
     */
    public Table compact(Arena targetArena) {
        Objects.requireNonNull(targetArena, "targetArena cannot be null");
        List<DataBuffer> compactedColumns = new ArrayList<>(columns.size());
        for (DataBuffer col : columns) {
            if (col instanceof ChunkedBuffer cb) {
                compactedColumns.add(cb.compact(targetArena));
            } else {
                compactedColumns.add(col);
            }
        }
        return new Table(this.schema, compactedColumns);
    }
}