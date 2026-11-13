package io.github.shridey.jdata.core.layout;

import io.github.shridey.jdata.core.DataBuffer;
import io.github.shridey.jdata.core.buffer.Bitmaps;
import io.github.shridey.jdata.core.buffer.BooleanBuffer;
import io.github.shridey.jdata.core.buffer.ChunkedBuffer;
import io.github.shridey.jdata.core.buffer.Int32Buffer;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * An immutable columnar dataset (equivalent to an Apache Arrow RecordBatch or Table)
 * that pairs a validated {@link Schema} with an aligned list of {@link DataBuffer} columns[cite: 11].
 */
public final class Table {

    private final Schema schema;
    private final List<DataBuffer> columns;
    private final long rowCount;

    /**
     * Constructs a validated {@link Table}, verifying that column counts, names, types,
     * row lengths, and nullability constraints strictly match the provided {@link Schema}[cite: 11].
     *
     * @param schema  the table schema[cite: 11]
     * @param columns the ordered list of column buffers[cite: 11]
     * @throws NullPointerException     if {@code schema}, {@code columns}, or any column is {@code null}[cite: 11]
     * @throws IllegalArgumentException if any column violates schema alignment, type, or nullability invariants[cite: 11]
     */
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

    /**
     * Returns the {@link Schema} of this table[cite: 11].
     *
     * @return the table schema[cite: 11]
     */
    public Schema schema() {
        return schema;
    }

    /**
     * Returns the number of rows in this table[cite: 11].
     *
     * @return the row count[cite: 11]
     */
    public long rowCount() {
        return rowCount;
    }

    /**
     * Returns the number of columns in this table[cite: 11].
     *
     * @return the column count[cite: 11]
     */
    public int columnCount() {
        return columns.size();
    }

    /**
     * Returns an unmodifiable list of the columns in this table.
     *
     * @return the immutable list of column buffers
     */
    public List<DataBuffer> columns() {
        return columns;
    }

    /**
     * Returns the column {@link DataBuffer} at the specified zero-based column index[cite: 11].
     *
     * @param index the zero-based column index[cite: 11]
     * @return the column buffer at {@code index}[cite: 11]
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= columnCount()}[cite: 11]
     */
    public DataBuffer getColumn(int index) {
        return columns.get(index);
    }

    /**
     * Returns the column {@link DataBuffer} with the specified column name[cite: 11].
     *
     * @param name the column name to look up[cite: 11]
     * @return the matching column buffer[cite: 11]
     * @throws IllegalArgumentException if no column with {@code name} exists[cite: 11]
     */
    public DataBuffer getColumn(String name) {
        int index = schema.getFieldIndex(name);
        if (index == -1) {
            throw new IllegalArgumentException("Column not found: " + name);
        }
        return columns.get(index);
    }

    /**
     * Creates an O(1) zero-copy horizontal slice of this table across all columns[cite: 11].
     *
     * @param offset the starting zero-based row offset[cite: 11]
     * @param length the number of rows to include in the slice[cite: 11]
     * @return a zero-copy sliced {@link Table}[cite: 11]
     * @throws IndexOutOfBoundsException if {@code offset} or {@code length} is out of bounds[cite: 11]
     */
    public Table slice(long offset, long length) {
        Objects.checkFromIndexSize(offset, length, this.rowCount);

        List<DataBuffer> slicedColumns = columns.stream()
                .map(col -> col.slice(offset, length))
                .toList();

        return new Table(this.schema, slicedColumns);
    }

    /**
     * Returns up to the first {@code n} rows of this table as an O(1) zero-copy slice[cite: 11].
     *
     * @param n the maximum number of leading rows to return[cite: 11]
     * @return a zero-copy sliced {@link Table}[cite: 11]
     * @throws IllegalArgumentException if {@code n < 0}[cite: 11]
     */
    public Table head(long n) {
        if (n < 0) throw new IllegalArgumentException("n cannot be negative: " + n);
        return slice(0, Math.min(n, this.rowCount));
    }

    /**
     * Returns up to the last {@code n} rows of this table as an O(1) zero-copy slice[cite: 11].
     *
     * @param n the maximum number of trailing rows to return[cite: 11]
     * @return a zero-copy sliced {@link Table}[cite: 11]
     * @throws IllegalArgumentException if {@code n < 0}[cite: 11]
     */
    public Table tail(long n) {
        if (n < 0) throw new IllegalArgumentException("n cannot be negative: " + n);
        long count = Math.min(n, this.rowCount);
        return slice(this.rowCount - count, count);
    }

    /**
     * Projects a subset of columns by name in O(1) zero-copy metadata time[cite: 11].
     *
     * @param columnNames the ordered names of the columns to retain[cite: 11]
     * @return a new {@link Table} containing only the projected columns[cite: 11]
     * @throws IllegalArgumentException if any column name is missing or duplicated[cite: 11]
     */
    public Table select(String... columnNames) {
        Objects.requireNonNull(columnNames, "columnNames cannot be null");
        List<Field> projectedFields = new ArrayList<>(columnNames.length);
        List<DataBuffer> projectedCols = new ArrayList<>(columnNames.length);
        for (String name : columnNames) {
            int idx = schema.getFieldIndex(name);
            if (idx == -1) {
                throw new IllegalArgumentException("Column not found: " + name);
            }
            projectedFields.add(schema.getField(idx));
            projectedCols.add(columns.get(idx));
        }
        return new Table(new Schema(projectedFields), projectedCols);
    }

    /**
     * Appends a new column to the right of this table in O(1) zero-copy time[cite: 11].
     *
     * @param field  the schema field metadata for the new column[cite: 11]
     * @param column the column buffer to append[cite: 11]
     * @return a new {@link Table} including the appended column[cite: 11]
     */
    public Table addColumn(Field field, DataBuffer column) {
        Objects.requireNonNull(field, "field cannot be null");
        Objects.requireNonNull(column, "column cannot be null");
        List<DataBuffer> updatedCols = new ArrayList<>(columns.size() + 1);
        updatedCols.addAll(columns);
        updatedCols.add(column);
        return new Table(schema.addField(field), updatedCols);
    }

    /**
     * Replaces the column at the specified zero-based column index in O(1) zero-copy time[cite: 11].
     *
     * @param index  the zero-based column index to replace[cite: 11]
     * @param field  the replacement field metadata[cite: 11]
     * @param column the replacement column buffer[cite: 11]
     * @return a new {@link Table} with the replaced column[cite: 11]
     */
    public Table replaceColumn(int index, Field field, DataBuffer column) {
        Objects.requireNonNull(field, "field cannot be null");
        Objects.requireNonNull(column, "column cannot be null");
        Objects.checkIndex(index, columns.size());
        List<DataBuffer> updatedCols = new ArrayList<>(columns);
        updatedCols.set(index, column);
        return new Table(schema.replaceField(index, field), updatedCols);
    }

    /**
     * Removes the column with the specified name in O(1) zero-copy time[cite: 11].
     *
     * @param columnName the name of the column to drop[cite: 11]
     * @return a new {@link Table} without the specified column[cite: 11]
     * @throws IllegalArgumentException if {@code columnName} does not exist[cite: 11]
     */
    public Table dropColumn(String columnName) {
        int idx = schema.getFieldIndex(columnName);
        if (idx == -1) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
        List<DataBuffer> updatedCols = new ArrayList<>(columns.size() - 1);
        for (int i = 0; i < columns.size(); i++) {
            if (i != idx) {
                updatedCols.add(columns.get(i));
            }
        }
        return new Table(schema.dropField(columnName), updatedCols);
    }

    /**
     * Renames a column in O(1) zero-copy time[cite: 11].
     *
     * @param oldName the current column name[cite: 11]
     * @param newName the new column name[cite: 11]
     * @return a new {@link Table} with the renamed column[cite: 11]
     * @throws IllegalArgumentException if {@code oldName} is not found or {@code newName} duplicates another column[cite: 11]
     */
    public Table renameColumn(String oldName, String newName) {
        Objects.requireNonNull(newName, "newName cannot be null");
        int idx = schema.getFieldIndex(oldName);
        if (idx == -1) {
            throw new IllegalArgumentException("Column not found: " + oldName);
        }
        Schema renamedSchema = schema.renameField(oldName, newName);
        List<DataBuffer> updatedCols = new ArrayList<>(columns);
        updatedCols.set(idx, columns.get(idx).withName(newName));
        return new Table(renamedSchema, updatedCols);
    }

    /**
     * Gathers rows across all columns using a 32-bit SelectionVector ({@code indices})[cite: 11].
     *
     * @param indices     the 32-bit row indices to gather[cite: 11]
     * @param targetArena the arena in which to allocate the gathered columns[cite: 11]
     * @return a new materialized {@link Table} containing the selected rows[cite: 11]
     */
    public Table take(Int32Buffer indices, Arena targetArena) {
        Objects.requireNonNull(indices, "indices cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");

        boolean indicesHaveNulls = indices.nullCount() > 0;
        List<Field> outFields = new ArrayList<>(columns.size());
        List<DataBuffer> outCols = new ArrayList<>(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            Field f = schema.getField(i);
            DataBuffer gathered = columns.get(i).take(indices, targetArena);
            outCols.add(gathered);
            outFields.add(indicesHaveNulls && !f.nullable() ? new Field(f.name(), f.type(), true) : f);
        }
        return new Table(new Schema(outFields), outCols);
    }

    /**
     * Filters this table using a {@link BooleanBuffer} predicate mask via hardware-accelerated
     * 64-bit word scanning and SelectionVector gathering[cite: 11].
     *
     * @param mask        the boolean predicate mask (must have length equal to {@link #rowCount()})[cite: 11]
     * @param targetArena the arena in which to allocate the filtered columns[cite: 11]
     * @return a new materialized {@link Table} containing only rows where {@code mask} is valid and {@code true}[cite: 11]
     * @throws IllegalArgumentException if {@code mask.length() != rowCount()}[cite: 11]
     */
    public Table filter(BooleanBuffer mask, Arena targetArena) {
        Objects.requireNonNull(mask, "mask cannot be null");
        Objects.requireNonNull(targetArena, "targetArena cannot be null");
        if (mask.length() != this.rowCount) {
            throw new IllegalArgumentException(
                    "Filter mask length (" + mask.length() + ") does not match Table rowCount (" + this.rowCount + ")"
            );
        }
        Int32Buffer selectionIndices = Bitmaps.toSelectionIndices(mask, targetArena);
        return take(selectionIndices, targetArena);
    }

    /**
     * Concatenates this table with {@code other} in O(1) zero-copy time using {@link ChunkedBuffer} columns[cite: 11].
     *
     * @param other the table to append to this table[cite: 11]
     * @return a zero-copy concatenated {@link Table}[cite: 11]
     * @throws IllegalArgumentException if the schemas do not match[cite: 11]
     */
    public Table concat(Table other) {
        Objects.requireNonNull(other, "other Table cannot be null");
        return concat(List.of(this, other));
    }

    /**
     * Concatenates multiple tables sharing an identical {@link Schema} in O(1) zero-copy time[cite: 11].
     *
     * @param tables the non-empty list of tables to concatenate[cite: 11]
     * @return a zero-copy concatenated {@link Table}[cite: 11]
     * @throws IllegalArgumentException if {@code tables} is empty or schemas do not match[cite: 11]
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
     * Compacts any {@link ChunkedBuffer} columns in this table into single contiguous buffers in {@code targetArena}[cite: 11].
     *
     * @param targetArena the arena in which compacted buffers will be allocated[cite: 11]
     * @return a {@link Table} backed exclusively by contiguous physical buffers[cite: 11]
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

    @Override
    public String toString() {
        return "Table[rows=" + rowCount + ", columns=" + columns.size() + ", schema=" + schema + "]";
    }
}