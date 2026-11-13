package io.github.shridey.jdata.core.layout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable, ordered collection of {@link Field} definitions describing the
 * structure of a {@link Table}[cite: 10, 11]. Guarantees unique column names and O(1) name-to-index lookup[cite: 10].
 */
public final class Schema {

    private final List<Field> fields;
    private final Map<String, Integer> nameToIndex;

    /**
     * Creates a new immutable {@link Schema} from the given ordered list of fields[cite: 10].
     *
     * @param fields the ordered list of column {@link Field} definitions[cite: 10]
     * @throws NullPointerException     if {@code fields} or any element within it is {@code null}[cite: 10]
     * @throws IllegalArgumentException if duplicate field names are present[cite: 10]
     */
    public Schema(List<Field> fields) {
        Objects.requireNonNull(fields, "Fields list cannot be null");
        this.nameToIndex = new HashMap<>(fields.size() * 2);

        for (int i = 0; i < fields.size(); i++) {
            Field f = Objects.requireNonNull(fields.get(i), "Field at index " + i + " cannot be null");
            if (this.nameToIndex.put(f.name(), i) != null) {
                throw new IllegalArgumentException("Duplicate column name in Schema: " + f.name());
            }
        }
        this.fields = List.copyOf(fields);
    }

    /**
     * Returns the number of columns in this schema[cite: 10].
     *
     * @return the column count[cite: 10]
     */
    public int columnCount() {
        return fields.size();
    }

    /**
     * Returns an unmodifiable view of the ordered {@link Field}s in this schema[cite: 10].
     *
     * @return the immutable list of fields[cite: 10]
     */
    public List<Field> fields() {
        return fields;
    }

    /**
     * Returns the {@link Field} at the specified zero-based column index[cite: 10].
     *
     * @param index the zero-based column index[cite: 10]
     * @return the {@link Field} at {@code index}[cite: 10]
     * @throws IndexOutOfBoundsException if {@code index < 0 || index >= columnCount()}[cite: 10]
     */
    public Field getField(int index) {
        return fields.get(index);
    }

    /**
     * Returns the {@link Field} matching the specified column name[cite: 10].
     *
     * @param name the column name to look up[cite: 10]
     * @return the matching {@link Field}[cite: 10]
     * @throws IllegalArgumentException if no field with {@code name} exists in this schema[cite: 10]
     */
    public Field getField(String name) {
        int idx = getFieldIndex(name);
        if (idx == -1) {
            throw new IllegalArgumentException("Field not found in Schema: " + name);
        }
        return fields.get(idx);
    }

    /**
     * Returns the zero-based column index for the given field name, or {@code -1} if not found[cite: 10].
     *
     * @param name the column name to look up[cite: 10]
     * @return the zero-based column index, or {@code -1} if absent[cite: 10]
     */
    public int getFieldIndex(String name) {
        return nameToIndex.getOrDefault(name, -1);
    }

    /**
     * Creates a new {@link Schema} containing only the specified field names in the requested order[cite: 10].
     *
     * @param fieldNames the column names to project[cite: 10]
     * @return a projected {@link Schema}[cite: 10]
     * @throws IllegalArgumentException if any field name is not found or is duplicated[cite: 10]
     */
    public Schema select(String... fieldNames) {
        Objects.requireNonNull(fieldNames, "fieldNames cannot be null");
        List<Field> selected = new ArrayList<>(fieldNames.length);
        for (String name : fieldNames) {
            selected.add(getField(name));
        }
        return new Schema(selected);
    }

    /**
     * Returns a new {@link Schema} with {@code newField} appended to the end[cite: 10].
     *
     * @param newField the field to append[cite: 10]
     * @return an updated {@link Schema}[cite: 10]
     * @throws IllegalArgumentException if a field with the same name already exists[cite: 10]
     */
    public Schema addField(Field newField) {
        Objects.requireNonNull(newField, "newField cannot be null");
        List<Field> updated = new ArrayList<>(fields.size() + 1);
        updated.addAll(fields);
        updated.add(newField);
        return new Schema(updated);
    }

    /**
     * Returns a new {@link Schema} with the field at {@code index} replaced by {@code replacement}[cite: 10].
     *
     * @param index       the zero-based column index to replace[cite: 10]
     * @param replacement the new {@link Field} definition[cite: 10]
     * @return an updated {@link Schema}[cite: 10]
     * @throws IndexOutOfBoundsException if {@code index} is out of bounds[cite: 10]
     * @throws IllegalArgumentException  if {@code replacement} introduces a duplicate column name[cite: 10]
     */
    public Schema replaceField(int index, Field replacement) {
        Objects.requireNonNull(replacement, "replacement cannot be null");
        Objects.checkIndex(index, fields.size());
        List<Field> updated = new ArrayList<>(fields);
        updated.set(index, replacement);
        return new Schema(updated);
    }

    /**
     * Returns a new {@link Schema} with the specified field removed[cite: 10].
     *
     * @param name the name of the field to remove[cite: 10]
     * @return an updated {@link Schema} without {@code name}[cite: 10]
     * @throws IllegalArgumentException if {@code name} is not found in this schema[cite: 10]
     */
    public Schema dropField(String name) {
        int idx = getFieldIndex(name);
        if (idx == -1) {
            throw new IllegalArgumentException("Field not found in Schema: " + name);
        }
        List<Field> updated = new ArrayList<>(fields.size() - 1);
        for (int i = 0; i < fields.size(); i++) {
            if (i != idx) {
                updated.add(fields.get(i));
            }
        }
        return new Schema(updated);
    }

    /**
     * Returns a new {@link Schema} with {@code oldName} renamed to {@code newName} at the same index[cite: 10].
     *
     * @param oldName the existing column name[cite: 10]
     * @param newName the new column name[cite: 10]
     * @return an updated {@link Schema}[cite: 10]
     * @throws IllegalArgumentException if {@code oldName} is not found or {@code newName} duplicates another column[cite: 10]
     */
    public Schema renameField(String oldName, String newName) {
        Objects.requireNonNull(newName, "newName cannot be null");
        int idx = getFieldIndex(oldName);
        if (idx == -1) {
            throw new IllegalArgumentException("Field not found in Schema: " + oldName);
        }
        Field current = fields.get(idx);
        return replaceField(idx, new Field(newName, current.type(), current.nullable()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Schema other)) return false;
        return fields.equals(other.fields);
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }

    @Override
    public String toString() {
        return "Schema" + fields;
    }
}