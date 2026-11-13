package io.github.shridey.jdata.core.layout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable, ordered collection of Fields defining a Table's structure.
 */
public final class Schema {

    private final List<Field> fields;
    private final Map<String, Integer> nameToIndex;

    public Schema(List<Field> fields) {
        Objects.requireNonNull(fields, "Fields list cannot be null");
        this.fields = List.copyOf(fields);
        this.nameToIndex = new HashMap<>(this.fields.size() * 2);

        for (int i = 0; i < this.fields.size(); i++) {
            Field f = Objects.requireNonNull(this.fields.get(i), "Field at index " + i + " cannot be null");
            if (this.nameToIndex.put(f.name(), i) != null) {
                throw new IllegalArgumentException("Duplicate column name in Schema: " + f.name());
            }
        }
    }

    public int columnCount() {
        return fields.size();
    }

    public List<Field> fields() {
        return fields;
    }

    public Field getField(int index) {
        return fields.get(index);
    }

    public Field getField(String name) {
        int idx = getFieldIndex(name);
        if (idx == -1) {
            throw new IllegalArgumentException("Field not found in Schema: " + name);
        }
        return fields.get(idx);
    }

    public int getFieldIndex(String name) {
        return nameToIndex.getOrDefault(name, -1);
    }

    public Schema select(String... fieldNames) {
        Objects.requireNonNull(fieldNames, "fieldNames cannot be null");
        List<Field> selected = new ArrayList<>(fieldNames.length);
        for (String name : fieldNames) {
            selected.add(getField(name));
        }
        return new Schema(selected);
    }

    public Schema addField(Field newField) {
        Objects.requireNonNull(newField, "newField cannot be null");
        List<Field> updated = new ArrayList<>(fields.size() + 1);
        updated.addAll(fields);
        updated.add(newField);
        return new Schema(updated);
    }

    public Schema replaceField(int index, Field replacement) {
        Objects.requireNonNull(replacement, "replacement cannot be null");
        Objects.checkIndex(index, fields.size());
        List<Field> updated = new ArrayList<>(fields);
        updated.set(index, replacement);
        return new Schema(updated);
    }

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