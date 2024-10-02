package de.hpi.isg.RelationalDependencyRules;

import java.util.HashSet;
import java.util.Objects;

public class Cell {
    public static class HyperEdge extends HashSet<Cell> {
        public HyperEdge(int capacity) {
            super(capacity);
        }

        public Cell minCell = null;
    }

    public Attribute attribute;
    public String key;

    public String value;
    public long insertionTime;
    public long cost = Integer.MAX_VALUE;

    public Cell(Attribute attribute, String key) {
        this.attribute = attribute;
        this.key = key;
    }

    public Cell(Attribute attribute, String key, String value) {
        this.attribute = attribute;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return attribute.toString() + "[" + key + "] => " + value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Cell cell = (Cell) o;

        if (!attribute.equals(cell.attribute)) return false;
        if (!key.equals(cell.key)) return false;
        return Objects.equals(value, cell.value);
    }

    @Override
    public int hashCode() {
        int result = attribute.hashCode();
        result = 31 * result + key.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
