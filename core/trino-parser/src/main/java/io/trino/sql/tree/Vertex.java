package io.trino.sql.tree;

import java.util.List;
import java.util.Optional;

public class Vertex extends Node {

    protected Vertex(Optional<NodeLocation> location) {
        super(location);
    }

    @Override
    public List<? extends Node> getChildren() {
        return null;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public String toString() {
        return null;
    }
}
