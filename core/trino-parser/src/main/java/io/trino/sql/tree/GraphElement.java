package io.trino.sql.tree;

import java.util.Optional;

public abstract class GraphElement extends Node {
    protected GraphElement(Optional<NodeLocation> location) {
        super(location);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGraphElement(this, context);
    }
}
