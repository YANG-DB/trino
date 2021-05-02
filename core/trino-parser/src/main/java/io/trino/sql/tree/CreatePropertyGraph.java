/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreatePropertyGraph
        extends Statement
    {
    private final QualifiedName name;
    private final List<GraphElement> elements;
    private final boolean notExists;
    private final List<Vertex> vertices;
    private final List<Edge> edges;
    private final Optional<String> comment;

    public CreatePropertyGraph(QualifiedName name, List<GraphElement> elements, boolean notExists, List<Vertex> vertices, List<Edge> edges, Optional<String> comment)
    {
        this(Optional.empty(), name, elements, notExists, vertices, edges, comment);
    }

    public CreatePropertyGraph(NodeLocation location, QualifiedName name, List<GraphElement> elements, boolean notExists, List<Vertex> vertices, List<Edge> edges, Optional<String> comment)
    {
        this(Optional.of(location), name, elements, notExists, vertices, edges, comment);
    }

    private CreatePropertyGraph(Optional<NodeLocation> location, QualifiedName name, List<GraphElement> elements, boolean notExists, List<Vertex> vertices, List<Edge> edges, Optional<String> comment)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
        this.notExists = notExists;
        this.edges = requireNonNull(edges, "properties is null");
        this.vertices = requireNonNull(vertices, "properties is null");
        this.comment = requireNonNull(comment, "comment is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public List<GraphElement> getElements()
    {
        return elements;
    }

    public boolean isNotExists()
    {
        return notExists;
    }


    public List<Vertex> getVertices()
    {
        return vertices;
    }


    public List<Edge> getEdges()
    {
        return edges;
    }

    public Optional<String> getComment()
{
        return comment;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
{
        return (R) visitor.visitCreateGraph(this, context);
    }


    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .addAll(elements)
                .addAll(vertices)
                .addAll(edges)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, elements, notExists, vertices, edges, comment);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
{
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass()))
{
            return false;
        }
        CreatePropertyGraph o = (CreatePropertyGraph) obj;
        return Objects.equals(name, o.name) &&
                Objects.equals(elements, o.elements) &&
                Objects.equals(notExists, o.notExists) &&
                Objects.equals(vertices, o.vertices) &&
                Objects.equals(edges, o.edges) &&
                Objects.equals(comment, o.comment);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("elements", elements)
                .add("notExists", notExists)
                .add("vertices", vertices)
                .add("edges", edges)
                .add("comment", comment)
                .toString();
    }
}
