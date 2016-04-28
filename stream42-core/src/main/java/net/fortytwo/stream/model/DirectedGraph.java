package net.fortytwo.stream.model;

import java.util.function.Consumer;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface DirectedGraph<T> {
    void forVertices(Consumer<T> visitor);
    void forOutEdges(T tail, Consumer<T> headVisitor);
}
