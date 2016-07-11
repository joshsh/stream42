package net.fortytwo.stream.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SimpleDirectedGraph<T> implements DirectedGraph<T> {
    private final Set<T> vertices = new HashSet<>();
    private final Map<T, Set<T>> edges = new HashMap<>();

    public SimpleDirectedGraph() {
    }

    // copy constructor
    public SimpleDirectedGraph(final DirectedGraph<T> other) {
        other.forVertices(new Consumer<T>() {
            @Override
            public void accept(final T vertex) {
                vertices.add(vertex);

                other.forOutEdges(vertex, head -> addEdge(vertex, head));
            }
        });
    }

    public void addEdge(T tail, T head) {
        vertices.add(tail);
        vertices.add(head);

        Set<T> inc = edges.get(tail);
        if (null == inc) {
            inc = new HashSet<>();
            edges.put(tail, inc);
        }

        inc.add(head);
    }

    @Override
    public void forVertices(Consumer<T> visitor) {
        vertices.forEach(visitor::accept);
    }

    @Override
    public void forOutEdges(T tail, Consumer<T> headVisitor) {
        Set<T> inc = edges.get(tail);
        if (null == inc) {
            throw new IllegalArgumentException();
        }

        inc.forEach(headVisitor::accept);
    }

    public boolean isFullyConnected() {
        // the empty graph is considered here to be unconnected
        if (0 == edges.size()) {
            return 1 == vertices.size();
        }

        if (1 == edges.size() && 1 == vertices.size()) {
            return true;
        }

        Set<T> tails = new HashSet<>();
        tails.add(edges.keySet().iterator().next());
        int before;
        do {
            before = tails.size();
            Collection<T> buffer = new LinkedList<>();
            buffer.addAll(tails);
            for (T tail : buffer) {
                Set<T> heads = edges.get(tail);
                if (null != heads) {
                    tails.addAll(heads);
                }
            }
        } while (tails.size() != before);

        return tails.size() == vertices.size();
    }
}
