package net.fortytwo.stream.shj;

import net.fortytwo.stream.model.VariableOrConstant;
import net.fortytwo.stream.model.DirectedGraph;
import net.fortytwo.stream.model.SimpleDirectedGraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphPattern<K, V> {

    private final TuplePattern<K, V>[] patterns;

    public GraphPattern(TuplePattern<K, V>... patterns) {
        if (0 == patterns.length) {
            throw new IllegalArgumentException("the empty graph pattern is not supported");
        }
        this.patterns = patterns;
    }

    public TuplePattern<K, V>[] getPatterns() {
        return patterns;
    }

    public boolean isFullyConnected() {
        final Map<K, Set<TuplePattern<K, V>>> patternByVariable = new HashMap<>();
        final Map<TuplePattern<K, V>, Set<K>> variableByPattern = new HashMap<>();

        for (TuplePattern<K, V> p : patterns) {
            Set<K> vars = new HashSet<>();
            variableByPattern.put(p, vars);
            for (VariableOrConstant<K, V> vc : p.getPattern()) {
                K var = vc.getVariable();
                if (null != var) {
                    vars.add(var);
                    Set<TuplePattern<K, V>> set = patternByVariable.get(var);
                    if (null == set) {
                        set = new HashSet<>();
                        patternByVariable.put(var, set);
                    }
                    set.add(p);
                }
            }
        }

        SimpleDirectedGraph<TuplePattern<K, V>> graph = new SimpleDirectedGraph<>(
                new DirectedGraph<TuplePattern<K, V>>() {
                    @Override
                    public void forVertices(Consumer<TuplePattern<K, V>> visitor) {
                        for (TuplePattern<K, V> vertex : patterns) {
                            visitor.accept(vertex);
                        }
                    }

                    @Override
                    public void forOutEdges(TuplePattern<K, V> tail,
                                            Consumer<TuplePattern<K, V>> headVisitor) {
                        for (K key : variableByPattern.get(tail)) {
                            Set<TuplePattern<K, V>> heads = patternByVariable.get(key);
                            if (null != heads) {
                                heads.stream().filter(head -> head != tail).forEach(headVisitor::accept);
                            }
                        }
                    }
                });

        return graph.isFullyConnected();
    }
}
