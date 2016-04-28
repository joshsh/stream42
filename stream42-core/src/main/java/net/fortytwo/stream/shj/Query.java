package net.fortytwo.stream.shj;

import net.fortytwo.stream.model.VariableOrConstant;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Query<K, V> implements Expirable {
    
    private GraphPattern<K, V> graphPattern;
    private BiConsumer<Map<K, V>, Long> solutionHandler;
    private final long expirationTime;
    private final Index<Query<K, V>> expirationIndex;

    private List<JoinHelper<K, V>> allHelpers;

    public Query(GraphPattern<K, V> graphPattern,
                 long expirationTime,
                 Index<Query<K, V>> expirationIndex,
                 BiConsumer<Map<K, V>, Long> solutionHandler) {
        this.graphPattern = graphPattern;
        this.expirationTime = expirationTime;
        this.expirationIndex = expirationIndex;
        this.solutionHandler = solutionHandler;

        checkValid();
    }

    public GraphPattern<K, V> getGraphPattern() {
        return graphPattern;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public void expire() {
        expirationIndex.remove(this);

        graphPattern = null;
        solutionHandler = null;
    }

    @Override
    public boolean isExpired() {
        return null == graphPattern;
    }

    public List<JoinHelper<K, V>> getAllHelpers() {
        return allHelpers;
    }

    public void setAllHelpers(List<JoinHelper<K, V>> allHelpers) {
        if (null != this.allHelpers) {
            throw new IllegalStateException();
        }

        this.allHelpers = allHelpers;

        Map<K, Set<JoinHelper<K, V>>> helpersByVariable = new HashMap<>();
        for (JoinHelper<K, V> wrapper : allHelpers) {
            for (K key : wrapper.getKeys()) {
                Set<JoinHelper<K, V>> set = helpersByVariable.get(key);
                if (null == set) {
                    set = QueryContext.newConcurrentSet();
                    helpersByVariable.put(key, set);
                }
                set.add(wrapper);
            }
        }

        // TODO: now construct the query plan. Use solutionHandler.

        for (JoinHelper<K, V> helper : allHelpers) {
            helper.initialize(allHelpers, helpersByVariable, solutionHandler);
        }
    }

    private void checkValid() {
        int i = 0;
        for (TuplePattern<K, V> pattern : graphPattern.getPatterns()) {
            boolean hasVariables = false;
            for (VariableOrConstant<K, V> vc : pattern.getPattern()) {
                if (null != vc.getVariable()) {
                    hasVariables = true;
                    break;
                }
            }
            if (!hasVariables) {
                throw new IllegalArgumentException("tuple pattern " + i + " has no variables");
            }
            i++;
        }
    }
}
