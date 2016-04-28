package net.fortytwo.stream.shj;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A wrapper for a solution index which applies query-specific variable bindings
 * There is one wrapper for each query containing a triple pattern, ignoring variable names
 *
 * @param <K> the key type, e.g. String
 * @param <V> the value type, e.g. an RDF value class
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class JoinHelper<K, V> implements Consumer<Solution<V>> {

    private final SolutionIndex<V> solutionIndex;

    private final K[] keys;
    private final Map<K, Integer> indexByKey;
    private List<JoinHelper<K, V>> allHelpers;
    private Map<K, Set<JoinHelper<K, V>>> helpersByVariable;
    private BiConsumer<Map<K, V>, Long> solutionConsumer;

    public JoinHelper(SolutionIndex<V> solutionIndex, Map<K, Integer> indexByKey) {
        this.solutionIndex = solutionIndex;
        this.indexByKey = indexByKey;

        keys = (K[]) new Object[indexByKey.size()];
        for (Map.Entry<K, Integer> e : indexByKey.entrySet()) {
            keys[e.getValue()] = e.getKey();
        }
    }

    public Set<Solution<V>> getSolutions() {
        return solutionIndex.getSolutions();
    }

    /**
     * Finds all solutions with the given key/value pair
     *
     * @param key a key present in any matching solutions
     * @param val the corresponding value for the key in any matching solutions
     * @return a set of all matching solutions. A null is returned for a logically empty set.
     */
    public Set<Solution<V>> getSolutions(K key, V val) {
        Integer index = indexByKey.get(key);
        return null == index ? null : solutionIndex.getSolutions(index, val);
    }

    public K[] getKeys() {
        return keys;
    }

    // TODO: eliminate this separate call
    public void initialize(List<JoinHelper<K, V>> allHelpers,
                           Map<K, Set<JoinHelper<K, V>>> helpersByVariable,
                           BiConsumer<Map<K, V>, Long> solutionConsumer) {
        this.allHelpers = allHelpers;
        this.helpersByVariable = helpersByVariable;
        this.solutionConsumer = solutionConsumer;
    }

    @Override
    public void accept(Solution<V> solution) {
        Set<JoinHelper<K, V>> remaining = new HashSet<>();
        remaining.addAll(allHelpers);
        Collection<Solution<V>> solutions = new LinkedList<>();
        solutions.add(solution);

        // arbitrarily choose the first solution key as the join key
        K newBoundKey = keys[0];

        // create an expanding array of temporary maps, to save on new object creation
        List<Map<K, V>> tmpMaps = new ArrayList<>();
        // first map contains the accepted partial solution. This will not change.
        Map<K, V> mapping = new HashMap<>();
        V[] values = solution.getValues();
        for (int i = 0; i < keys.length; i++) {
            mapping.put(keys[i], values[i]);
        }

        hashJoin(solutions, remaining, newBoundKey, tmpMaps, mapping, 0, false);
    }

    private void hashJoin(Collection<Solution<V>> solutions,
                          Set<JoinHelper<K, V>> remaining,
                          K newBoundKey,
                          List<Map<K, V>> maps,
                          Map<K, V> curMapping,
                          int depth,
                          boolean checkCompatible) {
        remaining.remove(this);

        Map<K, V> nextMapping;
        if (checkCompatible) {
            // re-use a map if possible
            if (depth < maps.size()) {
                nextMapping = maps.get(depth);
                nextMapping.clear();
            } else {
                nextMapping = new HashMap<>();
                nextMapping.putAll(curMapping);
            }
        } else {
            nextMapping = curMapping;
        }

        for (Solution<V> solution : solutions) {
            trySolution(solution, remaining, newBoundKey, maps, curMapping, nextMapping, depth + 1, checkCompatible);
        }

        remaining.add(this);
    }

    private void trySolution(Solution<V> solution, Set<JoinHelper<K, V>> remaining, K newBoundKey,
                             List<Map<K, V>> maps, Map<K, V> curMapping, Map<K, V> nextMapping,
                             int depth, boolean checkCompatible) {
        V[] values = solution.getValues();

        // first iterate over the solution's key/value pairs to filter out incompatible solutions
        // no need to filter if this is the first helper or there are no additional keys
        if (checkCompatible && keys.length > 1) {
            boolean compatible = true;
            for (int i = 0; i < keys.length; i++) {
                K key = keys[i];

                if (!key.equals(newBoundKey)) {
                    V val = values[i];
                    V boundVal = curMapping.get(key);
                    if (null != boundVal && !boundVal.equals(val)) {
                        compatible = false;
                        break;
                    }

                    // also add the solution's bindings to the mapping under construction
                    nextMapping.put(key, val);
                }
            }
            if (!compatible) return;
        }

        if (remaining.isEmpty()) {
            // create a copy of the mapping, as the temporary one may continue to change
            Map<K, V> copy = new HashMap<>(nextMapping);
            // this is a complete solution; write it to the query's consumer
            solutionConsumer.accept(copy, solution.getExpirationTime());
        } else {
            // this is an incomplete solution; we must join the partial solution with others
            JoinHelper<K, V> bestHelper = null;
            K bestKey = null;
            Set<Solution<V>> bestSet = null;
            int minSize = Integer.MAX_VALUE;

            // iterate over all key/value pairs in the new mapping to find the smallest join set
            for (Map.Entry<K, V> e : nextMapping.entrySet()) {
                K key = e.getKey();
                V val = e.getValue();

                // try all helpers with this already-bound key
                for (JoinHelper<K, V> helper : helpersByVariable.get(key)) {
                    // ignore helpers already used to compute the solution in progress
                    if (remaining.contains(helper)) {
                        Set<Solution<V>> sols = helper.getSolutions(key, val);
                        if (null == sols) {
                            // there is at least one pattern containing this key without a partial solution,
                            // so a complete solution is not possible
                            return;
                        }

                        // as a computation-saving heuristic, choose the smallest set to join
                        int size = sols.size();
                        if (size < minSize) {
                            bestHelper = helper;
                            bestKey = key;
                            bestSet = sols;
                            minSize = size;
                        }
                    }
                }
            }

            // we found at least one join set; we pick this one and ignore the others
            if (null != bestHelper) {
                bestHelper.hashJoin(bestSet, remaining, bestKey, maps, nextMapping, depth, true);
            }
        }
    }
}
