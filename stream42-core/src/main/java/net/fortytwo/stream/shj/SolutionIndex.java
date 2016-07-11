package net.fortytwo.stream.shj;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * A SolutionIndex blocks for concurrent write operations, and does not block reads with respect to reads or writes.
 *
 * @param <V> the value type, e.g. an RDF value class
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionIndex<V> implements Index<Solution<V>> {

    // the number of variables bound in solutions
    private final int cardinality;

    private final QueryContext<?, V> queryContext;

    // note: must be thread-safe, as we read and write concurrently, even though we do not iterate
    private final Map<Solution<V>, Solution<V>> allSolutions = new ConcurrentHashMap<>();

    // note: must be thread-safe, as we read and write concurrently, even though we do not iterate
    private final Map<V, Set<Solution<V>>>[] solutionsByBinding;

    // note: must be thread-safe because we may iterate over and add subscribers concurrently
    private final Set<Consumer<Solution<V>>> consumers = QueryContext.newConcurrentSet();

    private final Index<Consumer<Solution<V>>> consumerIndex = new Index<Consumer<Solution<V>>>() {
        @Override
        public void add(Consumer<Solution<V>> toAdd) {
            consumers.add(toAdd);
        }

        @Override
        public boolean remove(Consumer<Solution<V>> toRemove) {
            return consumers.remove(toRemove);
        }

        @Override
        public void clear() {
            consumers.clear();
        }

        @Override
        public boolean isEmpty() {
            return consumers.isEmpty();
        }
    };

    public SolutionIndex(QueryContext<?, V> queryContext, int cardinality) {
        this.cardinality = cardinality;
        this.queryContext = queryContext;

        if (null == queryContext) {
            throw new IllegalArgumentException("null context");
        }

        if (cardinality < 1) {
            throw new IllegalArgumentException("illegal index length: " + cardinality);
        }

        solutionsByBinding = new Map[cardinality];
        for (int i = 0; i < cardinality; i++) {
            // note: must be thread-safe, as we read and write concurrently
            solutionsByBinding[i] = new ConcurrentHashMap<>();
        }
    }

    /**
     * Gets the variable-cardinality of this index
     *
     * @return the number of distinct variables of each solution in this index
     */
    public int getCardinality() {
        return cardinality;
    }

    public Index<Consumer<Solution<V>>> getConsumerIndex() {
        return consumerIndex;
    }

    /**
     * Adds a solution to the index.
     * Identical but earlier-expiring solutions are displaced
     *
     * @param solution the solution to be added
     */
    @Override
    public void add(Solution<V> solution) {
        // step 1 of symmetric hash join: index locally
        // this immediately makes the solution available for retrieval through all join indices
        addInternal(solution);

        // step 2 of symmetric hash join: resolve and push to join indices for join operations
        for (Consumer<Solution<V>> s : consumers) {
            s.accept(solution);
        }
    }

    @Override
    public synchronized boolean remove(Solution<V> solution) {
        Solution<V> removed = allSolutions.remove(solution);
        if (null != removed) {
            for (int i = 0; i < cardinality; i++) {
                V val = solution.getValues()[i];
                Map<V, Set<Solution<V>>> solsForVariable = solutionsByBinding[i];
                Set<Solution<V>> sols = solsForVariable.get(val);
                if (null != sols) {
                    sols.remove(solution);
                    if (sols.isEmpty()) {
                        solsForVariable.remove(val);
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Removes any matching solution from the index.
     * Removal is idempotent; it is possible to "remove" without error a pattern which has already been removed
     *
     * @param pattern a pattern matching the solutions to be removed
     * @return whether any matching solutions were found and removed
     */
    public boolean removePattern(V[] pattern) {
        // select the smallest solution set for iteration and filtering
        int minCard = Integer.MAX_VALUE;
        int bestIndex = -1;
        V bestValue = null;
        for (int i = 0; i < cardinality; i++) {
            V val = pattern[i];
            if (null != val) {
                Map<V, Set<Solution<V>>> byBinding = solutionsByBinding[i];
                Set<Solution<V>> sols = byBinding.get(val);
                if (null == sols) {
                    return false;
                } else {
                    // note: solution sets are never empty
                    int card = sols.size();
                    if (card < minCard) {
                        bestIndex = i;
                        minCard = card;
                        bestValue = val;
                    }
                }
            }
        }

        if (-1 == bestIndex) {
            // all wildcards; remove all
            return removeAllInternal();
        } else {
            // synchronize on write
            synchronized (this) {
                Set<Solution<V>> toRemove = new HashSet<>();

                // TODO minor optimization: assign these above
                Map<V, Set<Solution<V>>> byBinding = solutionsByBinding[bestIndex];
                Set<Solution<V>> sols = byBinding.get(bestValue);
                for (Solution<V> s : sols) {
                    boolean matches = true;
                    for (int i = 0; i < cardinality; i++) {
                        V val = pattern[i];
                        if (null != val && !val.equals(s.getValues()[i])) {
                            matches = false;
                            break;
                        }
                    }

                    if (matches) {
                        toRemove.add(s);
                    }
                }
                boolean removed = !toRemove.isEmpty();
                for (Solution<V> s : toRemove) {
                    sols.remove(s);
                    if (sols.isEmpty()) {
                        byBinding.remove(bestValue);
                    }
                    if (null != allSolutions.remove(s)) {
                        removeFromManager(s);
                    }
                }
                toRemove.clear();
                return removed;
            }
        }
    }

    @Override
    public synchronized void clear() {
        allSolutions.clear();
        for (Map<V, Set<Solution<V>>> set : solutionsByBinding) {
            set.clear();
        }
        consumerIndex.clear();
    }

    @Override
    public boolean isEmpty() {
        return allSolutions.isEmpty();
    }

    public Set<Solution<V>> getSolutions() {
        return allSolutions.keySet();
    }

    public Set<Solution<V>> getSolutions(int index, V value) {
        // note: no buffering required, as we trust the concurrent set not to lock on reads
        // during lengthy matching operations
        return solutionsByBinding[index].get(value);
    }

    // synchronize on write
    private synchronized boolean removeAllInternal() {
        boolean removed = !allSolutions.isEmpty();

        // note: non-expiring solutions have no effect on the manager
        allSolutions.keySet().forEach(this::removeFromManager);
        allSolutions.clear();

        for (Map<V, Set<Solution<V>>> solutions : solutionsByBinding) {
            solutions.clear();
        }

        return removed;
    }

    private void addInternal(Solution<V> solution) {
        Solution<V> existing = allSolutions.get(solution);
        if (null != existing) {
            int cmp = solution.compareByExpirationTime(existing);
            if (cmp <= 0) {
                // existing solution already contains the new one
                return;
            } else {
                // new solution supersedes the existing solution
                remove(existing);
            }
        }

        // synchronize on write
        synchronized (this) {
            allSolutions.put(solution, solution);
            for (int i = 0; i < cardinality; i++) {
                Map<V, Set<Solution<V>>> byBinding = solutionsByBinding[i];
                V val = solution.getValues()[i];
                Set<Solution<V>> sols = byBinding.get(val);
                if (null == sols) {
                    // note: must be thread-safe, as we iterate and modify concurrently
                    sols = QueryContext.newConcurrentSet();
                    byBinding.put(val, sols);
                }
                sols.add(solution);
            }
        }
    }

    private void removeFromManager(Solution<V> toRemove) {
        queryContext.getSolutionExpirationManager().remove(toRemove);
    }
}
