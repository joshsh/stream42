package edu.rpi.twc.sesamestream.tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

/**
 * An index of partial and complete solutions for a particular query
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionIndex<T> {
    private final Query.QueryVariables queryVariables;
    private final int totalPatterns;

    private final Map<String, Map<T, Set<SolutionGroup<T>>>> groupsByBinding
            = new HashMap<String, Map<T, Set<SolutionGroup<T>>>>();
    private final Map<Integer, SolutionGroup<T>> groupsByHash
            = new HashMap<Integer, SolutionGroup<T>>();

    /**
     * Constructs a new solution index
     *
     * @param queryVariables the variables of the query for which this is an index
     * @param totalPatterns  the total number of tuple patterns in the query for which this is an index
     */
    public SolutionIndex(final Query.QueryVariables queryVariables,
                         final int totalPatterns) {
        this.queryVariables = queryVariables;
        this.totalPatterns = totalPatterns;
    }

    /**
     * Adds a solution to this index.
     *
     * @param s   the solution to add
     * @param now the current time, in milliseconds since the Unix epoch
     * @return whether the solution was actually added, rather than being ignored due to equality with
     * or containment within another solution which does not expire sooner
     */
    public boolean add(final Solution<T> s, final long now) {
        Bindings<T> b = s.getBindings();
        int hash = b.getHash();
        SolutionGroup<T> g = groupsByHash.get(hash);

        if (null == g) {
            // note: assumes that the bindings will not change externally
            g = new SolutionGroup<T>(b);
            groupsByHash.put(hash, g);

            for (Map.Entry<String, T> e : b.entrySet()) {
                Map<T, Set<SolutionGroup<T>>> groupsByValue = groupsByBinding.get(e.getKey());
                if (null == groupsByValue) {
                    groupsByValue = new HashMap<T, Set<SolutionGroup<T>>>();
                    groupsByBinding.put(e.getKey(), groupsByValue);
                }
                Set<SolutionGroup<T>> groups = groupsByValue.get(e.getValue());
                if (null == groups) {
                    groups = new HashSet<SolutionGroup<T>>();
                    groupsByValue.put(e.getValue(), groups);
                }
                groups.add(g);
            }
        }

        return g.add(s, now);
    }

    /**
     * Retrieves all unexpired solutions for a given variable/value pair
     *
     * @param variable a query variable
     * @param value    a value bound to a query variable
     * @param now      the current time, in milliseconds since the Unix epoch
     * @return an iterator over all solutions to the query which contain the given binding
     */
    public Iterator<Solution<T>> getSolutions(final String variable, final T value, final long now) {
        Set<SolutionGroup<T>> groups = getSolutionGroups(variable, value);
        return null == groups ? null : new SolutionIterator<T>(groups.iterator(), now);
    }

    // note: used only in unit tests
    public Iterator<Solution<T>> getComposableSolutions(final String variable,
                                                        final T value,
                                                        final Solution<T> ps,
                                                        final long now) {
        Iterator<Solution<T>> iter = getSolutions(variable, value, now);
        return null == iter ? null : new FilteredIterator<Solution<T>, Solution<T>>(iter) {
            @Override
            protected Solution<T> test(final Solution<T> element) {
                return ps.composableWith(element, queryVariables) ? ps : null;
            }
        };
    }

    // note: used only in unit tests
    public Iterator<Solution<T>> composeSolutions(final String variable,
                                                  final T value,
                                                  final Solution<T> ps,
                                                  final long now) {
        Iterator<Solution<T>> iter = getSolutions(variable, value, now);
        return null == iter ? null : new FilteredIterator<Solution<T>, Solution<T>>(iter) {
            @Override
            protected Solution<T> test(final Solution<T> element) {
                return ps.composableWith(element, queryVariables)
                        // TODO: avoid new object creation here when possible
                        ? new Solution<T>(totalPatterns, ps, element) : null;
            }
        };
    }

    /**
     * Finds the result of joining composable combinations of an original solution together with all
     * unexpired solutions containing any of a provided set of bindings.
     *
     * @param matchedSolution the initial solution produced by matching the tuple pattern against the tuple
     * @param bindings        a set of bindings
     * @param solutions       a stack of solutions which will be cleared, and to which solutions will be added
     * @param helper          a secondary stack for use in computation. It will be cleared.
     */
    public void joinSolutions(final Solution<T> matchedSolution,
                              final Bindings<T> bindings,
                              final Stack<Solution<T>> solutions,
                              final Stack<Solution<T>> helper,
                              final long now) {
        solutions.clear();
        helper.clear();

        // The original solution is among the solutions produced. We assume that it is unexpired.
        solutions.push(matchedSolution);

        // For each binding pair, we retrieve matching solutions from the index,
        // and join them with all solutions produced in previous steps, including the original solution.
        for (Map.Entry<String, T> e : bindings.entrySet()) {
            String var = e.getKey();
            T value = e.getValue();
            Iterator<Solution<T>> retrieved = getSolutions(var, value, now);

            if (null != retrieved && retrieved.hasNext()) {
                while (retrieved.hasNext()) {
                    Solution<T> retrievedSolution = retrieved.next();

                    for (Solution<T> s : solutions) {
                        if (retrievedSolution.composableWith(s, queryVariables)) {
                            // note: this incidentally allows us to discard the mutable object provided by the iterator
                            helper.push(new Solution<T>(totalPatterns, retrievedSolution, s));
                        }
                    }
                }

                while (!helper.isEmpty()) {
                    solutions.push(helper.pop());
                }
            }
        }
    }

    /**
     * Removes all expired solutions from this index.
     * This is an exhaustive, top-down operation which takes a relatively large amount of time.
     *
     * @param now the current time, in milliseconds since the Unix epoch
     * @return the number of solutions removed
     */
    public int removeExpired(final long now) {
        int count = 0;

        Collection<SolutionGroup<T>> toRemove = new LinkedList<SolutionGroup<T>>();

        for (SolutionGroup<T> g : groupsByHash.values()) {
            count += g.removeExpired(now);
            if (g.getSolutions().isNil()) {
                toRemove.add(g);
            }
        }

        for (SolutionGroup<T> g : toRemove) {
            for (Map.Entry<String, T> e : g.getBindings().entrySet()) {
                Map<T, Set<SolutionGroup<T>>> groupsByValue = groupsByBinding.get(e.getKey());
                if (null != groupsByValue) {
                    Set<SolutionGroup<T>> groups = groupsByValue.get(e.getValue());
                    if (null != groups) {
                        groups.remove(g);
                        if (0 == groups.size()) {
                            groupsByValue.remove(e.getValue());
                        }
                        if (0 == groupsByValue.size()) {
                            groupsByBinding.remove(e.getKey());
                        }
                    }
                }
            }
        }

        return count;
    }

    private Set<SolutionGroup<T>> getSolutionGroups(final String variable, final T value) {
        Map<T, Set<SolutionGroup<T>>> groupsByValue = groupsByBinding.get(variable);
        if (null == groupsByValue) {
            return null;
        }

        return groupsByValue.get(value);
    }

    private static class SolutionIterator<T> implements Iterator<Solution<T>> {

        // note: solution is mutated on each call to next().  Read but do not store the object.
        private final Solution<T> solution = new Solution<T>(0, 0, null, 0);

        private final Iterator<SolutionGroup<T>> groupIterator;
        private LList<SolutionPattern> currentSolutions;
        private boolean advanced;
        private final long now;

        // note: assumes a non-empty iterator of groups
        public SolutionIterator(final Iterator<SolutionGroup<T>> groupIterator,
                                final long now) {
            this.groupIterator = groupIterator;
            this.now = now;
        }

        @Override
        public boolean hasNext() {
            if (!advanced) {
                advance();
            }

            return !currentSolutions.isNil();
        }

        @Override
        public Solution<T> next() {
            if (!advanced) {
                throw new IllegalStateException();
            }

            advanced = false;

            SolutionPattern r = currentSolutions.getValue();
            // update the type info of the current solution on each step
            solution.copyFrom(r);

            return solution;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void advance() {
            if (null != currentSolutions) {
                setCurrentSolutions(currentSolutions.getRest());
            }

            // note: the loop tolerates empty groups, although these should not occur
            while (null == currentSolutions || currentSolutions.isNil()) {
                if (groupIterator.hasNext()) {
                    SolutionGroup<T> g = groupIterator.next();
                    setCurrentSolutions(g.getSolutions());
                    // update the bindings of the current solution whenever we enter a new group
                    solution.setBindings(g.getBindings());
                } else {
                    break;
                }
            }

            advanced = true;
        }

        private void setCurrentSolutions(final LList<SolutionPattern> sols) {
            currentSolutions = sols;

            // skip expired solutions.  For now, we don't remove them, as we don't want empty groups and indices.
            while (!currentSolutions.isNil() && currentSolutions.getValue().isExpired(now)) {
                currentSolutions = currentSolutions.getRest();
            }
        }
    }

    private static abstract class FilteredIterator<S, T> implements Iterator<T> {
        private final Iterator<S> baseIterator;
        private T currentValue;

        protected FilteredIterator(Iterator<S> baseIterator) {
            this.baseIterator = baseIterator;
            advance();
        }

        protected abstract T test(S element);

        @Override
        public boolean hasNext() {
            return null != currentValue;
        }

        @Override
        public T next() {
            T lastValue = currentValue;
            advance();
            return lastValue;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void advance() {
            while (baseIterator.hasNext()) {
                S fromValue = baseIterator.next();
                T toValue = test(fromValue);
                if (null != toValue) {
                    currentValue = toValue;
                    return;
                }
            }

            currentValue = null;
        }
    }
}
