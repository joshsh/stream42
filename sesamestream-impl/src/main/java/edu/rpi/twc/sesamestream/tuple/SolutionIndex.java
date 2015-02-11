package edu.rpi.twc.sesamestream.tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Stack;
import java.util.logging.Logger;

/**
 * An index of partial and complete solutions for a given query
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionIndex<T> {
    private final Map<String, Map<T, GroupIndex<T>>> solutionsByBinding
            = new HashMap<String, Map<T, GroupIndex<T>>>();
    private final GraphPattern.QueryVariables queryVariables;
    private final int totalPatterns;

    public SolutionIndex(final GraphPattern.QueryVariables queryVariables,
                         final int totalPatterns) {
        this.queryVariables = queryVariables;
        this.totalPatterns = totalPatterns;
    }

    public void add(final Solution<T> ps, final long now) {
        // make the added partial solution accessible through each of its bindings
        for (Map.Entry<String, T> e : ps.getBindings().entrySet()) {
            add(e.getKey(), e.getValue(), ps, now);
        }
    }

    public void add(final String variable, final T value, final Solution<T> ps, final long now) {
        Map<T, GroupIndex<T>> byVariable = solutionsByBinding.get(variable);
        if (null == byVariable) {
            byVariable = new HashMap<T, GroupIndex<T>>();
            solutionsByBinding.put(variable, byVariable);
        }

        GroupIndex<T> byValue = byVariable.get(value);
        if (null == byValue) {
            byValue = new GroupIndex<T>();
            byVariable.put(value, byValue);
        }

        byValue.add(ps, now);
    }

    public Iterator<Solution<T>> getSolutions(final String variable, final T value) {
        GroupIndex<T> g = getGroupIndex(variable, value);
        return null == g ? null : new SolutionIterator<T>(g.groups.values().iterator());
    }

    public Iterator<Solution<T>> getComplementarySolutions(final String variable,
                                                                  final T value,
                                                                  final Solution<T> ps) {
        Iterator<Solution<T>> iter = getSolutions(variable, value);
        return null == iter ? null : new FilteredIterator<Solution<T>, Solution<T>>(iter) {
            @Override
            protected Solution<T> test(final Solution<T> element) {
                return ps.complements(element, queryVariables) ? ps : null;
            }
        };
    }

    public Iterator<Solution<T>> composeSolutions(final String variable,
                                                         final T value,
                                                         final Solution<T> ps) {
        Iterator<Solution<T>> iter = getSolutions(variable, value);
        return null == iter ? null : new FilteredIterator<Solution<T>, Solution<T>>(iter) {
            @Override
            protected Solution<T> test(final Solution<T> element) {
                return ps.complements(element, queryVariables)
                        // TODO: avoid new object creation here when possible
                        ? new Solution<T>(totalPatterns, ps, element) : null;
            }
        };
    }

    public int bindAndSolve(final Solution<T> matchedSolution,
                            final TuplePattern<T> pattern,
                            final Tuple<T> tuple,
                            final int level,
                            final int tupleSize,
                            final Stack<Solution<T>> solutions) {
        if (level == tupleSize) {
            return 0;
        }

        // these solutions stay on the stack, but the current term is a variable which binds to previous solutions,
        // a cross-product of solutions will also be added to the stack
        int added = bindAndSolve(matchedSolution, pattern, tuple, level + 1, tupleSize, solutions);

        String var = pattern.getTerms()[level].getVariable();
        if (null == var) {
            return added;
        } else {
            int count = added;

            T value = tuple.getElements()[level];
            Iterator<Solution<T>> iter = getSolutions(var, value);
            if (null != iter) {
                // helper stack avoids random access into the solution stack
                // TODO: eliminate or re-use this
                Stack<Solution<T>> helper = new Stack<Solution<T>>();
                for (int i = 0; i < added; i++) {
                    helper.push(solutions.pop());
                }

                while (iter.hasNext()) {
                    Solution<T> ps = iter.next();
                    if (matchedSolution.complements(ps, queryVariables)) {
                        // create a copy of the mutable object provided by the iterator
                        solutions.push(new Solution<T>(ps));
                        count++;

                        for (Solution<T> lower : helper) {
                            if (ps.complements(lower, queryVariables)) {
                                Solution<T> lowerComposed = new Solution<T>(totalPatterns, ps, lower);
                                solutions.push(lowerComposed);
                                count++;
                            }
                        }
                    }
                }

                while (!helper.isEmpty()) {
                    // put the lower-level solution back on the solution stack.
                    // It is a candidate solution in is own right, before composition with the variable-bound
                    // solutions.
                    solutions.push(helper.pop());
                }
            }

            return count;
        }
    }

    public long removeExpiredSolutions(final long now) {
        int removedSolutions = 0;

        Collection<SolutionGroup<T>> toRemove = new LinkedList<SolutionGroup<T>>();

        for (Map.Entry<String, Map<T, GroupIndex<T>>> e : solutionsByBinding.entrySet()) {
            for (Map.Entry<T, GroupIndex<T>> e2 : e.getValue().entrySet()) {
                GroupIndex<T> gi = e2.getValue();
                for (Map.Entry<Long, SolutionGroup<T>> e3 : gi.groups.entrySet()) {
                    SolutionGroup<T> g = e3.getValue();
                    removedSolutions += g.removeExpired(now);
                    if (g.getSolutions().isNil()) {
                        toRemove.add(g);
                    }
                }
            }
        }

        for (SolutionGroup<T> g : toRemove) {
            VariableBindings<T> bindings = g.getBindings();
            for (Map.Entry<String, T> e : bindings.entrySet()) {
                GroupIndex<T> gi = getGroupIndex(e.getKey(), e.getValue());
                if (null != gi) {
                    gi.groups.remove(bindings.getHash());
                    if (0 == gi.groups.size()) {
                        Map<T, GroupIndex<T>> byVariable = solutionsByBinding.get(e.getKey());
                        byVariable.remove(e.getValue());
                        if (0 == byVariable.size()) {
                            solutionsByBinding.remove(e.getKey());
                        }
                    }
                }
            }
        }

        return removedSolutions;
    }

    private GroupIndex<T> getGroupIndex(final String variable, final T value) {
        Map<T, GroupIndex<T>> byVariable = solutionsByBinding.get(variable);
        if (null == byVariable) {
            return null;
        }

        return byVariable.get(value);
    }

    private static class GroupIndex<T> {
        private final Map<Long, SolutionGroup<T>> groups = new HashMap<Long, SolutionGroup<T>>();

        public void add(final Solution<T> ps,
                        final long now) {
            long h = ps.getBindings().getHash();
            SolutionGroup<T> g = groups.get(h);

            if (null == g) {
                // note: assumes that the bindings will not change externally
                g = new SolutionGroup<T>(ps.getBindings());
                groups.put(h, g);
            }

            g.add(ps, now);
        }
    }

    private static class SolutionIterator<T> implements Iterator<Solution<T>> {

        // note: solution is mutated on each call to next().  Read but do not store the object.
        private final Solution<T> solution = new Solution<T>(0, 0, null, 0);

        private final Iterator<SolutionGroup<T>> groupIterator;
        private LList<SolutionPattern> currentSolutions;
        private boolean advanced;

        // note: assumes a non-empty iterator of groups
        public SolutionIterator(final Iterator<SolutionGroup<T>> groupIterator) {
            this.groupIterator = groupIterator;
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
                currentSolutions = currentSolutions.getRest();
            }

            // note: the loop tolerates empty groups, although these should not occur
            while (null == currentSolutions || currentSolutions.isNil()) {
                if (groupIterator.hasNext()) {
                    SolutionGroup<T> g = groupIterator.next();
                    currentSolutions = g.getSolutions();
                    // update the bindings of the current solution whenever we enter a new group
                    solution.setBindings(g.getBindings());
                } else {
                    break;
                }
            }

            advanced = true;
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
