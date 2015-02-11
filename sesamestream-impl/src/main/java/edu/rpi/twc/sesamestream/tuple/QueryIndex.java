package edu.rpi.twc.sesamestream.tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Stack;
import java.util.logging.Logger;

/**
 * A recursive data structure which indexes tuple-based graph patterns and matches incoming tuples against
 * stored patterns in a forward-chaining fashion.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryIndex<T> {
    private static final Logger logger = Logger.getLogger(QueryIndex.class.getName());

    // the "leaves" of this index
    private Set<TuplePattern<T>> patterns;
    // child indices matching specific values
    private Map<T, QueryIndex<T>> valueIndexes;
    // child index matching any value
    private QueryIndex<T> wildcardIndex;

    // only the root index has or needs these fields
    private final RootMetadata<T> rootMetadata;

    /**
     * Constructs a new query index for tuples of a given size
     *
     * @param tupleSize the size of tuples to index and match.
     *                  For RDF, an appropriate tuple size is 3, or 4 for RDF with Named Graphs
     */
    public QueryIndex(final int tupleSize) {
        rootMetadata = new RootMetadata<T>(tupleSize);
    }

    /**
     * Constructs an internal/subordinate index
     */
    private QueryIndex() {
        rootMetadata = null;
    }

    /**
     * Returns this index to its original, empty state
     */
    public synchronized void clear() {
        // note: no deconstruction.  Trusting Java GC
        wildcardIndex = null;
        valueIndexes = null;
        patterns = null;

        rootMetadata.clearAll();
    }

    /**
     * Indexes a graph pattern for matching of future tuples
     *
     * @param graphPattern the pattern to index
     */
    public synchronized void add(final GraphPattern<T> graphPattern) {
        // generate a unique id for the pattern, and store it with the pattern
        String id = rootMetadata.add(graphPattern);
        graphPattern.setId(id);

        for (TuplePattern<T> p : graphPattern.getPatterns()) {
            if (rootMetadata.tupleSize != p.getTerms().length) {
                throw new IllegalArgumentException("tuple pattern is not of expected length " + rootMetadata.tupleSize);
            }

            add(p, 0);
        }
    }

    /**
     * Removes a previously added graph pattern.
     * The given pattern will no longer match incoming tuples, and the memory it previously occupied will be freed.
     *
     * @param graphPattern the pattern to remove
     */
    public synchronized void remove(final GraphPattern<T> graphPattern) {
        if (rootMetadata.remove(graphPattern)) {
            for (TuplePattern<T> p : graphPattern.getPatterns()) {
                remove(p, 0);
            }
        } else {
            throw new IllegalArgumentException("no such query");
        }
    }

    public synchronized void renew(final GraphPattern<T> graphPattern, final long ttl, final long now) {
        // assign a new expiration time to the graph pattern
        graphPattern.setExpirationTime(0 == ttl ? 0 : now + ttl);

        // re-order the queue of patterns
        rootMetadata.graphPatterns.remove(graphPattern);
        rootMetadata.graphPatterns.add(graphPattern);
    }

    /**
     * Consumes a tuple and matches it against any and all applicable graph patterns,
     * possibly producing one or more solutions.
     *
     * @param tuple   the input tuple
     * @param handler a handler for any solutions produced
     * @return whether at least one tuple pattern is applied to the given tuple, changing the state of the query index.
     * If no tuple pattern is applied, the tuple simply passes through.
     */
    public synchronized boolean match(final Tuple<T> tuple,
                                      final SolutionHandler<T> handler,
                                      final long ttl,
                                      final long now) {
        if (rootMetadata.tupleSize != tuple.getElements().length) {
            throw new IllegalArgumentException("tuple is not of expected length " + rootMetadata.tupleSize);
        }

        long expirationTime = ttl > 0 ? now + ttl : 0;

        // reset in case of previous exceptions
        rootMetadata.clearSolutionMetadata();

        // Calculate all new partial solutions and save them in a buffer until all matching tuple patterns have been
        // applied to the tuple.
        // Do not allow solutions created while processing one tuple pattern to be accessible while processing
        // the next.
        boolean matched = match(tuple, 0, rootMetadata, expirationTime);

        produceSolutions(handler, rootMetadata, now);

        return matched;
    }

    /**
     * Enforce query and tuple time-to-live by removing expired graph patterns and solutions.
     * The latter is a space-saving operation which has no effect on query evaluation;
     * it forces all expired solutions to be removed at once, rather than merely becoming irrelevant
     * and being gradually removed upon discovery.
     */
    public synchronized void removeExpired(final long now) {
        long startTime = System.currentTimeMillis();
        int removedQueries = 0;
        int removedSolutions = 0;

        Collection<GraphPattern<T>> toRemove = new LinkedList<GraphPattern<T>>();
        for (GraphPattern<T> gp : rootMetadata.graphPatterns) {
            if (gp.isExpired(now)) {
                toRemove.add(gp);
            }
        }
        for (GraphPattern<T> gp : toRemove) {
            remove(gp);
            removedQueries++;
        }

        for (GraphPattern<T> gp : rootMetadata.graphPatterns) {
            SolutionIndex<T> index = gp.getSolutionIndex();
            removedSolutions += index.removeExpiredSolutions(now);
        }

        if (removedQueries > 0 || removedSolutions > 0) {
            long endTime = System.currentTimeMillis();
            logger.info("removed " + removedQueries + " queries and "
                    + removedSolutions + " solutions in " + (endTime - startTime) + "ms");
        }
    }

    private void add(final TuplePattern<T> pattern,
                     final int level) {
        if (pattern.getTerms().length == level) {
            if (null == patterns) {
                patterns = new HashSet<TuplePattern<T>>();
            }

            patterns.add(pattern);
        } else {
            Term<T> term = pattern.getTerms()[level];
            String var = term.getVariable();
            if (null == var) {
                if (null == valueIndexes) {
                    valueIndexes = new HashMap<T, QueryIndex<T>>();
                }
                QueryIndex<T> idx = valueIndexes.get(term.getValue());
                if (null == idx) {
                    idx = new QueryIndex<T>();
                    valueIndexes.put(term.getValue(), idx);
                }
                idx.add(pattern, level + 1);
            } else {
                if (null == wildcardIndex) {
                    wildcardIndex = new QueryIndex<T>();
                }
                wildcardIndex.add(pattern, level + 1);
            }
        }
    }

    private boolean remove(final TuplePattern<T> pattern,
                           final int level) {
        if (pattern.getTerms().length == level) {
            patterns.remove(pattern);

            return 0 == patterns.size();
        } else {
            Term<T> term = pattern.getTerms()[level];
            String var = term.getVariable();
            if (null == var) {
                QueryIndex<T> idx = valueIndexes.get(term.getValue());
                if (idx.remove(pattern, level + 1)) {
                    valueIndexes.remove(term.getValue());
                }
            } else {
                if (wildcardIndex.remove(pattern, level + 1)) {
                    wildcardIndex = null;
                }
            }

            return null == wildcardIndex && (null == valueIndexes || 0 == valueIndexes.size());
        }
    }

    private boolean match(final Tuple<T> tuple,
                          final int level,
                          final RootMetadata<T> meta,
                          final long expirationTime) {
        boolean matched = false;

        if (meta.tupleSize == level) {
            for (TuplePattern<T> pattern : patterns) {
                match(pattern, tuple, meta, expirationTime);
                matched = true;
            }
        } else {
            if (null != wildcardIndex) {
                matched = wildcardIndex.match(tuple, level + 1, meta, expirationTime);
            }

            if (null != valueIndexes) {
                QueryIndex<T> idx = valueIndexes.get(tuple.getElements()[level]);
                if (null != idx) {
                    matched |= idx.match(tuple, level + 1, meta, expirationTime);
                }
            }
        }

        return matched;
    }

    private void match(final TuplePattern<T> pattern,
                       final Tuple<T> tuple,
                       final RootMetadata<T> meta,
                       final long expirationTime) {
        Map<String, T> bindings = new HashMap<String, T>();
        for (int i = 0; i < meta.tupleSize; i++) {
            String v = pattern.getTerms()[i].getVariable();
            if (null != v) {
                bindings.put(v, tuple.getElements()[i]);
            }
        }
        VariableBindings<T> b = new VariableBindings<T>(bindings, pattern.getGraphPattern().getVariables());

        // create a new partial solution
        Solution<T> solutionMatched = new Solution<T>(
                pattern.getGraphPattern().getPatterns().size(), pattern.getIndex(), b, expirationTime);

        TuplePatternSolutions<T> ts = bindAndSolve(solutionMatched, pattern, tuple, meta);
        meta.tpSolutions.add(ts);
    }

    private TuplePatternSolutions<T> bindAndSolve(final Solution<T> matchedSolution,
                                                  final TuplePattern<T> pattern,
                                                  final Tuple<T> tuple,
                                                  final RootMetadata<T> meta) {
        TuplePatternSolutions<T> ts = new TuplePatternSolutions<T>();
        ts.pattern = pattern;
        ts.solutions = new Stack<Solution<T>>();

        meta.helperStack.clear();

        pattern.getGraphPattern().getSolutionIndex().bindAndSolve(
                matchedSolution, pattern, tuple, 0, meta.tupleSize, meta.helperStack);

        GraphPattern.QueryVariables vars = pattern.getGraphPattern().getVariables();
        int totalPatterns = pattern.getGraphPattern().getPatterns().size();
        for (Solution<T> ps : meta.helperStack) {
            if (matchedSolution.complements(ps, vars)) {
                ts.solutions.push(new Solution<T>(totalPatterns, matchedSolution, ps));
            }
        }

        // add the original partial solution.  It will only be indexed if it is not contained within some other
        // solution produced in this phase.
        ts.solutions.push(matchedSolution);

        // prepare for the solution-production phase
        pattern.getGraphPattern().getSolutionHashes().clear();

        return ts;
    }

    private void produceSolutions(final SolutionHandler<T> handler,
                                  final RootMetadata<T> meta,
                                  final long now) {
        for (TuplePatternSolutions<T> ts : meta.tpSolutions) {
            SolutionIndex<T> solutionIndex = ts.pattern.getGraphPattern().getSolutionIndex();
            Set<Long> hashes = ts.pattern.getGraphPattern().getSolutionHashes();

            for (Solution<T> ps : ts.solutions) {
                // if the join is a complete solution, handle it as such,
                // provided that an identical solution has not already been produced in response to the current tuple.
                if (ps.isComplete()) {
                    long hash = ps.getBindings().getHash();
                    if (hashes.contains(hash)) {
                        continue;
                    }
                    hashes.add(hash);

                    handler.handle(ts.pattern.getGraphPattern().getId(), ps.getBindings());
                }

                solutionIndex.add(ps, now);
            }
        }

        meta.clearSolutionMetadata();
    }

    /**
     * A handler for candidate solutions.  These potentially become SPARQL solutions after filtering and projection.
     */
    public static interface SolutionHandler<T> {
        void handle(String id, VariableBindings<T> bindings);
    }

    private static class TuplePatternSolutions<T> {
        public Stack<Solution<T>> solutions;
        public TuplePattern<T> pattern;
    }

    private static class RootMetadata<T> {
        private final int tupleSize;
        private final PriorityQueue<GraphPattern<T>> graphPatterns = new PriorityQueue<GraphPattern<T>>();
        private final Collection<TuplePatternSolutions<T>> tpSolutions = new LinkedList<TuplePatternSolutions<T>>();
        private final Stack<Solution<T>> helperStack = new Stack<Solution<T>>();
        private int patternCount = 0;

        private RootMetadata(final int tupleSize) {
            this.tupleSize = tupleSize;
        }

        public void clearAll() {
            clearSolutionMetadata();
            graphPatterns.clear();
        }

        public void clearSolutionMetadata() {
            tpSolutions.clear();
            helperStack.clear();
        }

        public String add(final GraphPattern<T> graphPattern) {
            graphPatterns.add(graphPattern);
            return "gp" + (++patternCount);
        }

        public boolean remove(final GraphPattern<T> graphPattern) {
            return graphPatterns.remove(graphPattern);
        }
    }
}
