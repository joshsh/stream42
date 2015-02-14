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
 * A recursive data structure which indexes tuple-based queries and matches incoming tuples against
 * stored queries in a forward-chaining fashion.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryIndex<T> {
    private static final Logger logger = Logger.getLogger(QueryIndex.class.getName());

    // the "leaves" of this index
    private Set<Query.PatternInQuery<T>> patterns;
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
     * Returns this index to its original, empty state, removing all queries and all solutions
     */
    public synchronized void clear() {
        // note: no deconstruction.  Trusting Java GC.
        wildcardIndex = null;
        valueIndexes = null;
        patterns = null;

        rootMetadata.clearAll();
    }

    /**
     * Indexes a query for matching of future tuples
     *
     * @param query the query to index
     */
    public synchronized void add(final Query<T> query) {
        // generate a unique id for the pattern, and store it with the pattern
        String id = rootMetadata.add(query);
        query.setId(id);

        logger.fine("adding query " + id);

        for (Query.PatternInQuery<T> p : query.getPatterns()) {
            if (rootMetadata.tupleSize != p.getTerms().length) {
                throw new IllegalArgumentException("tuple pattern is not of expected length " + rootMetadata.tupleSize);
            }

            add(p, 0);
        }
    }

    /**
     * Removes a previously added query.
     * The given query will no longer match incoming tuples, and the memory it previously occupied will be freed.
     *
     * @param query the query to remove
     */
    public synchronized void remove(final Query<T> query) {
        logger.fine("removing query " + query.getId());

        if (rootMetadata.remove(query)) {
            for (Query.PatternInQuery<T> p : query.getPatterns()) {
                remove(p, 0);
            }
        } else {
            throw new IllegalArgumentException("no such query");
        }
    }

    /**
     * Renews a previously submitted query, applying a new time-to-live with respect to the current moment.
     * The query will expire only after its new expiration time has been reached.
     *
     * @param query the query to refresh
     * @param ttl   a time-to-live, in seconds. If the special value 0 is supplied, the query will never expire.
     * @param now   the current time, in milliseconds since the Unix epoch
     */
    public synchronized void renew(final Query<T> query, final int ttl, final long now) {
        // assign a new expiration time to the graph pattern
        query.setExpirationTime(0 == ttl ? 0 : now + 1000L * ttl);

        // re-order the queue of patterns
        rootMetadata.queries.remove(query);
        rootMetadata.queries.add(query);
    }

    /**
     * Consumes a tuple and matches it against any and all applicable queries,
     * possibly producing one or more solutions.
     *
     * @param tuple   the input tuple
     * @param handler a handler for any solutions produced
     * @param ttl     the time-to-live of the tuple and any derived solutions, in seconds.
     *                Use 0 for infinite time-to-live.
     * @param now     the current Unix time, in milliseconds (not seconds)
     * @return whether at least one tuple pattern is applied to the given tuple, changing the state of the query index.
     * If no tuple pattern is applied, the tuple simply passes through.
     */
    public synchronized boolean add(final T[] tuple,
                                    final SolutionHandler<T> handler,
                                    final int ttl,
                                    final long now) {
        if (rootMetadata.tupleSize != tuple.length) {
            throw new IllegalArgumentException("tuple is not of expected length " + rootMetadata.tupleSize);
        }

        long expirationTime = ttl > 0 ? now + 1000L * ttl : 0;

        // reset in case of previous exceptions
        rootMetadata.clearSolutionMetadata();

        // Calculate all new partial solutions and save them in a buffer until all matching tuple patterns have been
        // applied to the tuple.
        // Do not allow solutions created while processing one tuple pattern to be accessible while processing
        // the next.
        boolean changed = add(tuple, 0, rootMetadata, expirationTime);

        produceSolutions(handler, rootMetadata, now);

        return changed;
    }

    /**
     * Enforce query and tuple time-to-live by removing expired graph patterns and solutions.
     * The latter is a space-saving operation which has no effect on query evaluation;
     * it forces all expired solutions to be removed at once, rather than merely becoming irrelevant
     * and being gradually removed upon discovery.
     *
     * @param now the current time, in milliseconds since the Unix epoch
     */
    public synchronized void removeExpired(final long now) {
        long startTime = System.currentTimeMillis();
        int removedQueries = 0;
        int removedSolutions = 0;

        // identify expired queries
        Collection<Query<T>> toRemove = new LinkedList<Query<T>>();
        for (Query<T> query : rootMetadata.queries) {
            if (query.isExpired(now)) {
                toRemove.add(query);
            }
        }
        // remove expired queries
        for (Query<T> query : toRemove) {
            remove(query);
            removedQueries++;
        }

        // for the remaining queries, remove expired solutions
        for (Query<T> query : rootMetadata.queries) {
            SolutionIndex<T> index = query.getSolutionIndex();
            removedSolutions += index.removeExpired(now);
        }

        if (removedQueries > 0 || removedSolutions > 0) {
            long endTime = System.currentTimeMillis();
            logger.info("removed " + removedQueries + " queries and "
                    + removedSolutions + " solutions in " + (endTime - startTime) + "ms");
        }
    }

    private void add(final Query.PatternInQuery<T> pattern,
                     final int level) {
        if (pattern.getTerms().length == level) {
            if (null == patterns) {
                patterns = new HashSet<Query.PatternInQuery<T>>();
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

    private boolean remove(final Query.PatternInQuery<T> pattern,
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

    private boolean add(final T[] tuple,
                        final int level,
                        final RootMetadata<T> meta,
                        final long expirationTime) {
        boolean changed = false;

        if (meta.tupleSize == level) {
            for (Query.PatternInQuery<T> pattern : patterns) {
                add(pattern, tuple, meta, expirationTime);
                changed = true;
            }
        } else {
            if (null != wildcardIndex) {
                changed = wildcardIndex.add(tuple, level + 1, meta, expirationTime);
            }

            if (null != valueIndexes) {
                QueryIndex<T> idx = valueIndexes.get(tuple[level]);
                if (null != idx) {
                    changed |= idx.add(tuple, level + 1, meta, expirationTime);
                }
            }
        }

        return changed;
    }

    private void add(final Query.PatternInQuery<T> pattern,
                     final T[] tuple,
                     final RootMetadata<T> meta,
                     final long expirationTime) {
        Map<String, T> bindings = new HashMap<String, T>();
        for (int i = 0; i < meta.tupleSize; i++) {
            String v = pattern.getTerms()[i].getVariable();
            if (null != v) {
                bindings.put(v, tuple[i]);
            }
        }
        Bindings<T> b = new Bindings<T>(bindings, pattern.getQuery().getVariables());

        // create a new partial solution
        Solution<T> solutionMatched = new Solution<T>(
                pattern.getQuery().getPatterns().size(), pattern.getIndex(), b, expirationTime);

        TuplePatternSolutions<T> ts = bindAndSolve(solutionMatched, pattern, tuple, meta);
        meta.tpSolutions.add(ts);
    }

    private TuplePatternSolutions<T> bindAndSolve(final Solution<T> matchedSolution,
                                                  final Query.PatternInQuery<T> pattern,
                                                  final T[] tuple,
                                                  final RootMetadata<T> meta) {
        TuplePatternSolutions<T> ts = new TuplePatternSolutions<T>();
        ts.pattern = pattern;
        ts.solutions = new Stack<Solution<T>>();

        meta.helperStack.clear();

        pattern.getQuery().getSolutionIndex().bindAndSolve(
                matchedSolution, pattern.getTerms(), tuple, meta.helperStack);

        Query.QueryVariables vars = pattern.getQuery().getVariables();
        int totalPatterns = pattern.getQuery().getPatterns().size();
        for (Solution<T> ps : meta.helperStack) {
            if (matchedSolution.composableWith(ps, vars)) {
                ts.solutions.push(new Solution<T>(totalPatterns, matchedSolution, ps));
            }
        }

        // add the original partial solution.  It will only be indexed if it is not contained within some other
        // solution produced in this phase.
        ts.solutions.push(matchedSolution);

        // prepare for the solution-production phase
        pattern.getQuery().getSolutionHashes().clear();

        return ts;
    }

    private void produceSolutions(final SolutionHandler<T> handler,
                                  final RootMetadata<T> meta,
                                  final long now) {
        for (TuplePatternSolutions<T> ts : meta.tpSolutions) {
            SolutionIndex<T> solutionIndex = ts.pattern.getQuery().getSolutionIndex();
            Set<Long> hashes = ts.pattern.getQuery().getSolutionHashes();

            for (Solution<T> ps : ts.solutions) {
                // if the join is a complete solution, handle it as such,
                // provided that an identical solution has not already been produced in response to the current tuple.
                if (ps.isComplete()) {
                    long hash = ps.getBindings().getHash();
                    if (hashes.contains(hash)) {
                        continue;
                    }
                    hashes.add(hash);

                    handler.handle(ts.pattern.getQuery().getId(), ps.getBindings());
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
        /**
         * Consumes a solution represented by a set of bindings, associated with the id of the graph pattern
         *
         * @param id       the automatically generated id of the graph pattern (query) to which the bindings are a solution
         * @param bindings the solution, which maps string-based keys to values
         */
        void handle(String id, Bindings<T> bindings);
    }

    private static class TuplePatternSolutions<T> {
        public Stack<Solution<T>> solutions;
        public Query.PatternInQuery<T> pattern;
    }

    private static class RootMetadata<T> {
        private final int tupleSize;
        private final PriorityQueue<Query<T>> queries = new PriorityQueue<Query<T>>();
        private final Collection<TuplePatternSolutions<T>> tpSolutions = new LinkedList<TuplePatternSolutions<T>>();
        private final Stack<Solution<T>> helperStack = new Stack<Solution<T>>();
        private int patternCount = 0;

        private RootMetadata(final int tupleSize) {
            this.tupleSize = tupleSize;
        }

        public void clearAll() {
            clearSolutionMetadata();
            queries.clear();
        }

        public void clearSolutionMetadata() {
            tpSolutions.clear();
            helperStack.clear();
        }

        public String add(final Query<T> query) {
            queries.add(query);
            return "gp" + (++patternCount);
        }

        public boolean remove(final Query<T> query) {
            return queries.remove(query);
        }
    }

}
