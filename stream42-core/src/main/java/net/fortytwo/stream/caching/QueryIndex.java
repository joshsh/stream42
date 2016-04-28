package net.fortytwo.stream.caching;

import net.fortytwo.stream.StreamProcessor;
import net.fortytwo.stream.model.VariableOrConstant;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Stack;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

/**
 * A recursive data structure which indexes tuple-based queries and matches incoming tuples against
 * stored queries in an incremental fashion.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryIndex<T, C> {
    private static final Logger logger = Logger.getLogger(QueryIndex.class.getName());

    // the "leaves" of this index
    private Set<Query.PatternInQuery<T, C>> patterns;
    // child indices matching specific values
    private Map<T, QueryIndex<T, C>> valueIndexes;
    // child index matching any value
    private QueryIndex<T, C> wildcardIndex;

    // only the root index has or needs these fields
    private final RootMetadata<T, C> rootMetadata;

    /**
     * Constructs a new query index for tuples of a given size
     *
     * @param tupleSize the size of tuples to index and match.
     *                  For RDF, an appropriate tuple size is 3, or 4 for RDF with Named Graphs
     */
    public QueryIndex(final int tupleSize) {
        rootMetadata = new RootMetadata<>(tupleSize);
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

        rootMetadata.clear();
    }

    /**
     * Indexes a query for matching of future tuples
     *
     * @param query the query to index
     */
    public synchronized void add(final Query<T, C> query) {
        rootMetadata.add(query);

        logger.fine("adding query " + query.getSubscription());

        for (Query.PatternInQuery<T, C> p : query.getPatterns()) {
            if (rootMetadata.tupleSize != p.getPattern().length) {
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
    public synchronized void remove(final Query<T, C> query) {
        logger.fine("removing query " + query.getSubscription());

        if (rootMetadata.remove(query)) {
            for (Query.PatternInQuery<T, C> p : query.getPatterns()) {
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
    public synchronized void renew(final Query<T, C> query, final int ttl, final long now) {
        // assign a new expiration time to the graph pattern
        query.setExpirationTime(StreamProcessor.INFINITE_TTL == ttl ? 0 : now + 1000L * ttl);

        // re-order the queue of patterns
        rootMetadata.queries.remove(query);
        rootMetadata.queries.add(query);
    }

    /**
     * Consumes a tuple and matches it against any and all applicable queries,
     * possibly producing one or more solutions.
     * Note: this method is thread-safe; any number of threads may add tuples concurrently.
     *
     * @param tuple   the input tuple
     * @param handler a handler for any solutions produced
     * @param ttl     the time-to-live of the tuple and any derived solutions, in seconds.
     *                Use 0 for infinite time-to-live.
     * @param now     the current Unix time, in milliseconds (not seconds)
     * @return whether at least one tuple pattern is applied to the given tuple, changing the state of the query index.
     * If no tuple pattern is applied, the tuple simply passes through.
     */
    public boolean add(final T[] tuple,
                       final BiConsumer<C, Bindings<T>> handler,
                       final int ttl,
                       final long now) {
        if (rootMetadata.tupleSize != tuple.length) {
            throw new IllegalArgumentException("tuple is not of expected length " + rootMetadata.tupleSize);
        }

        long expirationTime = StreamProcessor.INFINITE_TTL == ttl ? StreamProcessor.NEVER_EXPIRE : now + 1000L * ttl;

        return add(tuple, rootMetadata, handler, expirationTime, now, 0);
    }

    /**
     * Enforce query and tuple time-to-live by removing expired graph patterns and solutions.
     * The latter is a space-saving operation which has no effect on query evaluation;
     * it forces all expired solutions to be removed at once, rather than merely becoming irrelevant
     * and being gradually removed upon discovery.
     *
     * @param now the current time, in milliseconds since the Unix epoch
     */
    public void removeExpired(final long now) {
        long startTime = System.currentTimeMillis();
        int removedSolutions = 0;

        int removedQueries = removeExpiredQueries(now);

        // for the remaining queries, remove expired solutions
        for (Query<T, C> query : rootMetadata.queries) {
            SolutionIndex<T> index = query.getSolutionIndex();
            removedSolutions += index.removeExpired(now);
        }

        if (removedQueries > 0 || removedSolutions > 0) {
            long endTime = System.currentTimeMillis();
            logger.info("removed " + removedQueries + " queries and "
                    + removedSolutions + " solutions in " + (endTime - startTime) + "ms");
        }
    }

    private synchronized int removeExpiredQueries(final long now) {
        int removedQueries = 0;

        // identify expired queries
        Collection<Query<T, C>> toRemove = new LinkedList<>();
        for (Query<T, C> query : rootMetadata.queries) {
            if (query.isExpired(now)) {
                toRemove.add(query);
            }
        }
        // remove expired queries
        for (Query<T, C> query : toRemove) {
            remove(query);
            removedQueries++;
        }

        return removedQueries;
    }

    private void add(final Query.PatternInQuery<T, C> pattern,
                     final int level) {
        if (pattern.getPattern().length == level) {
            if (null == patterns) {
                patterns = new HashSet<>();
            }

            patterns.add(pattern);
        } else {
            VariableOrConstant<String, T> term = pattern.getPattern()[level];
            String var = term.getVariable();
            if (null == var) {
                if (null == valueIndexes) {
                    valueIndexes = new HashMap<>();
                }
                QueryIndex<T, C> idx = valueIndexes.get(term.getConstant());
                if (null == idx) {
                    idx = new QueryIndex<>();
                    valueIndexes.put(term.getConstant(), idx);
                }
                idx.add(pattern, level + 1);
            } else {
                if (null == wildcardIndex) {
                    wildcardIndex = new QueryIndex<>();
                }
                wildcardIndex.add(pattern, level + 1);
            }
        }
    }

    private boolean remove(final Query.PatternInQuery<T, C> pattern,
                           final int level) {
        if (pattern.getPattern().length == level) {
            patterns.remove(pattern);

            return 0 == patterns.size();
        } else {
            VariableOrConstant<String, T> term = pattern.getPattern()[level];
            String var = term.getVariable();
            if (null == var) {
                QueryIndex<T, C> idx = valueIndexes.get(term.getConstant());
                if (idx.remove(pattern, level + 1)) {
                    valueIndexes.remove(term.getConstant());
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
                        final RootMetadata<T, C> meta,
                        final BiConsumer<C, Bindings<T>> handler,
                        final long expirationTime,
                        final long now,
                        final int level) {
        boolean changed = false;

        if (meta.tupleSize == level) {
            for (Query.PatternInQuery<T, C> pattern : patterns) {
                Stack<Solution<T>> solutions = new Stack<>();

                Bindings<T> b = pattern.getQuery().getVariables().bind(pattern.getPattern(), tuple);

                // create a new partial solution
                Solution<T> matchedSolution = new Solution<>(
                        pattern.getQuery().getPatterns().size(), pattern.getIndex(), b, expirationTime);

                pattern.getQuery().getSolutionIndex().joinSolutions(
                        matchedSolution, b, solutions, now);

                // Immediately add solutions to the solution index.
                // As a side-effect, these solutions are available when subsequent patterns of the same query
                // are applied to the same tuple.
                for (Solution<T> s : solutions) {
                    // Add both complete and incomplete solutions to the solution index.
                    // The awareness of complete solutions potentially saves work, as contained solutions are abandoned.
                    boolean added = pattern.getQuery().getSolutionIndex().add(s, now);
                    if (added && s.isComplete()) {
                        // if the join is a complete solution, handle it as such, provided that it is unique.
                        handler.accept(pattern.getQuery().getSubscription(), s.getBindings());
                    }
                    changed |= added;
                }
            }
        } else {
            if (null != wildcardIndex) {
                changed = wildcardIndex.add(tuple, meta, handler, expirationTime, now, level + 1);
            }

            if (null != valueIndexes) {
                QueryIndex<T, C> idx = valueIndexes.get(tuple[level]);
                if (null != idx) {
                    changed |= idx.add(tuple, meta, handler, expirationTime, now, level + 1);
                }
            }
        }

        return changed;
    }

    private static class RootMetadata<T, C> {
        private final int tupleSize;
        private final PriorityQueue<Query<T, C>> queries = new PriorityQueue<>();

        private RootMetadata(final int tupleSize) {
            this.tupleSize = tupleSize;
        }

        public void clear() {
            queries.clear();
        }

        public void add(final Query<T, C> query) {
            queries.add(query);
        }

        public boolean remove(final Query<T, C> query) {
            return queries.remove(query);
        }
    }

}
