package net.fortytwo.stream.shj;

import net.fortytwo.stream.model.VariableOrConstant;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * An index for tuple queries and their solutions
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryIndex<K, V> implements Index<Query<K, V>> {

    private static final Logger logger = Logger.getLogger(QueryIndex.class.getName());

    private final QueryContext<K, V> queryContext;

    private QueryIndex<K, V>[] variableIndices;
    private Map<V, QueryIndex<K, V>> constantIndices;
    private SolutionIndex<V> solutionIndex;

    private final Set<Query<K, V>> queries;

    protected QueryIndex() {
        queryContext = null;
        queries = null;
    }

    public QueryIndex(QueryContext<K, V> queryContext) {
        this.queryContext = queryContext;
        this.queries = new HashSet<>();
    }

    // note: we manually break up cyclical references for the benefit of the garbage collector
    public synchronized void clear() {
        if (null != variableIndices) {
            for (int i = 0; i < variableIndices.length; i++) {
                QueryIndex<K, V> index = variableIndices[i];
                if (null != index) {
                    index.clear();

                    variableIndices[i] = null;
                }
            }
            variableIndices = null;
        }

        if (null != constantIndices) {
            for (QueryIndex<K, V> index : constantIndices.values()) {
                index.clear();
            }
            constantIndices.clear();
            constantIndices = null;
        }

        if (null != solutionIndex) {
            solutionIndex.clear();
            solutionIndex = null;
        }

        if (null != queries) {
            queries.clear();
        }
        if (null != queryContext) {
            queryContext.clear();
        }
    }

    // block on writing to the query index itself. This does not interrupt tuple processing.
    public synchronized void add(Query<K, V> query) {
        if (null == query) {
            throw new IllegalArgumentException("null query");
        }

        if (!query.getGraphPattern().isFullyConnected()) {
            throw new IllegalArgumentException("graph pattern is not fully connected");
        }

        queryContext.evictExpired();

        boolean success = false;
        try {
            // We preserve the order of consumers for easy inspection,
            // although the order plays no role in query evaluation.
            List<JoinHelper<K, V>> wrappers = new LinkedList<>();

            GraphPattern<K, V> graphPattern = query.getGraphPattern();
            for (TuplePattern<K, V> tuplePattern : graphPattern.getPatterns()) {
                Map<K, Integer> indexByKey = new HashMap<>();
                wrappers.add(addTuplePattern(tuplePattern, indexByKey, 0, queryContext));
            }

            query.setAllHelpers(wrappers);
            queryContext.getQueryExpirationManager().notifyFinishedAdding();

            success = true;
        } finally {
            if (!success) {
                logger.warning("query uncleanly added: " + query);
            } else {
                queries.add(query);
            }
        }
    }

    // block on writing to the query index itself. This does not interrupt tuple processing.
    public synchronized boolean remove(Query<K, V> query) {
        if (queries.remove(query)) {
            boolean success = false;
            try {
                boolean allRemoved = true;
                for (TuplePattern<K, V> tuplePattern : query.getGraphPattern().getPatterns()) {
                    Map<K, Integer> indexByKey = new HashMap<>();
                    if (!removeTuplePattern(tuplePattern, 0, indexByKey)) {
                        logger.warning("failed to remove tuple pattern " + tuplePattern + " of query " + query);
                        allRemoved = false;
                        // even in case of failure, attempt to remove any remaining patterns
                    }
                }
                success = allRemoved;
                return success;
            } finally {
                if (!success) {
                    logger.warning("query uncleanly removed: " + query);
                }
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean isEmpty() {
        return null == solutionIndex && null == variableIndices && null == constantIndices;
    }

    /**
     * Note: the provided tuple is modified in the process of indexing
     * Note: thread-safe without synchronization; we only read from this index,
     * although we may write to solution indices
     *
     * @param tuple          the tuple to index
     * @param expirationTime the expiration time of the tuple, in milliseconds since the Unix epoch
     * @return whether the tuple was added to any solution index
     */
    public boolean add(V[] tuple, long expirationTime) {
        evictExpired();

        boolean success = false;
        try {
            V[] values = (V[]) new Object[tuple.length];

            boolean ret = addTuple(tuple, values, 0, 0, expirationTime);

            if (ret) {
                queryContext.getSolutionExpirationManager().notifyFinishedAdding();
            }

            success = true;
            return ret;
        } finally {
            if (!success) {
                logger.warning("tuple uncleanly added");
            }
        }
    }

    /**
     * Removes all matching tuples from solution indices.
     *
     * @param tuple a tuple in which any element may be null, representing a wildcard
     * @return whether any matching tuples were found and removed
     */
    public boolean remove(V[] tuple) {
        evictExpired();

        boolean success = false;
        try {
            V[] values = (V[]) new Object[tuple.length];

            boolean ret = removeTuple(tuple, values, 0, 0);
            success = true;
            return ret;
        } finally {
            if (!success) {
                logger.warning("tuple pattern uncleanly removed");
            }
        }
    }

    private void evictExpired() {
        QueryContext<K, V> queryContextSafe = queryContext;
        if (null != queryContextSafe) {
            queryContextSafe.evictExpired();
        }
    }

    // There is one solution index per tuple pattern, ignoring variable names
    // For simplicity, we block entirely on writing so as to avoid race conditions.
    // Addition of tuple patterns is expected to be infrequent, so multi-threading is unimportant.
    private synchronized JoinHelper<K, V> addTuplePattern(TuplePattern<K, V> tuplePattern,
                                                                Map<K, Integer> indexByKey,
                                                                int depth,
                                                                QueryContext<K, V> queryContext) {
        if (depth == tuplePattern.getLength()) {
            if (null == solutionIndex) {
                solutionIndex = new SolutionIndex<>(queryContext, indexByKey.size());
            }

            JoinHelper<K, V> helper = new JoinHelper<>(solutionIndex, indexByKey);
            tuplePattern.setJoinHelper(helper);
            solutionIndex.getConsumerIndex().add(helper);
            return helper;
        } else {
            VariableOrConstant<K, V> el = tuplePattern.getPattern()[depth];
            QueryIndex<K, V> queryIndex;
            K variable = el.getVariable();
            if (null != variable) {
                if (null == variableIndices) {
                    variableIndices = new QueryIndex[depth + 1];
                }
                Integer index = indexByKey.get(variable);

                // offset of query index is 0 if this is a new variable, otherwise > 0,
                // pointing to the previous occurrence
                int offset;
                if (null == index) {
                    offset = 0;
                    indexByKey.put(variable, indexByKey.size());
                } else {
                    offset = depth - index;
                }

                queryIndex = variableIndices[offset];
                if (null == queryIndex) {
                    queryIndex = new QueryIndex<>();
                    variableIndices[offset] = queryIndex;
                }
            } else {
                V constant = el.getConstant();
                if (null == constantIndices) {
                    // thread-safe for reading
                    constantIndices = new HashMap<>();
                }
                queryIndex = constantIndices.get(constant);
                if (null == queryIndex) {
                    queryIndex = new QueryIndex<>();
                    constantIndices.put(constant, queryIndex);
                }
            }

            return queryIndex.addTuplePattern(tuplePattern, indexByKey, depth + 1, queryContext);
        }
    }

    private boolean removeTuplePattern(TuplePattern<K, V> tuplePattern, int depth, Map<K, Integer> indexByKey) {
        boolean removed;

        if (depth == tuplePattern.getLength()) {
            if (null == solutionIndex) {
                throw new IllegalStateException();
            }

            if (!solutionIndex.getConsumerIndex().remove(tuplePattern.getJoinHelper())) {
                throw new IllegalStateException();
            }

            if (solutionIndex.getConsumerIndex().isEmpty()) {
                solutionIndex = null;
            }

            removed = true;
        } else {
            VariableOrConstant<K, V> vc = tuplePattern.getPattern()[depth];
            K variable = vc.getVariable();
            if (null != variable) {
                if (null == variableIndices) {
                    throw new IllegalStateException();
                }

                Integer i = indexByKey.get(variable);
                int offset;
                if (null == i) {
                    offset = 0;
                    indexByKey.put(variable, indexByKey.size());
                } else {
                    offset = depth - i;
                }

                QueryIndex<K, V> index = variableIndices[offset];
                if (null == index) {
                    throw new IllegalStateException();
                }

                removed = index.removeTuplePattern(tuplePattern, depth + 1, indexByKey);

                if (removed) {
                    if (index.isEmpty()) {
                        variableIndices[offset] = null;
                        boolean nonEmpty = false;
                        for (QueryIndex<K, V> ix : variableIndices) {
                            if (null != ix) {
                                nonEmpty = true;
                                break;
                            }
                        }

                        if (!nonEmpty) {
                            variableIndices = null;
                        }
                    }
                }
            } else {
                V constant = vc.getConstant();
                QueryIndex<K, V> index = constantIndices.get(constant);
                if (null == index) {
                    throw new IllegalStateException();
                }

                removed = index.removeTuplePattern(tuplePattern, depth + 1, indexByKey);
                if (removed) {
                    if (index.isEmpty()) {
                        constantIndices.remove(constant);
                        if (constantIndices.isEmpty()) {
                            constantIndices = null;
                        }
                    }
                }
            }
        }

        return removed;
    }

    // non-blocking, although individual solution indices may block on writing
    private boolean addTuple(V[] tuple, V[] values, int tupleDepth, int variableDepth, long expirationTime) {
        boolean added = false;

        // Note: it is possible for a shorter tuple pattern to match a longer tuple;
        // the rest of the tuple is ignored in this case, matched by default.
        SolutionIndex<V> solutionIndexSafe = solutionIndex;
        if (null != solutionIndexSafe) {
            int card = solutionIndexSafe.getCardinality();
            V[] valuesCopy = (V[]) new Object[card];
            System.arraycopy(values, 0, valuesCopy, 0, card);

            Solution<V> solution = new Solution<>(valuesCopy, expirationTime, solutionIndexSafe);
            // there is a solution index at every leaf node
            solutionIndexSafe.add(solution);
            added = true;
        }

        // Note: is possible for a tuple to be shorter than all otherwise matching tuple patterns;
        // no solutions are generated in this case, as the tail of the patterns don't match.
        if (tupleDepth != tuple.length) {
            V value = tuple[tupleDepth];

            QueryIndex<K, V>[] variableIndicesSafe = variableIndices;
            if (null != variableIndicesSafe) {
                for (int offset = 0; offset < variableIndicesSafe.length; offset++) {
                    QueryIndex<K, V> queryIndex = variableIndicesSafe[offset];
                    if (null != queryIndex) {
                        // filter in case of repeated variables
                        if (offset > 0) {
                            if (!value.equals(tuple[tupleDepth - offset])) continue;

                            added |= queryIndex.addTuple(
                                    tuple, values, tupleDepth + 1, variableDepth, expirationTime);
                        } else {
                            values[variableDepth] = value;
                            added |= queryIndex.addTuple(
                                    tuple, values, tupleDepth + 1, variableDepth + 1, expirationTime);
                        }
                    }
                }
            }

            Map<V, QueryIndex<K, V>> constantIndicesSafe = constantIndices;
            if (null != constantIndicesSafe) {
                QueryIndex<K, V> index = constantIndicesSafe.get(value);
                if (null != index) {
                    added |= index.addTuple(tuple, values, tupleDepth + 1, variableDepth, expirationTime);
                }
            }
        }

        return added;
    }
    
    private boolean removeTuple(V[] tuple, V[] values, int tupleDepth, int variableDepth) {
        boolean removed = false;

        SolutionIndex<V> solutionIndexSafe = solutionIndex;
        if (null != solutionIndexSafe) {
            removed = solutionIndexSafe.removePattern(values);
        }

        if (tupleDepth != tuple.length) {
            V value = tuple[tupleDepth];

            QueryIndex<K, V>[] variableIndicesSafe = variableIndices;
            if (null != variableIndicesSafe) {
                for (int offset = 0; offset < variableIndicesSafe.length; offset++) {
                    QueryIndex<K, V> queryIndex = variableIndicesSafe[offset];
                    if (null != queryIndex) {
                        if (offset > 0) {
                            if (!value.equals(tuple[tupleDepth - offset])) continue;

                            removed |= queryIndex.removeTuple(
                                    tuple, values, tupleDepth + 1, variableDepth);
                        } else {
                            values[variableDepth] = value;
                            removed |= queryIndex.removeTuple(
                                    tuple, values, tupleDepth + 1, variableDepth + 1);
                        }
                    }
                }
            }

            Map<V, QueryIndex<K, V>> constantIndicesSafe = constantIndices;
            if (null != constantIndicesSafe) {
                if (null == value) {
                    for (QueryIndex<K, V> index : constantIndicesSafe.values()) {
                        removed |= index.removeTuple(tuple, values, tupleDepth + 1, variableDepth);
                    }
                } else {
                    QueryIndex<K, V> index = constantIndicesSafe.get(value);
                    if (null != index) {
                        removed |= index.removeTuple(tuple, values, tupleDepth + 1, variableDepth);
                    }
                }
            }
        }

        return removed;
    }
}