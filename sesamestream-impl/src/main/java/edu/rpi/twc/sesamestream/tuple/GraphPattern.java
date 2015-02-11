package edu.rpi.twc.sesamestream.tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A query composed of a set of tuple patterns associated with a subscription
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphPattern<T> implements Comparable<GraphPattern<T>> {
    private final List<TuplePattern<T>> patterns;
    private final QueryVariables variables;

    private String id;
    private long expirationTime;

    private final SolutionIndex<T> solutionIndex;

    // a temporary collection used externally for tuple matching
    private final Set<Long> solutionHashes = new HashSet<Long>();

    public GraphPattern(final List<TuplePattern<T>> patterns,
                        final long expirationTime) {
        if (patterns.size() > 32) {
            // because we create an integer-sized bit field of matched patterns
            throw new IllegalArgumentException(
                    "too many tuple patterns; implementation limit is 32 per graph pattern");
        }

        this.patterns = patterns;
        this.expirationTime = expirationTime;

        int i = 0;
        Set<String> variableSet = new HashSet<String>();
        for (TuplePattern<T> pattern : patterns) {
            pattern.setGraphPattern(this);
            pattern.setIndex(i++);
            for (Term<T> t : pattern.getTerms()) {
                String v = t.getVariable();
                if (null != v) {
                    variableSet.add(v);
                }
            }
        }
        variables = new QueryVariables(variableSet);

        this.solutionIndex = new SolutionIndex<T>(variables, patterns.size());
    }

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public List<TuplePattern<T>> getPatterns() {
        return patterns;
    }

    public QueryVariables getVariables() {
        return variables;
    }

    public SolutionIndex<T> getSolutionIndex() {
        return solutionIndex;
    }

    public Set<Long> getSolutionHashes() {
        return solutionHashes;
    }

    public boolean isExpired(final long now) {
        return expirationTime > 0 && expirationTime < now;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    // order graph patterns by increasing expiration time
    @Override
    public int compareTo(GraphPattern<T> other) {
        return 0 == expirationTime
                ? 0 == other.expirationTime ? 0 : 1
                : 0 == other.expirationTime ? -1 : ((Long) expirationTime).compareTo(other.expirationTime);
    }

    public static class QueryVariables {
        private final String[] variables;
        private final Map<String, Integer> variableIndices;

        public QueryVariables(final Collection<String> coll) {
            variables = coll.toArray(new String[coll.size()]);
            variableIndices = new HashMap<String, Integer>();
            for (int i = 0; i < variables.length; i++) {
                variableIndices.put(variables[i], i);
            }
        }

        public String[] asArray() {
            return variables;
        }

        public int indexOf(final String variable) {
            Integer i = variableIndices.get(variable);

            if (null == i) {
                throw new NoSuchElementException("no such variable: " + variable);
            } else {
                return i;
            }
        }
    }
}
