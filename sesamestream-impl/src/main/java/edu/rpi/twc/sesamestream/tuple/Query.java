package edu.rpi.twc.sesamestream.tuple;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A graph query composed of a set of tuple patterns.
 * Each query also has a unique ID and a finite or infinite expiration time.
 * A query must be submitted to at most one query index, which sets the ID in addition to other fields used in
 * query answering.
 * Furthermore, a query should not be submitted more than once to a query index, although it may be renewed with
 * <code>QueryIndex.renew()</code>.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Query<T> implements Comparable<Query<T>> {
    private final List<PatternInQuery<T>> patterns;
    private final QueryVariables variables;

    private String id;
    private long expirationTime;

    private final SolutionIndex<T> solutionIndex;

    /**
     * Constructs a new query given a set of tuple patterns and an expiration time
     *
     * @param patterns       the graph pattern, or set of tuple patterns, which defines this query
     * @param expirationTime the time, in milliseconds, since the Unix epoch at which this query will automatically
     *                       become inactve and be removed from the index.
     *                       If the special value 0 is supplied as the expiration time, the query will never expire,
     *                       but may be removed explicitly.
     */
    public Query(final List<Term<T>[]> patterns,
                 final long expirationTime) {
        if (0 == patterns.size()) {
            throw new IllegalArgumentException("a query must contain at least one tuple pattern");
        } else if (patterns.size() > 32) {
            // because we create an integer-sized bit field of matched patterns
            throw new IllegalArgumentException(
                    "too many tuple patterns; implementation limit is 32 per graph pattern");
        }

        this.patterns = new LinkedList<PatternInQuery<T>>();
        this.expirationTime = expirationTime;

        int i = 0;
        int tupleSize = patterns.get(0).length;
        Set<String> variableSet = new HashSet<String>();
        for (Term<T>[] p : patterns) {
            int size = p.length;
            if (size != tupleSize) {
                throw new IllegalArgumentException("inconsistent length of tuple patterns");
            }
            PatternInQuery<T> pattern = new PatternInQuery<T>(this, p, i++);
            this.patterns.add(pattern);
            for (Term<T> t : p) {
                String v = t.getVariable();
                if (null != v) {
                    variableSet.add(v);
                }
            }
        }
        variables = new QueryVariables(variableSet);

        this.solutionIndex = new SolutionIndex<T>(variables, patterns.size(), tupleSize);
    }

    /**
     * Get the unique ID of the query
     *
     * @return the unique ID of the query. This is assigned automatically by the query index
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the ID of the query.  If this query is managed by a query index, it should not be necessary
     * to set the ID elsewhere.
     *
     * @param id the new ID of the query
     */
    public void setId(final String id) {
        this.id = id;
    }

    /**
     * Gets this query's tuple patterns
     *
     * @return this query's graph pattern, or list of tuple patterns
     */
    public List<PatternInQuery<T>> getPatterns() {
        return patterns;
    }

    /**
     * Gets this query's variables
     *
     * @return the set of variables which appear in the tuple patterns of this query
     */
    public QueryVariables getVariables() {
        return variables;
    }

    /**
     * Gets the solution index stored along with this query
     *
     * @return the solution index stored along with this query, used by the query index
     * As there can be only one solution index, a query may only be submitted to one query index.
     */
    public SolutionIndex<T> getSolutionIndex() {
        return solutionIndex;
    }

    /**
     * Gets the expiration status of this query
     *
     * @param now the current time, in milliseconds since the Unix epoch
     * @return whether this query has expired.  An expired query will produce no additional answers,
     * and is automatically removed from the query index.
     */
    public boolean isExpired(final long now) {
        return expirationTime > 0 && expirationTime < now;
    }

    /**
     * Sets a new expiration time for this query.
     * This method should not be called once the query has been submitted to an index, which manages its expiration
     * from that point on.
     *
     * @param expirationTime the new expiration time of this query, in milliseconds since the Unix epoch.
     *                       A special value of 0 indicates that the query should never expire.
     */
    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    /**
     * Orders queries by increasing expiration time
     */
    @Override
    public int compareTo(Query<T> other) {
        return 0 == expirationTime
                ? 0 == other.expirationTime ? 0 : 1
                : 0 == other.expirationTime ? -1 : ((Long) expirationTime).compareTo(other.expirationTime);
    }

    /**
     * An array of variables used in a query, mapping an integer index to variable names and vice versa
     */
    public static class QueryVariables {
        private final String[] variables;
        private final Map<String, Integer> variableIndices;

        /**
         * Constructs a new array of variable names
         *
         * @param coll the variable names as a collection.  The order of the names is preserved
         */
        public QueryVariables(final Collection<String> coll) {
            variables = coll.toArray(new String[coll.size()]);
            variableIndices = new HashMap<String, Integer>();
            for (int i = 0; i < variables.length; i++) {
                variableIndices.put(variables[i], i);
            }
        }

        /**
         * Gets the variable names as an array
         *
         * @return the variable names as an array
         */
        public String[] asArray() {
            return variables;
        }

        /**
         * Gets the index of a given variable name
         *
         * @param variable the name of a variable
         * @return the index of the variable in this array
         */
        public int indexOf(final String variable) {
            Integer i = variableIndices.get(variable);

            if (null == i) {
                throw new NoSuchElementException("no such variable: " + variable);
            } else {
                return i;
            }
        }

        /**
         * Finds the set of bindings created by pairing the variables in the given pattern with the corresponding
         * elements of the given tuple
         *
         * @param pattern a tuple pattern
         * @param tuple a tuple
         * @param <T> the value tupe
         * @return the set of bindings created by pairing the variables in the given pattern with the corresponding
         * elements of the given tuple
         */
        public <T> Bindings<T> bind(final Term<T>[] pattern, final T[] tuple) {
            Map<String, T> bindings = new HashMap<String, T>();
            for (int i = 0; i < pattern.length; i++) {
                String v = pattern[i].getVariable();
                if (null != v) {
                    bindings.put(v, tuple[i]);
                }
            }

            return new Bindings<T>(bindings, this);
        }
    }

    /**
     * A tuple pattern with a reference to the containing query, as well as its index within that query
     *
     * @param <T> the type of values of the query and indices
     */
    public static class PatternInQuery<T> {
        private final Term<T>[] terms;
        private final Query<T> query;
        private final int index;

        /**
         * Constructs a new pattern
         *
         * @param query the containing query
         * @param terms the tuple pattern itself
         * @param index the index of this pattern within the query
         */
        private PatternInQuery(final Query<T> query,
                               final Term<T>[] terms,
                               final int index) {
            this.query = query;
            this.terms = terms;
            this.index = index;
        }

        /**
         * Gets the query containing this pattern
         *
         * @return the query containing this pattern
         */
        public Query<T> getQuery() {
            return query;
        }

        /**
         * Gets the terms of this pattern
         *
         * @return the array of terms, i.e. the actual tuple pattern
         */
        public Term<T>[] getTerms() {
            return terms;
        }

        /**
         * Gets the index of this pattern within the query
         *
         * @return the unique index of this pattern within the containing query. Indices range from 0 and up.
         */
        public int getIndex() {
            return index;
        }
    }
}
