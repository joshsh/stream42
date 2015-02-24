package edu.rpi.twc.sesamestream.tuple;

/**
 * An intermediate result of query answering,
 * representing a complete or partial solution to a query.
 * It stores the pattern or patterns of the query which have been matched
 * as well as the resulting variable/value bindings.
 * Logically, a SesameStream query index is a set of solutions associated with their queries.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Solution<T> extends SolutionPattern {
    private Bindings<T> bindings;

    /**
     * Copy constructor
     *
     * @param other another solution from which to produce a shallow copy
     */
    public Solution(final Solution<T> other) {
        super(other.remainingPatterns, other.matchedPatterns, other.expirationTime);
        this.bindings = other.bindings;
    }

    /**
     * Constructs a solution in which a single tuple pattern of a graph pattern has been matched
     *
     * @param totalPatterns  the total number of tuple patterns in the graph pattern
     * @param index          the index of the matched pattern (the first pattern having an index of 0)
     * @param bindings       the variable/term bindings created by the match
     * @param expirationTime the expiration time of this solution in milliseconds since the Unix epoch
     *                       (or 0 for infinite lifetime)
     */
    public Solution(final int totalPatterns,
                    final int index,
                    final Bindings<T> bindings,
                    final long expirationTime) {
        super(totalPatterns - 1, 1 << index, expirationTime);
        this.bindings = bindings;
    }

    // note: this constructor is currently only used in unit tests
    public Solution(final Solution<T> other,
                    final int index) {
        super(other.remainingPatterns - 1, other.matchedPatterns | (1 << index), other.expirationTime);
        this.bindings = other.bindings;
    }

    // note: this constructor is currently only used in unit tests
    public Solution(final Solution<T> other,
                    final int index,
                    final Bindings<T> newBindings) {
        super(other.remainingPatterns - 1, other.matchedPatterns | (1 << index), other.expirationTime);
        this.bindings = Bindings.from(other.bindings, newBindings);
    }

    // note: assumes complementary solutions
    public Solution(final int totalPatterns,
                    final Solution<T> first,
                    final Solution<T> second) {
        super(first.remainingPatterns + second.remainingPatterns - totalPatterns,
                first.matchedPatterns | second.matchedPatterns,
                minExpirationTime(first, second));

        bindings = Bindings.from(first.bindings, second.bindings);
    }

    /**
     * Gets the variables bindings of this solution
     *
     * @return the variable bindings of this solution.  When the solution is complete
     * (i.e. when all patterns of the query have been matched) the bindings are the the actual query "answer".
     */
    public Bindings<T> getBindings() {
        return bindings;
    }

    /**
     * Sets the variable bindings of this solution.  This method is used internally, by the solution index.
     *
     * @param bindings the new variable bindings
     */
    public void setBindings(final Bindings<T> bindings) {
        this.bindings = bindings;
    }

    /**
     * Checks for non-overlapping patterns and compatible bindings between this solution and another solution,
     * potentially reducing work and eliminating false positives from query results.
     *
     * @param other the other solution
     * @param vars  the query variables from which both solutions are drawn
     * @return whether the solutions are both disjoint in patterns and compatible in bindings
     */
    public boolean composableWith(final Solution<T> other,
                                  final Query.QueryVariables vars) {
        // note: we check for pattern disjointness first, as this is a much cheaper operation
        // than the check for conflicting bindings
        return disjointWith(other)
                && bindings.compatibleWith(other.bindings, vars);
    }
}
