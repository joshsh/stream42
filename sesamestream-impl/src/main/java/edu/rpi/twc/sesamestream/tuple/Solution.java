package edu.rpi.twc.sesamestream.tuple;

/**
 * An intermediate result of query answering,
 * representing a complete or partial solution to a query.
 * It stores the pattern or patterns of the query which have been matched
 * as well as the resulting variable/value bindings.
 * Logically, a SesameStream query index is a set of <code>PartialSolution</code>s associated with subscriptions.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Solution<T> extends SolutionPattern {
    private VariableBindings<T> bindings;

    public Solution(final Solution<T> other) {
        super(other.remainingPatterns, other.matchedPatterns);
        this.bindings = other.bindings;
    }

    public Solution(final int totalPatterns,
                    final int index,
                    final VariableBindings<T> bindings) {
        super(totalPatterns - 1, 1 << index);
        this.bindings = bindings;
    }

    public Solution(final Solution<T> other,
                    final int index) {
        super(other.remainingPatterns - 1, other.matchedPatterns | (1 << index));
        this.bindings = other.bindings;
    }

    public Solution(final Solution<T> other,
                    final int index,
                    final VariableBindings<T> newBindings) {
        super(other.remainingPatterns - 1, other.matchedPatterns | (1 << index));
        this.bindings = VariableBindings.from(other.bindings, newBindings);
    }

    // note: assumes complementary solutions
    public Solution(final int totalPatterns,
                    final Solution<T> first,
                    final Solution<T> second) {
        super(first.remainingPatterns + second.remainingPatterns - totalPatterns,
                first.matchedPatterns | second.matchedPatterns);

        bindings = VariableBindings.from(first.bindings, second.bindings);
    }

    public int getRemainingPatterns() {
        return remainingPatterns;
    }

    public boolean isComplete() {
        return 0 == remainingPatterns;
    }

    public VariableBindings<T> getBindings() {
        return bindings;
    }

    public void setBindings(final VariableBindings<T> bindings) {
        this.bindings = bindings;
    }

    /**
     * Checks for non-overlapping patterns and compatible bindings between this solution and another solution,
     * potentially reducing work and eliminating false positives from query results.
     *
     * @param other the other solution
     * @param vars the query variables from which both solutions are drawn
     * @return whether the solutions are both disjoint in patterns and compatible in bindings
     */
    public boolean complements(final Solution<T> other,
                               final GraphPattern.QueryVariables vars) {
        // note: we check for pattern disjointness first, as this is a much cheaper operation
        // than the check for conflicting bindings
        return disjointWith(other)
                && bindings.compatibleWith(other.bindings, vars);
    }
}
