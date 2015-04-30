package edu.rpi.twc.rdfstream4j.core;

/**
 * An object representing the subset of tuple patterns of a query which have already been matched.
 * A solution pattern is a solution omitting the bindings (which may be stored in a solution group).
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionPattern {
    /**
     * A relationship between two solution patterns which affects how they may be composed into new solutions.
     */
    public enum ContainmentRelation {
        /**
         * The relationship in which one solution contains all of the tuple patterns of a second solution,
         * in addition to one or more other patterns
         */
        Contains,
        /**
         * The relationship in which the tuple patterns of one solution are completely contained in a second solution,
         * which also contains other patterns
         */
        ContainedIn,
        /**
         * The relationship in which two solutions contain exactly the same patterns
         */
        Equal,
        /**
         * The relationship in which two solutions share one or more tuple patterns,
         * but both solutions contain one or more patterns which are not shared.
         */
        PartialIntersect,
        /**
         * The relationship in which two solutions have no tuple patterns in common
         */
        Disjoint
    }

    // total patterns remaining to be matched
    protected int remainingPatterns;

    // bit field; bits for already-matched patterns are 1, otherwise 0
    protected int matchedPatterns;

    // TODO: use int to save space, and chunk timestamps accordingly
    protected long expirationTime;

    protected SolutionPattern(final int remainingPatterns,
                              final int matchedPatterns,
                              final long expirationTime) {
        this.remainingPatterns = remainingPatterns;
        this.matchedPatterns = matchedPatterns;
        this.expirationTime = expirationTime;
    }

    /**
     * Copy constructor
     *
     * @param other another solution pattern to copy
     */
    public SolutionPattern(final SolutionPattern other) {
        copyFrom(other);
    }

    /**
     * Overwrites the fields of this solution with those of a given solution.
     * An alternative to the copy constructor which does not create a new object.
     *
     * @param other another solution pattern to copy
     */
    public void copyFrom(final SolutionPattern other) {
        this.remainingPatterns = other.remainingPatterns;
        this.matchedPatterns = other.matchedPatterns;
        this.expirationTime = other.expirationTime;
    }

    /**
     * Sets a new expiration time for this solution
     *
     * @param expirationTime an expiration time, in milliseconds since the Unix epoch.
     *                       The special value 0 indicates that the solution is valid indefinitely.
     */
    public void setExpirationTime(final long expirationTime) {
        this.expirationTime = expirationTime;
    }

    /**
     * Gets the expiration status of this solution
     *
     * @param now the current time, in millseconds since the Unix epoch
     * @return whether this solution has expired, i.e. is no longer valid, with respect to the current time
     */
    public boolean isExpired(final long now) {
        return expirationTime > 0 && expirationTime < now;
    }

    /**
     * Finds whether this solution is complete
     *
     * @return whether this solution is complete.
     * A solution is complete if all tuple patterns in the query have been matched.
     */
    public boolean isComplete() {
        return 0 == remainingPatterns;
    }

    /**
     * Finds whether this solution is disjoint with another solution
     *
     * @param other another solution
     * @return whether this solution is disjoint with the given solution, in terms of matched tuple patterns
     */
    public boolean disjointWith(final SolutionPattern other) {
        return 0 == (matchedPatterns & other.matchedPatterns);
    }

    /**
     * Finds the containment relation between this solution and another solution
     *
     * @param other another solution for comparison
     * @return the containment relation between this solution and the given solution
     */
    public ContainmentRelation relateTo(final SolutionPattern other) {
        int x = matchedPatterns ^ other.matchedPatterns;

        if (0 == x) {
            return matchedPatterns == other.matchedPatterns
                    ? ContainmentRelation.Equal
                    : ContainmentRelation.Disjoint;
        } else {
            return 0 == (matchedPatterns & x)
                    ? ContainmentRelation.ContainedIn
                    : 0 == (other.matchedPatterns & x)
                    ? ContainmentRelation.Contains
                    : ContainmentRelation.PartialIntersect;
        }
    }

    /**
     * Composes the expiration times of two inputs to produce an expiration time for an output.
     * The output expires as soon as possible given the expiration times of the two inputs.
     *
     * @param s1 a solution pattern
     * @param s2 a solution pattern
     * @return the resulting minimum expiration time
     */
    public static long minExpirationTime(final SolutionPattern s1, final SolutionPattern s2) {
        return 0 == s1.expirationTime
                ? 0 == s2.expirationTime ? 0 : s2.expirationTime
                : 0 == s2.expirationTime ? s1.expirationTime : Math.min(s1.expirationTime, s2.expirationTime);
    }

    /**
     * Composes the expiration times of two inputs to produce an expiration time for an output.
     * The output expires as late as possible given the expiration times of the two inputs.
     *
     * @param s1 a solution pattern
     * @param s2 a solution pattern
     * @return the resulting maximum expiration time. Note that if either expiration time is infinite
     * (represented by the special value 0) the result will be infinite, as well.
     */
    public static long maxExpirationTime(final SolutionPattern s1, final SolutionPattern s2) {
        return (0 == s1.expirationTime || 0 == s2.expirationTime)
                ? 0 : Math.max(s1.expirationTime, s2.expirationTime);
    }

    /**
     * Compares the expiration times of two solutions
     *
     * @param s1 a solution pattern
     * @param s2 a solution pattern
     * @return a comparison value relating the expiration times of the two solutions:
     * -1 if the first solution expires before the second, 1 if the first solution expires after the second,
     * and 0 if the solutions expire at the same time.
     * The infinite expiration time (represented by 0) exceeds all finite expiration times, but is equal to itself.
     */
    public static int compareExpirationTimes(final SolutionPattern s1, final SolutionPattern s2) {
        return 0 == s1.expirationTime
                ? 0 == s2.expirationTime ? 0 : 1
                : 0 == s2.expirationTime ? -1 : ((Long) s1.expirationTime).compareTo(s2.expirationTime);
    }
}
