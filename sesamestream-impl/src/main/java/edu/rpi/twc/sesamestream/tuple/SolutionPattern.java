package edu.rpi.twc.sesamestream.tuple;

/**
 * An object representing the subset of tuple patterns of a query which have already been matched.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionPattern {
    public enum ContainmentRelation {Contains, ContainedIn, Equal, PartialIntersect, Disjoint}

    // total patterns remaining to be matched
    protected int remainingPatterns;

    // bit field; bits for already-matched patterns are 1, otherwise 0
    protected int matchedPatterns;

    protected long expirationTime;

    protected SolutionPattern(final int remainingPatterns,
                              final int matchedPatterns,
                              final long expirationTime) {
        this.remainingPatterns = remainingPatterns;
        this.matchedPatterns = matchedPatterns;
        this.expirationTime = expirationTime;
    }

    public SolutionPattern(final SolutionPattern other) {
        copyFrom(other);
    }

    public void copyFrom(final SolutionPattern other) {
        this.remainingPatterns = other.remainingPatterns;
        this.matchedPatterns = other.matchedPatterns;
        this.expirationTime = other.expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public boolean isExpired(final long now) {
        return expirationTime > 0 && expirationTime < now;
    }

    public int getRemainingPatterns() {
        return remainingPatterns;
    }

    public boolean isComplete() {
        return 0 == remainingPatterns;
    }

    public boolean disjointWith(final SolutionPattern other) {
        return 0 == (matchedPatterns & other.matchedPatterns);
    }

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
     * Composes the expiration times of two inputs produce an expiration time for an output.
     * The output expires as soon as possible given the expiration times of the two inputs.
     *
     * @param time1 the expiration time of the first input
     * @param time2 the expiration time of the second input
     * @return the resulting expiration time
     */
    protected static long composeExpirationTimes(final long time1, final long time2) {
        return 0 == time1
                ? 0 == time2 ? 0 : time2
                : 0 == time2 ? time1 : Math.min(time1, time2);
    }
}
