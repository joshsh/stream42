package edu.rpi.twc.sesamestream.tuple;

/**
 * An object representing the subset of tuple patterns of a query which have already been matched.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionPattern {
    public enum ContainmentRelation {Contains, ContainedIn, Equal, PartialIntersect, Disjoint}

    protected int remainingPatterns;

    // bit field
    protected int matchedPatterns;

    protected SolutionPattern(final int remainingPatterns,
                              final int matchedPatterns) {
        setSolutionType(remainingPatterns, matchedPatterns);
    }

    public SolutionPattern(final SolutionPattern other) {
        setSolutionType(other.remainingPatterns, other.matchedPatterns);
    }

    public void setSolutionType(final int remainingPatterns,
                                final int matchedPatterns) {
        this.remainingPatterns = remainingPatterns;
        this.matchedPatterns = matchedPatterns;
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
}
