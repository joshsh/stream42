package edu.rpi.twc.rdfstream4j.core;

/**
 * A group of solutions with identical bindings but potentially different sets of matched tuple patterns.
 * The group takes advantage of containment relationships to eliminate redundant or weak solutions, saving work.
 * There is only one solution group per unique set of bindings per solution index.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionGroup<T> {
    private final Bindings<T> bindings;
    private LList<SolutionPattern> solutions = LList.NIL;

    /**
     * Creates a new solution group for a set of bindings
     *
     * @param bindings a set of bindings.  A solution group is uniquely identified by its bindings, in combination
     *                 with the query variables from which they are drawn.
     */
    public SolutionGroup(Bindings<T> bindings) {
        this.bindings = bindings;
    }

    /**
     * Gets the bindings of this solution group
     *
     * @return a set of bindings
     */
    public Bindings<T> getBindings() {
        return bindings;
    }

    /**
     * Gets the solutions contained in this group
     *
     * @return a linked list of solution pattern contained in this group.
     * Logically, a solution is a solution pattern combined with a set of bindings, which are provided here by
     * <code>getBindings()</code>.
     */
    public LList<SolutionPattern> getSolutions() {
        return solutions;
    }

    /**
     * Adds a solution, logically, to this group.
     * The provided solution may simply be appended to the list of solution patterns or, if it is equal to, contains
     * or is contained in another pattern in the group, will replace another pattern or simply be ignored if the
     * expiration times of the solutions in question make this possible.
     *
     * @param sol a new solution for potential addition to this group.
     *            The bindings of the added solution must be identical to those of this group.
     * @param now the current time in milliseconds since the Unix epoch.
     *            The solution group is updated with respect to this time stamp in that an expired solution will
     *            be rejected and expired solutions still present in the group and encountered in the process of
     *            insertion will be removed.
     */
    public boolean add(final Solution<T> sol,
                       final long now) {
        if (sol.isExpired(now)) {
            throw new IllegalStateException("we shouldn't add an expired solution");
        }

        LList<SolutionPattern> cur = solutions, prev = null;
        while (!cur.isNil()) {
            boolean removeOld = false;
            SolutionPattern curSol = cur.getValue();

            // remove any expired solutions we encounter which have not yet been removed automatically
            if (curSol.isExpired(now)) {
                removeOld = true;
            } else {
                SolutionPattern.ContainmentRelation r = curSol.relateTo(sol);
                switch (r) {
                    case Contains:
                        // old solution contains new solution; ignore the new one, but *only* if it will expire first
                        if (SolutionPattern.compareExpirationTimes(curSol, sol) >= 0) {
                            return false;
                        }
                        break;
                    case ContainedIn:
                        // new solution contains old solution; remove the former, but *only* if it expires first
                        if (SolutionPattern.compareExpirationTimes(curSol, sol) <= 0) {
                            removeOld = true;
                        }
                        break;
                    case Equal:
                        // new solution is identical to the old solution; simply update the expiration time
                        int cmp = SolutionPattern.compareExpirationTimes(curSol, sol);
                        if (cmp < 0) {
                            curSol.setExpirationTime(sol.expirationTime);
                            return true;
                        } else {
                            return false;
                        }
                    case PartialIntersect:
                        // old and new solutions have one or more patterns in common, and are yet distinct
                        break;
                    case Disjoint:
                        // old and new solutions are distinct
                        break;
                }
            }
            if (removeOld) {
                if (null == prev) {
                    solutions = cur.getRest();
                } else {
                    prev.setRest(cur.getRest());
                }
                LList<SolutionPattern> tmp = cur.getRest();
                cur.setRest(null);
                cur = tmp;
                continue;
            }
            prev = cur;
            cur = cur.getRest();
        }

        // at this point, we have found it necessary to add the new solution
        solutions = solutions.push(
                // as the group stores the bindings, we omit them here
                new SolutionPattern(sol));

        return true;
    }

    /**
     * Removes all expired solutions from this group.
     * Note that this operation may cause the group to become empty.
     *
     * @param now the current time, in milliseconds since the Unix epoch
     * @return the number of solutions removed from this group
     */
    public int removeExpired(final long now) {
        int removed = 0;

        LList<SolutionPattern> cur = solutions, prev = null;
        while (!cur.isNil()) {
            SolutionPattern curPs = cur.getValue();
            if (curPs.isExpired(now)) {
                removed++;

                if (null == prev) {
                    solutions = cur.getRest();
                } else {
                    prev.setRest(cur.getRest());
                }
                cur = cur.getRest();
                continue;
            }
            prev = cur;
            cur = cur.getRest();
        }

        return removed;
    }

    @Override
    public int hashCode() {
        return bindings.getHash();
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof SolutionGroup && (((SolutionGroup) other).bindings.getHash() == bindings.getHash());
    }
}
