package edu.rpi.twc.sesamestream.tuple;

/**
 * A group of partial solutions with identical bindings but potentially different sets of matched tuple patterns.
 * The group takes advantage of containment relationships to eliminate redundant or weak solutions, saving work.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionGroup<T> {
    private final VariableBindings<T> bindings;
    private LList<SolutionPattern> solutions = LList.NIL;

    public SolutionGroup(VariableBindings<T> bindings) {
        this.bindings = bindings;
    }

    public VariableBindings<T> getBindings() {
        return bindings;
    }

    public LList<SolutionPattern> getSolutions() {
        return solutions;
    }

    // note: we assume identical bindings
    public void add(final Solution<T> sol,
                    final long now) {
        if (sol.isExpired(now)) {
            return;
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
                        // old solution contains new solution; ignore the new one
                        return;
                    case ContainedIn:
                        // new solution contains old solution; remove the former
                        removeOld = true;
                        break;
                    case Equal:
                        // new solution is identical to the old solution; ignore the new one
                        return;
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
                cur = cur.getRest();
                continue;
            }
            prev = cur;
            cur = cur.getRest();
        }

        // at this point, we have found it necessary to add the new solution
        solutions = solutions.push(
                // as the group stores the bindings, we omit them here
                new SolutionPattern(sol));
    }

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
}
