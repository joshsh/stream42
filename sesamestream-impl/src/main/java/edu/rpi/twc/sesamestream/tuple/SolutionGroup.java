package edu.rpi.twc.sesamestream.tuple;

import edu.rpi.twc.sesamestream.impl.LList;

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
    public void add(final Solution<T> ps) {
        LList<SolutionPattern> cur = solutions, prev = null;
        while (!cur.isNil()) {
            SolutionPattern.ContainmentRelation r = cur.getValue().relateTo(ps);
            switch (r) {
                case Contains:
                    // old solution contains new solution; ignore the new one
                    return;
                case ContainedIn:
                    // new solution contains old solution; remove the former
                    if (null == prev) {
                        solutions = cur.getRest();
                    } else {
                        prev.setRest(cur.getRest());
                    }
                    cur = cur.getRest();
                    continue;
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
            prev = cur;
            cur = cur.getRest();
        }

        // at this point, we have found it necessary to add the new solution
        solutions = solutions.push(new SolutionPattern(ps));
    }
}
