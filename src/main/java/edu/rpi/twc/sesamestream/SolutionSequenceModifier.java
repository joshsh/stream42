package edu.rpi.twc.sesamestream;

import org.openrdf.query.BindingSet;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionSequenceModifier {
    private Set<BindingSet> distinctSet;
    private long limit = -1;

    private long count = 0;

    public void makeDistinct() {
        if (null == distinctSet) {
            distinctSet = new HashSet<BindingSet>();
        }
    }

    /**
     * Sets the LIMIT of the associated query, which puts an upper bound on the number of solutions returned
     *
     * @param limit the upper bound on the number of solutions returned
     */
    public void setLimit(final long limit) {
        // from the SPARQL spec: a LIMIT of 0 would cause no results to be returned. A limit may not be negative
        // limit == -1 is a special value ("no limit") which cannot be set explicitly
        if (limit < 0) {
            throw new IllegalArgumentException("negative LIMIT");
        }

        this.limit = limit;
    }

    /**
     * Sets the OFFSET of the associated query.
     * Do this only once, at query construction time.
     *
     * @param offset the number of solutions which will be generated before query answers are returned
     */
    public void setOffset(final long offset) {
        // from the SPARQL spec: an OFFSET of zero has no effect
        if (offset < 0) {
            throw new IllegalArgumentException("negative OFFSET");
        }

        count = -offset;
    }

    /**
     * Pass a potential solution through this sequence modifier to determine whether it should be returned as an answer
     *
     * @param solution the potential query answer
     * @param subs     the subscription to the query.  If a LIMIT is exceeded,
     * @return whether to produce the solution as an answer
     */
    public boolean trySolution(final BindingSet solution,
                               final Subscription subs) {
        // apply DISTINCT before LIMIT and OFFSET
        if (null != distinctSet) {
            if (distinctSet.contains(solution)) {
                return false;
            } else {
                distinctSet.add(solution);
            }
        }

        count++;

        // OFFSET has been exceeded, or if there is no OFFSET
        if (count > 0) {
            // apply LIMIT
            if (-1 == limit) {
                return true;
            } else {
                // not only do we exclude the result if the LIMIT has been exceeded, but we also unregister the query
                if (count == limit) {
                    subs.cancel();
                    return true;
                } else {
                    return count <= limit;
                }
            }
        } else {
            return false;
        }
    }
}
