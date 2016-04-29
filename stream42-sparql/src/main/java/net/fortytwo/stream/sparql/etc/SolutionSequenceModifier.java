package net.fortytwo.stream.sparql.etc;

import net.fortytwo.stream.Subscription;
import net.fortytwo.stream.sparql.SparqlStreamProcessor;
import org.openrdf.query.BindingSet;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionSequenceModifier {
    private Set<BindingSet> distinctSet;
    private boolean hasReduced = false;

    private long limit = -1;
    private long count = 0;

    /**
     * Mark the associated query with a DISTINCT modifier
     */
    public void makeDistinct() {
        if (null == distinctSet) {
            distinctSet = new HashSet<>();
        }
    }

    /**
     * Mark the associated query with a REDUCE modifier
     */
    public void makeReduced() {
        makeDistinct();
        hasReduced = true;
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
     * @throws IOException if there is a problem communicating with this query engine
     *                     (for example, if there are network operations involved)
     */
    public boolean trySolution(final BindingSet solution,
                               final Subscription subs) throws IOException {
        // apply DISTINCT and REDUCE before OFFSET and LIMIT
        if (null != distinctSet) {
            if (distinctSet.contains(solution)) {
                return false;
            } else {
                // this is REDUCED, which in SesameStream only allows the distinct set to grow to a specified size
                // Currently, the set is simply cleared when it overflows
                if (hasReduced) {
                    if (distinctSet.size() >= SparqlStreamProcessor.getReducedModifierCapacity()) {
                        distinctSet.clear();
                    }
                }

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
