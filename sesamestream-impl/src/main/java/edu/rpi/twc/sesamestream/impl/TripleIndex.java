package edu.rpi.twc.sesamestream.impl;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import java.util.HashMap;
import java.util.Map;

/**
 * A recursive data structure which associates {@link TriplePattern}s with SPARQL {@link edu.rpi.twc.sesamestream.impl.PartialSolution}s and computes
 * new solutions in response to incoming RDF statements.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class TripleIndex {

    // the "leaves" of this index
    private LList<PartialSolutionWrapper> partialSolutions;

    // child indices matching specific values
    private Map<Value, TripleIndex> valueIndices;

    // child index matching any value
    private TripleIndex wildcardIndex;

    /**
     * Returns this index to its original, empty state
     */
    public void clear() {
        partialSolutions = null;
        wildcardIndex = null;
        if (null != valueIndices) {
            valueIndices.clear();
        }
    }

    /**
     * Recursively associates a triple pattern with a partial solution such as a query
     *
     * @param p     the triple pattern against which incoming statements will logically be matched
     * @param tuple the remaining portion of the triple pattern to be indexed at this level
     * @param ps    the partial solution to store.
     *              When incoming statements match against the triple pattern, this partial solution will be retrieved.
     */
    public void index(final TriplePattern p,
                      final VarList tuple,
                      final PartialSolution ps) {
        //System.out.println("indexing:\n\t" + list + "\n\t" + ps);
        //print();
        //new Exception().printStackTrace(System.out); System.out.flush();

        // done recursing; add the partial solution at this level
        if (null == tuple) {
            //System.out.println("indexing nil list to " + ps);
            if (null == partialSolutions) {
                partialSolutions = LList.NIL;
            }

            partialSolutions = partialSolutions.push(new PartialSolutionWrapper(p, ps));
        }

        // continue recursing, "left" or "right" depending on whether the first value in the tuple
        // is a value or a wildcard
        else {
            Value v = tuple.getValue();

            if (null == v) {
                //System.out.println("indexing null from list " + list + " to " + ps);
                if (null == wildcardIndex) {
                    wildcardIndex = new TripleIndex();
                }

                wildcardIndex.index(p, tuple.getRest(), ps);
            } else {
                //System.out.println("indexing value " + v + " from list " + list + " to " + ps);

                if (null == valueIndices) {
                    valueIndices = new HashMap<Value, TripleIndex>();
                }

                TripleIndex n = valueIndices.get(v);

                if (null == n) {
                    n = new TripleIndex();
                    valueIndices.put(v, n);
                }

                n.index(p, tuple.getRest(), ps);
            }
        }
    }

    /**
     * Recursively matches a tuple pattern (at the top level, an RDF statement) against all indexed patterns,
     * handling matching partial solutions.
     *
     * @param tuple  the remaining portion of the statement to be matched
     * @param st     the original statement
     * @param binder a handler for matching partial solutions
     */
    public void match(final VarList tuple,
                      final Statement st,
                      final SolutionHandler binder) {
        // done recursing; all partial solutions at this level are matches
        if (null == tuple) {
            //System.out.println("matching nil list for statement " + st);
            if (null != partialSolutions) {
                LList<PartialSolutionWrapper> cur = partialSolutions;
                while (!cur.isNil()) {
                    PartialSolutionWrapper ps = cur.getValue();
                    VarList newBindings = null;

                    //System.out.println("\t" + ps.partialSolution);
                    TriplePattern matched = ps.triplePattern;
                    //System.out.println("\t\t" + p);
                    if (!matched.getSubject().hasValue()) {
                        //System.out.println("\t\t\tsubject");
                        newBindings = new VarList(matched.getSubject().getName(), st.getSubject(), newBindings);
                    }

                    if (!matched.getPredicate().hasValue()) {
                        //System.out.println("\t\t\tpredicate");
                        newBindings = new VarList(matched.getPredicate().getName(), st.getPredicate(), newBindings);
                    }

                    if (!matched.getObject().hasValue()) {
                        //System.out.println("\t\t\tobject");
                        newBindings = new VarList(matched.getObject().getName(), st.getObject(), newBindings);
                    }

                    binder.handle(ps.partialSolution, matched, newBindings);
                    cur = cur.getRest();
                }
            }
        }

        // keep recursing, "left" and "right"
        else {
            //System.out.println("matching non-nil list " + list + " for statement: " + st);
            if (null != valueIndices) {
                //System.out.println("we have value indices");
                TripleIndex child = valueIndices.get(tuple.getValue());

                if (null != child) {
                    //System.out.println("child found");
                    child.match(tuple.getRest(), st, binder);
                }
            }

            if (null != wildcardIndex) {
                //System.out.println("we have a wildcard index");
                wildcardIndex.match(tuple.getRest(), st, binder);
            }
        }
    }

    /**
     * Visits all partial solutions in the database.
     * Useful for inspecting the contents of the database at any given time, e.g. by counting or printing.
     *
     * @param v a visitor for partial solutions
     * @return whether all solutions were visited (<code>Visitor</code>s may choose to abort the traversal)
     */
    public boolean visitPartialSolutions(final Visitor<PartialSolution> v) {
        if (null != partialSolutions) {
            LList<PartialSolutionWrapper> cur = partialSolutions;
            while (!cur.isNil()) {
                if (!v.visit(cur.getValue().partialSolution)) {
                    return false;
                }
                cur = cur.getRest();
            }
        }

        if (null != valueIndices) {
            for (TripleIndex i : valueIndices.values()) {
                if (!i.visitPartialSolutions(v)) {
                    return false;
                }
            }
        }

        if (null != wildcardIndex) {
            if (!wildcardIndex.visitPartialSolutions(v)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Prints a plain text representation of this index to standard output
     */
    public void print() {
        printInternal("");
    }

    private void printInternal(String heading) {
        if (null != partialSolutions) {
            System.out.println(heading + ":");
            LList<PartialSolutionWrapper> cur = partialSolutions;
            while (!cur.isNil()) {
                System.out.println("\t" + cur.getValue().partialSolution);
                cur = cur.getRest();
            }
        } else {
            if (null != wildcardIndex) {
                wildcardIndex.printInternal(heading + "? ");
            }

            if (null != valueIndices) {
                for (Map.Entry<Value, TripleIndex> e : valueIndices.entrySet()) {
                    e.getValue().printInternal(heading + valueToString(e.getKey()) + " ");
                }
            }
        }
    }

    private String valueToString(final Value v) {
        if (v instanceof URI) {
            return "<" + v.stringValue() + ">";
        } else if (v instanceof BNode) {
            return "_:" + v.stringValue();
        } else if (v instanceof Literal) {
            // TODO: datatype, language
            return "\"" + v.stringValue() + "\"";
        } else {
            throw new IllegalStateException("value is of unknown type: " + v);
        }
    }

    // A wrapper which associates a PartialSolution with the TriplePattern under which it is indexed
    private class PartialSolutionWrapper {
        public final TriplePattern triplePattern;
        public final PartialSolution partialSolution;

        private PartialSolutionWrapper(final TriplePattern triplePattern,
                                       final PartialSolution partialSolution) {
            this.triplePattern = triplePattern;
            this.partialSolution = partialSolution;
        }
    }
}
