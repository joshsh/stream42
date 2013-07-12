package edu.rpi.twc.sesamestream;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class TripleIndex {

    // the "leaves" of this index
    private List<PartialSolutionWrapper> partialSolutions;

    // child indices matching specific values
    private Map<Value, TripleIndex> valueIndices;

    // child index matching any value
    private TripleIndex wildcardIndex;

    public void index(final TriplePattern p,
                      final VarList list,
                      final PartialSolution ps) {
        //System.out.println("indexing:\n\t" + list + "\n\t" + ps);
        //print();
        //new Exception().printStackTrace(System.out); System.out.flush();

        if (null == list) {
            //System.out.println("indexing nil list to " + ps);
            if (null == partialSolutions) {
                partialSolutions = new LinkedList<PartialSolutionWrapper>();
            }
            partialSolutions.add(new PartialSolutionWrapper(p, ps));
        } else {
            Value v = list.getValue();

            if (null == v) {
                //System.out.println("indexing null from list " + list + " to " + ps);
                if (null == wildcardIndex) {
                    wildcardIndex = new TripleIndex();
                }

                wildcardIndex.index(p, list.getRest(), ps);
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

                n.index(p, list.getRest(), ps);
            }
        }
    }

    public void match(final VarList list,
                      final Statement st,
                      final SolutionBinder handler) {
        if (null == list) {
            //System.out.println("matching nil list for statement " + st);
            if (null != partialSolutions) {

                for (PartialSolutionWrapper ps : partialSolutions) {
                    VarList newBindings = null;

                    //System.out.println("\t" + ps.partialSolution);
                    TriplePattern p = ps.triplePattern;
                    //System.out.println("\t\t" + p);
                    if (!p.getSubject().hasValue()) {
                        //System.out.println("\t\t\tsubject");
                        newBindings = new VarList(p.getSubject().getName(), st.getSubject(), newBindings);
                    }

                    if (!p.getPredicate().hasValue()) {
                        //System.out.println("\t\t\tpredicate");
                        newBindings = new VarList(p.getPredicate().getName(), st.getPredicate(), newBindings);
                    }

                    if (!p.getObject().hasValue()) {
                        //System.out.println("\t\t\tobject");
                        newBindings = new VarList(p.getObject().getName(), st.getObject(), newBindings);
                    }

                    handler.bind(ps.partialSolution, p, newBindings);
                }
            }
        } else {
            //System.out.println("matching non-nil list " + list + " for statement: " + st);
            if (null != valueIndices) {
                //System.out.println("we have bound variable indices");
                TripleIndex child = valueIndices.get(list.getValue());

                if (null != child) {
                    //System.out.println("child found");
                    child.match(list.getRest(), st, handler);
                }
            }

            if (null != wildcardIndex) {
                //System.out.println("we have unbound variable indices");
                wildcardIndex.match(list.getRest(), st, handler);
            }
        }
    }

    public boolean visitPartialSolutions(final Visitor<PartialSolution> v) {
        if (null != partialSolutions) {
            for (PartialSolutionWrapper ps : partialSolutions) {
                if (!v.visit(ps.partialSolution)) {
                    return false;
                }
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

    public void print() {
        printInternal("");
    }

    private void printInternal(String heading) {
        if (null != partialSolutions) {
            System.out.println(heading + ":");
            for (PartialSolutionWrapper ps : partialSolutions) {
                System.out.println("\t" + ps.partialSolution);
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
