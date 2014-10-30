package edu.rpi.twc.sesamestream.impl;

import edu.rpi.twc.sesamestream.QueryEngine;
import org.openrdf.query.Binding;
import org.openrdf.query.algebra.DescribeOperator;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.Slice;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.TupleExprs;
import org.openrdf.query.impl.BindingImpl;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * SesameStream's internal representation of a SPARQL query
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Query {
    private static final Logger LOGGER = Logger.getLogger(Query.class.getName());

    private final Set<String> bindingNames;
    private Map<String, String> extendedBindingNames;
    private LList<TriplePattern> graphPattern;
    private List<Filter> filters;
    private Set<Binding> constants;
    //private Map<String,>

    private final SolutionSequenceModifier sequenceModifier = new SolutionSequenceModifier();

    /**
     * Any of the four SPARQL query forms
     */
    public enum QueryForm {
        ASK, CONSTRUCT, DESCRIBE, SELECT
    }

    private final QueryForm queryForm;

    public Query(final TupleExpr expr,
                 final QueryEngineImpl.TriplePatternDeduplicator deduplicator) throws QueryEngine.IncompatibleQueryException {
        bindingNames = new HashSet<String>();

        graphPattern = LList.NIL;

        //System.out.println("query: " + expr);

        List<QueryModelNode> l = visit(expr);
        if (l.size() != 1) {
            throw new QueryEngine.IncompatibleQueryException("multiple root nodes");
        }
        QueryModelNode root = l.iterator().next();

        // TODO: eliminate redundant patterns
        Collection<StatementPattern> patterns = new LinkedList<StatementPattern>();

        queryForm = findQueryType(root);

        if (QueryForm.SELECT == queryForm) {
            findPatternsInRoot(root, patterns);
        } else {
            throw new QueryEngine.IncompatibleQueryException(queryForm.name() + " query form is currently not supported");
        }

        for (StatementPattern pat : patterns) {
            graphPattern = graphPattern.push(deduplicator.deduplicate(new TriplePattern(pat)));
        }
    }

    /**
     * @return the query form of this query (ASK, CONSTRUCT, DECRIBE, or SELECT)
     */
    public QueryForm getQueryForm() {
        return queryForm;
    }

    /**
     * @return any predefined bindings which are to be added to query solutions.
     *         For example, CONSTRUCT queries may bind constants to the subject, predicate, or object variable
     */
    public Set<Binding> getConstants() {
        return constants;
    }

    private static QueryForm findQueryType(final QueryModelNode root) throws QueryEngine.IncompatibleQueryException {
        if (root instanceof Slice) {
            // note: ASK queries also have Slice as root in Sesame, but we treat them as SELECT queries
            return QueryForm.SELECT;
        } else if (root instanceof Reduced) {
            // note: CONSTRUCT queries also have Reduced as root in Sesame, but this is because they have
            // been transformed to SELECT queries for {?subject, ?predicate, ?object}.
            // We simply treat them as SELECT queries.
            return QueryForm.SELECT;
        } else if (root instanceof Projection || root instanceof Distinct) {
            return QueryForm.SELECT;
        } else if (root instanceof DescribeOperator) {
            return QueryForm.DESCRIBE;
        } else {
            throw new QueryEngine.IncompatibleQueryException("could not infer type of query from root node: " + root);
        }
    }

    private void addExtendedBindingName(final String from,
                                        final String to) {
        // projections of x onto x happen quite often; save some space
        if (from.equals(to)) {
            return;
        }

        if (null == extendedBindingNames) {
            extendedBindingNames = new HashMap<String, String>();
        }

        extendedBindingNames.put(from, to);
    }

    public LList<TriplePattern> getGraphPattern() {
        return graphPattern;
    }

    public Set<String> getBindingNames() {
        return bindingNames;
    }

    public Map<String, String> getExtendedBindingNames() {
        return extendedBindingNames;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    /**
     * @return an object which represents this query's DISTINCT/REDUCED, OFFSET, and LIMIT behavior
     */
    public SolutionSequenceModifier getSequenceModifier() {
        return sequenceModifier;
    }

    private void findPatternsInRoot(final QueryModelNode root,
                                    final Collection<StatementPattern> patterns) throws QueryEngine.IncompatibleQueryException {
        if (root instanceof Projection) {
            findPatterns((Projection) root, patterns);
        } else if (root instanceof Join) {
            findPatterns((Join) root, patterns);
        } else if (root instanceof Filter) {
            findPatterns((Filter) root, patterns);
        } else if (root instanceof Distinct) {
            sequenceModifier.makeDistinct();

            List<QueryModelNode> l = visitChildren(root);
            if (1 != l.size()) {
                throw new QueryEngine.IncompatibleQueryException("exactly one node expected beneath DISTINCT");
            }

            findPatternsInRoot(l.get(0), patterns);
        } else if (root instanceof Reduced) {
            sequenceModifier.makeReduced();

            List<QueryModelNode> l = visitChildren(root);
            if (1 != l.size()) {
                throw new QueryEngine.IncompatibleQueryException("exactly one node expected beneath DISTINCT");
            }

            findPatternsInRoot(l.get(0), patterns);
        } else if (root instanceof Slice) {
            Slice s = (Slice) root;
            if (s.hasLimit()) {
                sequenceModifier.setLimit(s.getLimit());
            }
            if (s.hasOffset()) {
                sequenceModifier.setOffset(s.getOffset());
            }

            List<QueryModelNode> l = visitChildren(root);
            if (1 != l.size()) {
                throw new QueryEngine.IncompatibleQueryException("exactly one node expected beneath Slice");
            }

            findPatternsInRoot(l.get(0), patterns);
        } else {
            throw new QueryEngine.IncompatibleQueryException("expected Projection or Distinct at root node of query; found " + root);
        }
    }

    private void findPatterns(final StatementPattern p,
                              final Collection<StatementPattern> patterns) {
        patterns.add(p);
    }

    private void findPatterns(final Join j,
                              final Collection<StatementPattern> patterns) throws QueryEngine.IncompatibleQueryException {
        for (QueryModelNode n : visitChildren(j)) {
            if (n instanceof StatementPattern) {
                findPatterns((StatementPattern) n, patterns);
            } else if (n instanceof Join) {
                findPatterns((Join) n, patterns);
            } else {
                throw new QueryEngine.IncompatibleQueryException("unexpected node: " + n);
            }
        }
    }

    private void findPatterns(final Filter f,
                              final Collection<StatementPattern> patterns) throws QueryEngine.IncompatibleQueryException {
        if (null == filters) {
            filters = new LinkedList<Filter>();
        }
        filters.add(f);

        List<QueryModelNode> filterChildren = visitChildren(f);
        if (2 != filterChildren.size()) {
            throw new QueryEngine.IncompatibleQueryException("expected exactly two nodes beneath filter");
        }

        QueryModelNode valueExpr = filterChildren.get(0);
        if (!(valueExpr instanceof ValueExpr)) {
            throw new QueryEngine.IncompatibleQueryException("expected value expression as first child of filter; found " + valueExpr);
        }

        checkFilterFunctionSupported((ValueExpr) valueExpr);

        QueryModelNode filterChild = filterChildren.get(1);
        if (filterChild instanceof Join) {
            findPatterns((Join) filterChild, patterns);
        } else if (filterChild instanceof StatementPattern) {
            findPatterns((StatementPattern) filterChild, patterns);
        } else {
            if (filterChild instanceof Filter) {
                //System.out.println("filterChild = " + filterChild);
                Filter childFilter = (Filter) filterChild;
                ValueExpr ve = childFilter.getCondition();
                System.out.println("ve = " + ve);
                //TupleExpr te = childFilter.getArg();
                //System.out.println("te = " + te);
            }

            throw new QueryEngine.IncompatibleQueryException("expected join or statement pattern beneath filter; found " + filterChild);
        }
    }

    private void checkFilterFunctionSupported(final ValueExpr expr) throws QueryEngine.IncompatibleQueryException {
        if (expr instanceof Not) {
            List<QueryModelNode> children = visitChildren(expr);
            if (1 != children.size()) {
                throw new QueryEngine.IncompatibleQueryException("expected exactly one node beneath NOT");
            }

            QueryModelNode valueExpr = children.get(0);
            if (!(valueExpr instanceof ValueExpr)) {
                throw new QueryEngine.IncompatibleQueryException("expected value expression as first child of NOT; found " + valueExpr);
            }

            checkFilterFunctionSupported((ValueExpr) valueExpr);
        } else {
            // EXISTS is specifically not (yet) supported; all other filter functions are assumed to be supported
            if (expr instanceof Exists) {
                throw new QueryEngine.IncompatibleQueryException("EXISTS and NOT EXISTS are not supported");
            }
        }
    }

    private void findPatterns(final Projection p,
                              final Collection<StatementPattern> patterns) throws QueryEngine.IncompatibleQueryException {
        List<QueryModelNode> l = visitChildren(p);

        Extension ext = null;

        for (QueryModelNode n : l) {
            if (n instanceof Extension) {
                ext = (Extension) n;
            } else if (n instanceof ProjectionElemList) {
                ProjectionElemList pl = (ProjectionElemList) n;
                for (ProjectionElem pe : pl.getElements()) {
                    bindingNames.add(pe.getSourceName());
                    addExtendedBindingName(pe.getSourceName(), pe.getTargetName());
                }
            }
        }

        if (null != ext) {
            //System.out.println("visiting children");
            l = visitChildren(ext);
        }

        for (QueryModelNode n : l) {
            if (n instanceof Join) {
                Join j = (Join) n;
                if (TupleExprs.containsProjection(j)) {
                    throw new QueryEngine.IncompatibleQueryException("join contains projection");
                }

                findPatterns(j, patterns);
            } else if (n instanceof StatementPattern) {
                findPatterns((StatementPattern) n, patterns);
            } else if (n instanceof Filter) {
                findPatterns((Filter) n, patterns);
            } else if (n instanceof ProjectionElemList) {
                // TODO: remind self when these are encountered and why they are ignored
                //LOGGER.info("ignoring " + n);
            } else if (n instanceof ExtensionElem) {
                ExtensionElem ee = (ExtensionElem) n;

                ValueExpr ve = ee.getExpr();
                if (ve instanceof ValueConstant) {
                    String name = ee.getName();
                    String target = extendedBindingNames.get(name);

                    if (null == target) {
                        throw new QueryEngine.IncompatibleQueryException("ExtensionElem does not correspond to a projection variable");
                    }

                    ValueConstant vc = (ValueConstant) ve;
                    if (null == constants) {
                        constants = new HashSet<Binding>();
                    }
                    constants.add(new BindingImpl(target, vc.getValue()));
                } else if (ve instanceof Var) {
                    // do nothing; the source-->target mapping is already in the extended binding names
                } else {
                    throw new QueryEngine.IncompatibleQueryException("expected ValueConstant or Var within ExtensionElem; found " + ve);
                }
            } else if (n instanceof Order) {
                throw new QueryEngine.IncompatibleQueryException("the ORDER BY modifier is not supported by SesameStream");
            } else {
                throw new QueryEngine.IncompatibleQueryException("unexpected type: " + n.getClass());
            }
        }
    }

    private List<QueryModelNode> visit(final QueryModelNode node) {
        //System.out.println("### visit");
        List<QueryModelNode> visited = new LinkedList<QueryModelNode>();
        SimpleQueryModelVisitor v = new SimpleQueryModelVisitor(visited);

        try {
            node.visit(v);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        //for (QueryModelNode n : visited) {
        //    System.out.println("node: " + n);
        //}

        return visited;
    }

    private List<QueryModelNode> visitChildren(final QueryModelNode node) {
        //System.out.println("### visitChildren");
        List<QueryModelNode> visited = new LinkedList<QueryModelNode>();
        SimpleQueryModelVisitor v = new SimpleQueryModelVisitor(visited);

        try {
            node.visitChildren(v);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        /*
        for (QueryModelNode n : visited) {
            System.out.println("node: " + n);
        }
        //*/

        return visited;
    }

}
