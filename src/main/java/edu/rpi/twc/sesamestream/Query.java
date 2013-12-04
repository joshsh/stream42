package edu.rpi.twc.sesamestream;

import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.Distinct;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Order;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.helpers.TupleExprs;

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

    private Set<BindingSet> distinctSet;

    public Query(final TupleExpr expr,
                 final QueryEngine.TriplePatternDeduplicator deduplicator) throws IncompatibleQueryException {
        bindingNames = new HashSet<String>();

        graphPattern = LList.NIL;

        List<QueryModelNode> l = visit(expr);
        if (l.size() != 1) {
            throw new IncompatibleQueryException("multiple root nodes");
        }
        QueryModelNode root = l.iterator().next();
        Projection p = getRootProjection(root);
        for (ProjectionElem el : p.getProjectionElemList().getElements()) {
            addExtendedBindingName(el.getSourceName(), el.getTargetName());
        }

        // TODO: eliminate redundant patterns
        Collection<StatementPattern> patterns = new LinkedList<StatementPattern>();
        findPatterns(p, patterns);

        for (StatementPattern pat : patterns) {
            graphPattern = graphPattern.push(deduplicator.deduplicate(new TriplePattern(pat)));
        }
    }

    private void makeDistinct() {
        if (null == distinctSet) {
            distinctSet = new HashSet<BindingSet>();
        }
    }

    private Projection getRootProjection(final QueryModelNode root) throws IncompatibleQueryException {
        if (root instanceof Projection) {
            return (Projection) root;
        } else if (root instanceof Distinct) {
            makeDistinct();

            List<QueryModelNode> l = visitChildren(root);
            if (1 != l.size()) {
                throw new IncompatibleQueryException("exactly one node expected beneath DISTINCT");
            }

            QueryModelNode child = l.get(0);
            return getRootProjection(child);
        } else {
            throw new IncompatibleQueryException("expected Projection at root node of query; found " + root);
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
     * @return if this query uses DISTINCT, the set of distinct solutions already found.  Otherwise <code>null</code>
     */
    public Set<BindingSet> getDistinctSet() {
        return distinctSet;
    }

    private void findPatterns(final StatementPattern p,
                              final Collection<StatementPattern> patterns) {
        patterns.add(p);
    }

    private void findPatterns(final Join j,
                              final Collection<StatementPattern> patterns) throws IncompatibleQueryException {
        for (QueryModelNode n : visitChildren(j)) {
            if (n instanceof StatementPattern) {
                findPatterns((StatementPattern) n, patterns);
            } else if (n instanceof Join) {
                findPatterns((Join) n, patterns);
            } else {
                throw new IncompatibleQueryException("unexpected node: " + n);
            }
        }
    }

    private void findPatterns(final Filter f,
                              final Collection<StatementPattern> patterns) throws IncompatibleQueryException {
        if (null == filters) {
            filters = new LinkedList<Filter>();
        }
        filters.add(f);

        List<QueryModelNode> filterChildren = visitChildren(f);
        if (2 != filterChildren.size()) {
            throw new IncompatibleQueryException("expected exactly two nodes beneath filter");
        }

        QueryModelNode valueExpr = filterChildren.get(0);
        if (!(valueExpr instanceof ValueExpr)) {
            throw new IncompatibleQueryException("expected value expression as first child of filter; found " + valueExpr);
        }
        QueryModelNode filterChild = filterChildren.get(1);
        if (filterChild instanceof Join) {
            findPatterns((Join) filterChild, patterns);
        } else if (filterChild instanceof StatementPattern) {
            findPatterns((StatementPattern) filterChild, patterns);
        } else {
            throw new IncompatibleQueryException("expected join or statement pattern beneath filter; found " + filterChild);
        }
    }

    private void findPatterns(final Projection p,
                              final Collection<StatementPattern> patterns) throws IncompatibleQueryException {
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
                    throw new IncompatibleQueryException("join contains projection");
                }

                findPatterns(j, patterns);
            } else if (n instanceof StatementPattern) {
                findPatterns((StatementPattern) n, patterns);
            } else if (n instanceof Filter) {
                findPatterns((Filter) n, patterns);
            } else if (n instanceof ProjectionElemList) {
                // TODO: remind self why these are ignored
                //LOGGER.info("ignoring " + n);
            } else if (n instanceof ExtensionElem) {
                // TODO: remind self why these are ignored
                //LOGGER.info("ignoring " + n);
            } else if (n instanceof Order) {
                throw new IncompatibleQueryException("the ORDER BY modifier is not supported by SesameStream");
            } else {
                throw new IncompatibleQueryException("unexpected type: " + n.getClass());
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

    /**
     * An exception thrown when a valid SPARQL query is incompatible with SesameStream.
     * Only a subset of the SPARQL standard is supported.
     */
    public static class IncompatibleQueryException extends Exception {
        public IncompatibleQueryException() {
            super();
        }

        public IncompatibleQueryException(final String message) {
            super(message);
        }
    }
}
