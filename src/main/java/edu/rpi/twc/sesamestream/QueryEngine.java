package edu.rpi.twc.sesamestream;

import net.fortytwo.linkeddata.CacheEntry;
import net.fortytwo.linkeddata.LinkedDataCache;
import net.fortytwo.ripple.RippleException;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * A SesameStream continuous SPARQL query engine.
 * The engine receives SPARQL queries in advance of the data they query against, and answers them in a forward-chaining fashion.
 * Optionally (depending on {@link edu.rpi.twc.sesamestream.SesameStream} settings), performance data is generated in the process.
 * <p/>
 * <p/>
 * Current assumptions:
 * 1) only simple, conjunctive SELECT queries, without filters, unions, optionals, etc.
 * 2) no GRAPH constraints
 * 3) no duplicate queries
 * 4) no duplicate statements
 * 5) no duplicate triple patterns in a query
 * 6) single query at a time
 * 7) no combo of queries and statements such that multiple triple patterns match the same statement
 * 8 queries are only added, never removed
 * 9) statements are only added, never removed
 * 10) a statement will never match more than one triple pattern, per query, at a time
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryEngine {
    private static final Logger LOGGER = Logger.getLogger(QueryEngine.class.getName());

    //private final Map<TriplePattern, Collection<PartialSolution>> oldIndex;
    private final TripleIndex index;

    private final List<PartialSolution> intermediateSolutionBuffer = new LinkedList<PartialSolution>();

    // A buffer for statements added as the result of adding other statements (e.g. in response to Linked Data fetches)
    // This avoids concurrent modification exceptions.
    private final List<Statement> statementBuffer = new LinkedList<Statement>();

    // TODO: this is a bit of a hack, and a waste of space
    private final Map<TriplePattern, TriplePattern> uniquePatterns;
    private final TriplePatternDeduplicator deduplicator;

    private LinkedDataCache linkedDataCache;
    private SailConnection linkedDataCacheConnection;

    private final SolutionHandler binder = new SolutionHandler() {
        public void handle(final PartialSolution ps,
                           final TriplePattern matched,
                           final VarList newBindings) {
            increment(countBindingOps, false);

            handleSolution(ps, matched, newBindings);
        }
    };

    private boolean logHasChanged = false;

    private long timeCurrentOperationBegan;

    private final Counter
            countQueries = new Counter(),
            countTriplePatterns = new Counter(),
            countStatements = new Counter(),
            countPartialSolutions = new Counter(),
            countSolutions = new Counter(),
            countIndexOps = new Counter(),
            countBindingOps = new Counter(),
            countReplaceOps = new Counter();

    private final FilterEvaluator filterEvaluator;

    /**
     * Creates a new query engine with an empty index
     */
    public QueryEngine() {
        index = new TripleIndex();
        //oldIndex = new HashMap<TriplePattern, Collection<PartialSolution>>();
        uniquePatterns = new HashMap<TriplePattern, TriplePattern>();
        deduplicator = new TriplePatternDeduplicator();

        ValueFactory valueFactory = new ValueFactoryImpl();
        filterEvaluator = new FilterEvaluator(valueFactory);

        clear();
    }

    /**
     * Adds a Linked Data fetching and caching layer to this query engine.
     * Once added, the Linked Data cache will listen for new triple patterns indexed by this query engine,
     * and issue corresponding HTTP requests for additional information about URIs in those patterns.
     * Any RDF statements from retrieved documents are passed into the query engine, where they may contribute
     * to query results and/or partial solutions, and may trigger further HTTP requests.
     *
     * @param cache a collection of caching metadata about Linked Data already retrieved
     * @param sail  the RDF component of the caching metadata
     * @throws SailException if a cache-level query exception occurs
     */
    public void setLinkedDataCache(final LinkedDataCache cache,
                                   final Sail sail) throws SailException {
        this.linkedDataCache = cache;
        this.linkedDataCache.setAutoCommit(true);
        this.linkedDataCacheConnection = sail.getConnection();
    }

    /**
     * Gets the index, or in-memory database, of this query engine
     *
     * @return the index, or in-memory database, of this query engine.
     *         It should not be necessary to interact with the index directly, but it is possible to inspect the index via
     *         print and visit methods.
     */
    public TripleIndex getIndex() {
        return index;
    }

    /**
     * Removes all triple patterns along with their associated partial solutions, queries, and subscriptions
     */
    public void clear() {
        // TODO: index.clear()
        //oldIndex.clear();
        uniquePatterns.clear();

        countQueries.reset();
        countTriplePatterns.reset();
        countStatements.reset();
        countPartialSolutions.reset();
        countSolutions.reset();
        countIndexOps.reset();
        countBindingOps.reset();
        countReplaceOps.reset();

        logHeader();
    }

    /**
     * Adds a new query (subscription) to this query engine
     *
     * @param t the query to add
     * @param h a handler for eventual results of the query
     * @throws Query.IncompatibleQueryException
     *          if the syntax of the query is not supported by this engine
     */
    public void addQuery(final TupleExpr t,
                         final BindingSetHandler h) throws Query.IncompatibleQueryException {
        mutexUp();

        increment(countQueries, true);
        timeCurrentOperationBegan = System.currentTimeMillis();
        //System.out.println("query:\t" + t);

        Query q = new Query(t, deduplicator);

        Subscription s = new Subscription(q, h);

        PartialSolution query = new PartialSolution(s);
        addPartialSolution(query);
        flushPartialSolutions();

        logEntry();

        mutexDown();
    }

    /**
     * Adds a new statement to this query engine.
     * Depending on the queries registered with this engine,
     * the statement will either be discarded as irrelevant to the queries,
     * trigger the creation of partial solutions which are stored in anticipation of further statements,
     * or trigger the production query results
     *
     * @param s the statement to add
     */
    public void addStatement(final Statement s) {
        if (mutex) {
            //System.out.println("queueing statement: " + s);
            statementBuffer.add(s);
        } else {
            mutexUp();
            //System.out.println("adding statement: " + s);

            increment(countStatements, false);
            timeCurrentOperationBegan = System.currentTimeMillis();
            //System.out.println("statement:\t" + s);

            index.match(toVarList(s), s, binder);

            flushPartialSolutions();

            logEntry();

            mutexDown();
        }
    }

    private boolean mutex;

    private void mutexUp() {
        mutex = true;
    }

    private void mutexDown() {
        mutex = false;

        if (statementBuffer.size() > 0) {
            addStatement(statementBuffer.remove(0));
        }
    }

    private void addPartialSolution(final PartialSolution ps) {
        increment(countPartialSolutions, true);
        //System.out.println("intermediate result:\t" + q);

        intermediateSolutionBuffer.add(ps);
    }

    private void flushPartialSolutions() {
        for (PartialSolution q : intermediateSolutionBuffer) {
            LList<TriplePattern> cur = q.getGraphPattern();
            while (!cur.isNil()) {
                indexTriplePattern(cur.getValue(), q);
                cur = cur.getRest();
            }
        }
        intermediateSolutionBuffer.clear();
    }

    private VarList toVarList(final Statement s) {
        VarList l = null;

        l = new VarList(null, s.getObject(), l);
        l = new VarList(null, s.getPredicate(), l);
        l = new VarList(null, s.getSubject(), l);
        return l;
    }

    private VarList toVarList(TriplePattern p) {
        VarList l = null;
        l = new VarList(p.getObject().getName(), p.getObject().getValue(), l);
        l = new VarList(p.getPredicate().getName(), p.getPredicate().getValue(), l);
        l = new VarList(p.getSubject().getName(), p.getSubject().getValue(), l);
        return l;
    }

    private void indexTriplePattern(final TriplePattern p,
                                    final PartialSolution q) {
        increment(countIndexOps, false);

        VarList l = toVarList(p);
        index.index(p, l, q);

        if (null != linkedDataCache) {
            Value s = p.getSubject().getValue();
            Value o = p.getObject().getValue();

            try {
                if (null != s && s instanceof URI) {
                    System.out.println("looking up subject: " + s);
                    CacheEntry.Status status = linkedDataCache.retrieveUri((URI) s, linkedDataCacheConnection);
                    System.out.println("\t" + status);
                }

                if (null != o && o instanceof URI) {
                    System.out.println("looking up object: " + o);
                    linkedDataCache.retrieveUri((URI) o, linkedDataCacheConnection);
                }
            } catch (RippleException e) {
                LOGGER.severe(e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void handleSolution(final PartialSolution ps,
                                final TriplePattern matched,
                                final VarList newBindings) {
        //System.out.println("triple pattern satisfied: " + satisfiedPattern + " with bindings " + newBindings);

        // if a partial solution has only one triple pattern, and an incoming statement has just matched against it,
        // then a query solution has just been found
        // TODO: optimize this length = 1 check
        if (1 == ps.getGraphPattern().length()) {
            //System.out.println("producing solution: " + newBindings);
            produceSolution(ps, VarList.union(newBindings, ps.getBindings()));
        }

        // if there is more than one triple pattern in the partial solution, then the remaining patterns still
        // need to be matched by other statements before we have a complete solution.  Create and index a new partial
        // solution which adds the new bindings and subtracts the already-matched triple pattern.
        else {
            //System.out.println("creating new partial solution");
            LList<TriplePattern> nextPatterns = LList.NIL;

            VarList nextBindings = VarList.union(newBindings, ps.getBindings());

            LList<TriplePattern> cur = ps.getGraphPattern();
            while (!cur.isNil()) {
                TriplePattern t = cur.getValue();
                // Note: comparison with != is appropriate here thanks to de-duplication of triple patterns
                if (t != matched) {
                    TriplePattern p = replace(t, newBindings);

                    if (null == p) {
                        nextPatterns = nextPatterns.push(t);
                    } else {
                        nextPatterns = nextPatterns.push(deduplicator.deduplicate(p));
                    }
                }
                cur = cur.getRest();
            }

            addPartialSolution(
                    new PartialSolution(ps.getSubscription(), nextPatterns, nextBindings));
        }
    }

    // Note: this operation doesn't need to be counted; it happens exactly once for each solution (which are counted)
    private void produceSolution(final PartialSolution r,
                                 final VarList nextBindings) {
        List<Filter> filters = r.getSubscription().getQuery().getFilters();
        BindingSet solution;

        if (null == filters) {
            // if there are no filters, it is more efficient not to create an intermediate BindingSet
            solution = toSolutionBindings(nextBindings, r);
        } else {
            // this BindingSet may contain non-selected and pre-projected variables, suitable
            // for filtering, but not yet a final query result
            BindingSet bs = toFilterableBindingSet(nextBindings);

            // apply all filters, discarding this BindingSet if any filter rejects it
            for (Filter f : filters) {
                try {
                    if (!filterEvaluator.applyFilter(f, bs)) {
                        return;
                    }
                } catch (QueryEvaluationException e) {
                    LOGGER.severe("query evaluation error while applying filter");
                    e.printStackTrace(System.err);
                    return;
                }
            }

            // after applying filters, remove non-selected variables and project
            // the final names of the selected variables
            solution = toSolutionBindings(bs, r);
        }

        handleSolution(r.getSubscription().getHandler(), solution);
    }

    private BindingSet toFilterableBindingSet(final VarList bindings) {

        MapBindingSet bs = new MapBindingSet();
        VarList cur = bindings;
        while (null != cur) {
            bs.addBinding(cur.getName(), cur.getValue());

            cur = cur.getRest();
        }

        return bs;
    }

    private BindingSet toSolutionBindings(final BindingSet bs,
                                          final PartialSolution r) {
        MapBindingSet newBindings = new MapBindingSet();

        Set<String> names = r.getSubscription().getQuery().getBindingNames();
        Map<String, String> extNames = r.getSubscription().getQuery().getExtendedBindingNames();

        for (String name : names) {
            Value val = bs.getValue(name);
            if (null == val) {
                LOGGER.warning("no value bound to variable '" + name + "' in solution");
                continue;
            }

            String finalName;

            if (null == extNames) {
                finalName = name;
            } else {
                finalName = extNames.get(name);

                if (null == finalName) {
                    finalName = name;
                }
            }

            newBindings.addBinding(finalName, val);
        }

        return newBindings;
    }

    private BindingSet toSolutionBindings(final VarList bindings,
                                          final PartialSolution r) {
        MapBindingSet newBindings = new MapBindingSet();

        Set<String> names = r.getSubscription().getQuery().getBindingNames();
        Map<String, String> extNames = r.getSubscription().getQuery().getExtendedBindingNames();

        VarList cur = bindings;
        while (null != cur) {
            String name = cur.getName();

            if (names.contains(name)) {
                String finalName;

                if (null == extNames) {
                    finalName = name;
                } else {
                    finalName = extNames.get(name);

                    if (null == finalName) {
                        finalName = name;
                    }
                }

                newBindings.addBinding(finalName, cur.getValue());
            }

            cur = cur.getRest();
        }

        return newBindings;
    }

    // TODO: currently not efficient
    private TriplePattern replace(final TriplePattern p,
                                  final VarList bindings) {
        increment(countReplaceOps, false);

        Var newSubject = p.getSubject();
        Var newPredicate = p.getPredicate();
        Var newObject = p.getObject();
        boolean changed = false;

        VarList cur = bindings;
        while (null != cur) {
            if (!p.getSubject().hasValue() && p.getSubject().getName().equals(cur.getName())) {
                newSubject = cur.asVar();
                changed = true;
            }

            if (!p.getPredicate().hasValue() && p.getPredicate().getName().equals(cur.getName())) {
                newPredicate = cur.asVar();
                changed = true;
            }

            if (!p.getObject().hasValue() && p.getObject().getName().equals(cur.getName())) {
                newObject = cur.asVar();
                changed = true;
            }

            cur = cur.getRest();
        }

        return changed
                ? new TriplePattern(newSubject, newPredicate, newObject)
                : null;
    }

    private void increment(final Counter counter,
                           final boolean logChange) {
        if (SesameStream.getDoPerformanceMetrics()) {
            counter.increment();
            if (logChange) {
                logHasChanged = true;
            }
        }
    }

    private void logHeader() {
        if (SesameStream.getDoPerformanceMetrics()) {
            System.out.println("LOG\ttime1,time2,queries,statements,patterns,partial,solutions,indexOps,bindingOps,replaceOps");
        }
    }

    private void logEntry() {
        if (SesameStream.getDoPerformanceMetrics()) {
            if (!SesameStream.getDoUseCompactLogFormat() || logHasChanged) {
                System.out.println("LOG\t" + timeCurrentOperationBegan
                        + "," + System.currentTimeMillis()
                        + "," + countQueries.getCount()
                        + "," + countStatements.getCount()
                        + "," + countTriplePatterns.getCount()
                        + "," + countPartialSolutions.getCount()
                        + "," + countSolutions.getCount()
                        + "," + countIndexOps.getCount()
                        + "," + countBindingOps.getCount()
                        + "," + countReplaceOps.getCount());

                logHasChanged = false;
            }
        }
    }

    private static String toString(final BindingSet b) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String n : b.getBindingNames()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(n).append(":").append(b.getValue(n));
        }

        return sb.toString();
    }

    private void handleSolution(final BindingSetHandler handler,
                                final BindingSet solution) {
        increment(countSolutions, true);
        if (SesameStream.getDoPerformanceMetrics()) {
            System.out.println("SOLUTION\t" + System.currentTimeMillis() + "\t"
                    + QueryEngine.toString(solution));
        }

        handler.handle(solution);
    }

    /**
     * An object to ensure that triple patterns created by replacement are not duplicates
     */
    public class TriplePatternDeduplicator {
        public TriplePattern deduplicate(final TriplePattern t) {
            TriplePattern t2 = uniquePatterns.get(t);
            if (null == t2) {
                increment(countTriplePatterns, true);
                return t;
            } else {
                return t2;
            }
        }
    }

    private static class Counter {
        private long count = 0;

        public void increment() {
            count++;
        }

        public void reset() {
            count = 0;
        }

        public long getCount() {
            return count;
        }
    }
}
