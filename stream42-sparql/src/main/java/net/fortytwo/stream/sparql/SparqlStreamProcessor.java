package net.fortytwo.stream.sparql;

import net.fortytwo.linkeddata.LinkedDataCache;
import net.fortytwo.stream.BasicSubscription;
import net.fortytwo.stream.Subscription;
import net.fortytwo.stream.model.LList;
import net.fortytwo.stream.model.VariableOrConstant;
import net.fortytwo.stream.sparql.etc.FilterEvaluator;
import org.openrdf.model.IRI;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.SimpleValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * An RDF stream processor with SPARQL 1.1 support.
 * The processor consumes SPARQL queries and RDF statements, producing SPARQL solutions.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class SparqlStreamProcessor<Q> extends RDFStreamProcessor<SparqlQuery, Q> {

    private static long reducedModifierCapacity = 1000;

    private final FilterEvaluator filterEvaluator;

    private ExecutorService linkedDataService;
    // note: these threads are in addition to any threads created externally
    private final int httpThreadPoolSize = Runtime.getRuntime().availableProcessors() + 1;

    private LinkedDataCache linkedDataCache;

    /**
     * @return the number of distinct solutions which each query subscription can store before it begins recycling them
     * For a SELECT DISTINCT query, the set of distinct solution grows without bound,
     * but a duplicate answer will never appear in the output stream.
     * However, for a SELECT REDUCED query, the set of distinct solutions is limited in size().
     * This is much safer with respect to memory consumption,
     * although duplicate solutions may eventually appear in the output stream.
     */
    public static long getReducedModifierCapacity() {
        return reducedModifierCapacity;
    }

    /**
     * Sets the REDUCED capacity.
     *
     * @param capacity the number of distinct solutions which each query subscription can store
     *                 before it begins recycling them
     */
    public static void setReducedModifierCapacity(final long capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("unreasonable REDUCED capacity value: " + capacity);
        }

        reducedModifierCapacity = capacity;
    }

    protected SparqlStreamProcessor() {
        ValueFactory valueFactory = SimpleValueFactory.getInstance();
        filterEvaluator = new FilterEvaluator(valueFactory);
    }

    protected abstract void visitQueryPatterns(Q query,
                                               Consumer<VariableOrConstant<String, Value>[]> visitor);

    protected abstract boolean addTupleInternal(Value[] tuple, int ttl, long now);

    protected abstract void register(BasicSubscription<SparqlQuery, Q, BindingSet> subscription);

    protected abstract BasicSubscription<SparqlQuery, Q, BindingSet> createSubscriptionInternal(
            SparqlQuery q,
            List<VariableOrConstant<String, Value>[]> patterns,
            long expirationTime,
            BiConsumer<BindingSet, Long> consumer);

    @Override
    protected boolean addTuple(Value[] tuple, int ttl, long now) {
        boolean changed = addTupleInternal(tuple, ttl, now);

        // cue the Linked Data cache to dereference the subject and object IRIs of the statement,
        // but only if at least one pattern in the index has matched the tuple
        if (changed && null != linkedDataCache) {
            triggerLinkedDataCache(tuple);
        }

        return changed;
    }

    public RDFHandler createRDFHandler(final int ttl) {
        return new RDFHandler() {
            public void startRDF() throws RDFHandlerException {
                // do nothing
            }

            public void endRDF() throws RDFHandlerException {
                // do nothing
            }

            public void handleNamespace(String s, String s1) throws RDFHandlerException {
                // do nothing
            }

            public void handleStatement(Statement s) throws RDFHandlerException {
                try {
                    addInputs(ttl, s);
                } catch (Throwable t) {
                    throw new RDFHandlerException(t);
                }
            }

            public void handleComment(String s) throws RDFHandlerException {
                // do nothing
            }
        };
    }

    /**
     * Adds a Linked Data fetching and caching layer to this query engine.
     * Once added, the Linked Data cache will listen for new triple patterns indexed by this query engine,
     * and issue corresponding HTTP requests for additional information about IRIs in those patterns.
     * Any RDF statements from retrieved documents are passed into the query engine, where they may contribute
     * to query results and/or partial solutions, and may trigger further HTTP requests.
     *
     * @param cache a collection of caching metadata about Linked Data already retrieved
     */
    public void setLinkedDataCache(final LinkedDataCache cache) {
        this.linkedDataCache = cache;
        this.linkedDataCache.setAutoCommit(true);

        // Give the collected Linked Data infinite (= 0) time-to-live.
        // Results derived from this data will never expire.
        final int staticTtl = 0;

        LinkedDataCache.DataStore store = sc -> createRDFSink(staticTtl);
        cache.setDataStore(store);

        if (null != linkedDataService) {
            linkedDataService.shutdown();
        }

        linkedDataService = Executors.newFixedThreadPool(httpThreadPoolSize);
    }

    /**
     * Adds a new query, as a pre-parsed TupleExpr to this query engine, returning a subscription
     *
     * @param ttl       the time-to-live in seconds
     * @param tupleExpr the query to add
     * @param consumer  a handler for future query answers
     * @return a subscription for computation of future query answers
     * @throws IncompatibleQueryException if the syntax of the query is not supported by this engine
     */
    public Subscription addQuery(final int ttl,
                                 final TupleExpr tupleExpr,
                                 final BiConsumer<BindingSet, Long> consumer)
            throws IncompatibleQueryException, IOException {

        SparqlQuery sparqlQuery = new SparqlQuery(tupleExpr);
        return addQueryNative(ttl, sparqlQuery, consumer);
    }

    @Override
    protected BasicSubscription<SparqlQuery, Q, BindingSet> createSubscription(final int ttl,
                                                                               final SparqlQuery sparqlQuery,
                                                                               final BiConsumer<BindingSet, Long> consumer) {
        long expirationTime = toExpirationTime(ttl, getNow());

        List<VariableOrConstant<String, Value>[]> patterns = new LinkedList<>();
        LList<VariableOrConstant<String, Value>[]> tPatterns = sparqlQuery.getTriplePatterns();
        while (!tPatterns.isNil()) {
            patterns.add(tPatterns.getValue());
            tPatterns = tPatterns.getRest();
        }

        BasicSubscription<SparqlQuery, Q, BindingSet> sub
                = createSubscriptionInternal(sparqlQuery, patterns, expirationTime, consumer);

        if (null != linkedDataCache) {
            // invalidate the Linked Data cache when a new query is added, as the evaluation of the new query may
            // require statements from data sources which have already been processed
            clearLinkedDataCache();

            visitQueryPatterns(sub.getQuery(), this::triggerLinkedDataCache);
        }

        register(sub);

        return sub;
    }

    @Override
    protected SparqlQuery parseQuery(String queryStr) throws InvalidQueryException, IncompatibleQueryException {

        // TODO
        String baseIRI = "http://example.org/baseIRI";

        ParsedQuery parsedQuery;
        try {
            parsedQuery = QueryParserUtil.parseQuery(
                    QueryLanguage.SPARQL,
                    queryStr,
                    baseIRI);
        } catch (MalformedQueryException e) {
            throw new InvalidQueryException(e);
        }

        return new SparqlQuery(parsedQuery.getTupleExpr());
    }

    // this BindingSet may contain non-selected and pre-projected variables, suitable
    // for filtering, but not yet a final query result
    protected void handleCandidateSolution(BasicSubscription<SparqlQuery, Q, BindingSet> subscription,
                                           BindingSet bs,
                                           long expirationTime) throws IOException {

        SparqlQuery sparqlQuery = subscription.getConstraint();

        // After queries are removed from the query index, a few more query answers (from the last added statement,
        // which completed an ASK query, for example) may arrive here and need to be excluded
        if (!subscription.isActive()) {
            return;
        }

        List<Filter> filters = sparqlQuery.getFilters();

        // apply all filters, discarding this potential solution if any filter rejects it
        if (null != filters) {
            for (Filter f : filters) {
                try {
                    if (!filterEvaluator.applyFilter(f, bs)) {
                        return;
                    }
                } catch (QueryEvaluationException e) {
                    logger.log(Level.SEVERE, "query evaluation error while applying filter", e);
                    return;
                }
            }
        }

        MapBindingSet solution = new MapBindingSet();

        // remove non-selected variables and project the final names of the selected variables
        for (String key : sparqlQuery.getBindingNames()) {
            Value value = bs.getValue(key);
            if (null != value) {
                if (null != sparqlQuery.getExtendedBindingNames()) {
                    String keyp = sparqlQuery.getExtendedBindingNames().get(key);
                    if (null != keyp) {
                        key = keyp;
                    }
                }
                solution.addBinding(key, value);
            }
        }

        // adding constants after filter application assumes that one will never filter on constants
        if (null != sparqlQuery.getConstants()) {
            for (Map.Entry<String, Value> e : sparqlQuery.getConstants().entrySet()) {
                solution.addBinding(e.getKey(), e.getValue());
            }
        }

        SparqlQuery.QueryForm form = sparqlQuery.getQueryForm();

        // note: SesameStream's response to an ASK query which evaluates to true is an empty BindingSet
        // A result of false is never produced, as data sources are assumed to be infinite streams
        if (SparqlQuery.QueryForm.SELECT == form) {
            if (sparqlQuery.getSequenceModifier().trySolution(solution, subscription)) {
                handleSolution(subscription.getSolutionConsumer(), solution, expirationTime);
            }
        } else {
            throw new IllegalStateException("unexpected query form: " + form);
        }
    }

    private void triggerLinkedDataCache(final VariableOrConstant<String, Value>[] pattern) {
        if (pattern.length >= 1) {
            Value subject = pattern[0].getConstant();
            if (null != subject && subject instanceof IRI) {
                indexLinkedDataIri((IRI) subject);
            }

            if (pattern.length >= 3) {
                Value object = pattern[2].getConstant();
                if (null != object && object instanceof IRI) {
                    indexLinkedDataIri((IRI) object);
                }
            }
        }
    }

    private void triggerLinkedDataCache(final Value[] tuple) {
        if (tuple.length >= 3) {
            Value subject = tuple[0];
            Value object = tuple[2];

            if (subject instanceof IRI && object instanceof IRI) {
                indexLinkedDataIri((IRI) subject);
                indexLinkedDataIri((IRI) object);
            }
        }
    }

    // note: as the indexing of Linked Data IRIs may trigger the fetching and rdfization of data sources,
    // and as this occurs a pooled thread distinct from calling thread, query answers may be produced in that thread.
    private void indexLinkedDataIri(final IRI iri) {
        if (isHttpIri(iri)) {
            linkedDataService.execute(() -> {
                try {
                    linkedDataCache.retrieve(iri, linkedDataCache.getSailConnection());
                } catch (IOException e) {
                    logger.log(Level.WARNING, "failed to retrieve IRI", e);
                }
            });
        }
    }

    // pre-filter IRIs so as to avoid needlessly creating executor tasks
    private boolean isHttpIri(final IRI iri) {
        String s = iri.stringValue();
        return s.startsWith("http://") || s.startsWith("https://");
    }

    private void clearLinkedDataCache() {
        linkedDataCache.clear();
    }

    private Consumer<Statement> createRDFSink(final int ttl) {
        return statement -> addInputs(ttl, statement);
    }
}
