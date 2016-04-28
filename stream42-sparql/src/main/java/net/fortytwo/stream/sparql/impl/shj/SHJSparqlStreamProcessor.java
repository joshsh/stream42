package net.fortytwo.stream.sparql.impl.shj;

import net.fortytwo.stream.BasicSubscription;
import net.fortytwo.stream.model.VariableOrConstant;
import net.fortytwo.stream.shj.ExpirationManager;
import net.fortytwo.stream.shj.GraphPattern;
import net.fortytwo.stream.shj.Query;
import net.fortytwo.stream.shj.QueryContext;
import net.fortytwo.stream.shj.QueryIndex;
import net.fortytwo.stream.shj.Solution;
import net.fortytwo.stream.shj.TuplePattern;
import net.fortytwo.stream.sparql.SparqlQuery;
import net.fortytwo.stream.sparql.SparqlStreamProcessor;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * A space-efficient continuous SPARQL query engine which uses a symmetric hash join
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SHJSparqlStreamProcessor extends SparqlStreamProcessor<Query<String, Value>> {

    protected final ExpirationManager<Solution<Value>> solutionExpirationManager;
    protected final ExpirationManager<Query<String, Value>> queryExpirationManager;
    protected final QueryContext<String, Value> context;
    protected final QueryIndex<String, Value> queryIndex;

    public SHJSparqlStreamProcessor() {
        super();

        queryExpirationManager = new ExpirationManager<Query<String, Value>>() {
            @Override
            protected long getNow() {
                return SHJSparqlStreamProcessor.this.getNow();
            }
        };
        queryExpirationManager.setVerbose(true);

        solutionExpirationManager = new ExpirationManager<Solution<Value>>() {
            @Override
            protected long getNow() {
                return SHJSparqlStreamProcessor.this.getNow();
            }
        };
        solutionExpirationManager.setVerbose(true);

        context = new QueryContext<>(queryExpirationManager, solutionExpirationManager);

        queryIndex = new QueryIndex<>(context);
    }

    @Override
    public void clear() {
        // TODO: consider using a shared lock to avoid a race condition
        queryIndex.clear();
        context.clear();
    }

    @Override
    protected boolean addTupleInternal(Value[] tuple, int ttl, long now) {
        long expirationTime = toExpirationTime(ttl, now);

        return queryIndex.add(tuple, expirationTime);
    }

    @Override
    protected void register(BasicSubscription<SparqlQuery, Query<String, Value>, BindingSet> subscription) {
        Query<String, Value> query = subscription.getQuery();
        queryIndex.add(query);
    }

    @Override
    public void unregister(BasicSubscription<SparqlQuery, Query<String, Value>, BindingSet> subscription) throws IOException {
        queryIndex.remove(subscription.getQuery());
    }

    @Override
    public boolean renew(BasicSubscription<SparqlQuery, Query<String, Value>, BindingSet> subscription, int ttl)
            throws IOException {

        throw new UnsupportedOperationException(
                "query renewal is not yet supported; remove the query and add a new one");
    }

    @Override
    protected void visitQueryPatterns(Query<String, Value> query, Consumer<VariableOrConstant<String, Value>[]> visitor) {
        for (TuplePattern<String, Value> tuplePattern : query.getGraphPattern().getPatterns()) {
            visitor.accept(tuplePattern.getPattern());
        }
    }

    @Override
    protected BasicSubscription<SparqlQuery, Query<String, Value>, BindingSet> createSubscriptionInternal(
            SparqlQuery sparqlQuery,
            List<VariableOrConstant<String, Value>[]> patterns,
            long expirationTime,
            BiConsumer<BindingSet, Long> consumer) {

        final BasicSubscription<SparqlQuery, Query<String, Value>, BindingSet> subscription
                = new BasicSubscription<>(sparqlQuery, null, null, this);

        BiConsumer<Map<String, Value>, Long> solutionHandler = new BiConsumer<Map<String, Value>, Long>() {
            @Override
            public void accept(Map<String, Value> mapping, Long expirationTime) {
                BindingSet solution = toBindingSet(mapping);
                try {
                    handleCandidateSolution(subscription, solution, expirationTime);
                } catch (IOException e) {
                    logger.log(Level.WARNING, "failed to handle solution " + solution, e);
                }
            }
        };

        Query<String, Value> query;

        TuplePattern<String, Value>[] tuplePatterns = new TuplePattern[patterns.size()];
        int i = 0;
        for (VariableOrConstant<String, Value>[] p : patterns) {
            tuplePatterns[i++] = new TuplePattern<>(p);
        }

        GraphPattern<String, Value> graphPattern = new GraphPattern<>(tuplePatterns);
        query = new Query<>(
                graphPattern,
                expirationTime,
                context.getQueryExpirationManager(),
                solutionHandler);

        subscription.setQuery(query);
        subscription.setSolutionConsumer(consumer);

        return subscription;
    }

    private BindingSet toBindingSet(final Map<String, Value> mapping) {

        MapBindingSet bs = new MapBindingSet();
        for (Map.Entry<String, Value> e : mapping.entrySet()) {
            bs.addBinding(e.getKey(), e.getValue());
        }

        return bs;
    }
}
