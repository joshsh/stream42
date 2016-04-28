package net.fortytwo.stream.sparql.impl.caching;

import net.fortytwo.stream.StreamProcessor;
import net.fortytwo.stream.caching.Bindings;
import net.fortytwo.stream.caching.Query;
import net.fortytwo.stream.caching.QueryIndex;
import net.fortytwo.stream.model.VariableOrConstant;
import net.fortytwo.stream.sparql.SparqlStreamProcessor;
import net.fortytwo.stream.sparql.SparqlQuery;
import net.fortytwo.stream.BasicSubscription;
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
 * Concrete implementation of a SesameStream continuous SPARQL query engine
 * which materializes and caches intermediate solutions.
 * For a more space-efficient implementation, see SHJSparqlQueryEngine.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class CachingSparqlStreamProcessor extends SparqlStreamProcessor<Query<Value, ?>> {

    private final QueryIndex<Value, BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet>> queryIndex;
    private final BiConsumer<BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet>, Bindings<Value>> solutionHandler;

    private CleanupPolicy cleanupPolicy;
    private long timeOfLastCleanup = 0;
    private int queriesAddedSinceLastCleanup = 0;
    private int statementsAddedSinceLastCleanup = 0;

    private final Object cleanupLock = "";
    private long cleanupNow;

    /**
     * Creates a new query engine with an empty index
     */
    public CachingSparqlStreamProcessor() {
        super();

        // note: this implementation does not support RDF quads
        queryIndex = new QueryIndex<>(3);

        solutionHandler = new BiConsumer<BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet>, Bindings<Value>>() {
            @Override
            public void accept(final BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet> subscription,
                               final Bindings<Value> bindings) {
                try {
                    // note: this implementation does not compute an expiration time for solutions
                    handleCandidateSolution(subscription, toBindingSet(bindings), StreamProcessor.NEVER_EXPIRE);
                } catch (IOException e) {
                    logger.log(Level.WARNING, "failed to handle solution " + bindings, e);
                }
            }
        };

        cleanupPolicy = new CleanupPolicy() {
            @Override
            public boolean doCleanup(int secondsElapsedSinceLast,
                                     int queriesAddedSinceLast,
                                     int statementsAddedSinceLast) {
                return secondsElapsedSinceLast >= 30;
            }
        };

        clear();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        synchronized (cleanupLock) {
                            try {
                                cleanupLock.wait();
                            } catch (InterruptedException e) {
                                logger.warning("interrupted while waiting on TTL cleanup lock");
                            }
                        }

                        // terminated
                        if (!isActive()) {
                            return;
                        }

                        queryIndex.removeExpired(cleanupNow);
                    }
                } catch (Throwable t) {
                    logger.log(Level.SEVERE, "TTL cleanup thread failed", t);
                }
            }
        }).start();
    }

    /**
     * Sets a policy for removing expired solutions from this query engine.
     * The policy is triggered after every query or set of statements is added to the query engine.
     *
     * @param cleanupPolicy a policy which determines when a cleanup operation is executed.
     *                      The default policy executes every 30 seconds, provided that new statements are added.
     */
    public void setCleanupPolicy(final CleanupPolicy cleanupPolicy) {
        this.cleanupPolicy = cleanupPolicy;
    }

    @Override
    public void clear() {
        queryIndex.clear();
        clearCounters();
    }

    @Override
    public void shutDown() {
        // terminate the cleanup thread
        synchronized (cleanupLock) {
            cleanupLock.notify();
        }

        super.shutDown();
    }

    private synchronized void checkCleanup(final long now) {
        int seconds = (int) ((now - timeOfLastCleanup) / 1000);

        if (cleanupPolicy.doCleanup(seconds, queriesAddedSinceLastCleanup, statementsAddedSinceLastCleanup)) {
            timeOfLastCleanup = now;
            queriesAddedSinceLastCleanup = 0;
            statementsAddedSinceLastCleanup = 0;
            cleanupNow = now;

            synchronized (cleanupLock) {
                cleanupLock.notify();
            }
        }
    }

    @Override
    public void unregister(final BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet> subscription) throws IOException {
        queryIndex.remove((Query<Value, BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet>>) subscription.getQuery());
    }

    @Override
    public boolean renew(final BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet> subscription, final int ttl)
            throws IOException {

        if (isActive()) {
            queryIndex.renew((Query<Value, BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet>>) subscription.getQuery(),
                    ttl, getNow());
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected void visitQueryPatterns(Query<Value, ?> query, Consumer<VariableOrConstant<String, Value>[]> visitor) {
        for (Query.PatternInQuery<Value, ?> p : query.getPatterns()) {
            visitor.accept(p.getPattern());
        }
    }

    @Override
    protected void register(BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet> subscription) {
        Query<Value, ?> query = subscription.getQuery();
        setQuerySubscription(query, subscription);

        queryIndex.add(((Query<Value, BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet>>) query));
        queriesAddedSinceLastCleanup++;
        checkCleanup(getNow());
    }

    @Override
    protected boolean addTupleInternal(Value[] tuple, int ttl, long now) {
        boolean changed = queryIndex.add(tuple, solutionHandler, ttl, now);

        if (changed) {
            statementsAddedSinceLastCleanup++;
            checkCleanup(getNow());
        }

        return changed;
    }

    @Override
    protected BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet> createSubscriptionInternal(
            SparqlQuery sparqlQuery,
            List<VariableOrConstant<String, Value>[]> patterns,
            long expirationTime,
            BiConsumer<BindingSet, Long> consumer) {

        final Query<Value, ?> query = new Query<>(patterns, expirationTime);
        return new BasicSubscription<>(sparqlQuery, query, consumer, this);
    }

    private void setQuerySubscription(Query<Value, ?> query, BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet>
            subscription) {
        ((Query<Value, BasicSubscription<SparqlQuery, Query<Value, ?>, BindingSet>>) query).setSubscription(subscription);
    }

    private BindingSet toBindingSet(final Bindings<Value> bindings) {

        MapBindingSet bs = new MapBindingSet();
        for (Map.Entry<String, Value> e : bindings.entrySet()) {
            bs.addBinding(e.getKey(), e.getValue());
        }

        return bs;
    }

    /**
     * A policy for removing expired solutions from a query engine
     */
    public static interface CleanupPolicy {
        /**
         * Determines whether a cleanup operation should be executed
         *
         * @param secondsElapsedSinceLast  the number of seconds elapsed since the last cleanup operation
         * @param queriesAddedSinceLast    the number of queries added to the query engine since the last
         *                                 cleanup operation
         * @param statementsAddedSinceLast the number of statements added to the query engine since the last
         *                                 cleanup operation
         * @return whether a cleanup operation should be executed
         */
        boolean doCleanup(int secondsElapsedSinceLast,
                          int queriesAddedSinceLast,
                          int statementsAddedSinceLast);
    }
}
