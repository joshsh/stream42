package edu.rpi.twc.sesamestream.impl;

import edu.rpi.twc.sesamestream.BindingSetHandler;
import edu.rpi.twc.sesamestream.Subscription;
import edu.rpi.twc.sesamestream.tuple.GraphPattern;
import org.openrdf.model.Value;

/**
 * An object which associates a SPARQL query with a handler for the query's results
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SubscriptionImpl implements Subscription {
    private final Query query;
    private final GraphPattern<Value> graphPattern;
    private final BindingSetHandler handler;
    private final QueryEngineImpl queryEngine;

    private boolean active;

    private static long maxId = 0;

    private final String id;

    public SubscriptionImpl(final Query query,
                            final GraphPattern<Value> graphPattern,
                            final BindingSetHandler handler,
                            final QueryEngineImpl queryEngine) {
        this.query = query;
        this.graphPattern = graphPattern;
        this.handler = handler;
        this.queryEngine = queryEngine;

        this.active = true;
        this.id = "" + ++maxId;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public void cancel() {
        active = false;

        queryEngine.unregister(this);
    }

    @Override
    public boolean renew(long ttl) {
        if (isActive()) {
            queryEngine.renew(this, ttl);
            return true;
        } else {
            return false;
        }
    }

    /**
     * @return the SPARQL query which has been registered in the {@link QueryEngineImpl}
     */
    public Query getQuery() {
        return query;
    }

    /**
     * @return the graph pattern which has been indexed in the forward-chaining tuple store implementation
     */
    public GraphPattern<Value> getGraphPattern() {
        return graphPattern;
    }

    /**
     * @return the handler for the query's results
     */
    public BindingSetHandler getHandler() {
        return handler;
    }
}
