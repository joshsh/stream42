package edu.rpi.twc.sesamestream.impl;

import edu.rpi.twc.sesamestream.BindingSetHandler;
import edu.rpi.twc.sesamestream.Subscription;
import edu.rpi.twc.sesamestream.tuple.Query;
import org.openrdf.model.Value;

/**
 * An object which associates a SPARQL query with a handler for the query's results
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SubscriptionImpl implements Subscription {
    private final SparqlQuery sparqlQuery;
    private final Query<Value> query;
    private final BindingSetHandler handler;
    private final QueryEngineImpl queryEngine;

    private boolean active;

    private static long maxId = 0;

    private final String id;

    public SubscriptionImpl(final SparqlQuery sparqlQuery,
                            final Query<Value> query,
                            final BindingSetHandler handler,
                            final QueryEngineImpl queryEngine) {
        this.sparqlQuery = sparqlQuery;
        this.query = query;
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
    public boolean renew(int ttl) {
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
    public SparqlQuery getSparqlQuery() {
        return sparqlQuery;
    }

    /**
     * @return the graph pattern which has been indexed in the forward-chaining tuple store implementation
     */
    public Query<Value> getQuery() {
        return query;
    }

    /**
     * @return the handler for the query's results
     */
    public BindingSetHandler getHandler() {
        return handler;
    }
}
