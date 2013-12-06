package edu.rpi.twc.sesamestream;

/**
 * An object which associates a SPARQL query with a handler for the query's results
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Subscription {
    private final Query query;
    private final BindingSetHandler handler;
    private boolean active;

    public Subscription(final Query query,
                        final BindingSetHandler handler) {
        this.query = query;
        this.handler = handler;

        // always active
        this.active = true;
    }

    /**
     * @return the SPARQL query which has been registered in the {@link QueryEngine}
     */
    public Query getQuery() {
        return query;
    }

    /**
     * @return the handler for the query's results
     */
    public BindingSetHandler getHandler() {
        return handler;
    }

    /**
     * @return whether this subscription is still registered with the {@link QueryEngine}
     * (occupying resources and potentially receiving and handling query results)
     */
    public boolean isActive() {
        return active;
    }

    /**
     * marks this subscription as inactive, so that the query no longer receives results
     */
    public void cancel() {
        active = false;

        // TODO: in future, also free up the resources (esp. in TripleIndex) occupied by this subscription
    }
}
