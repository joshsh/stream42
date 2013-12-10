package edu.rpi.twc.sesamestream;

/**
 * An object which associates a SPARQL query with a handler for the query's results
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SubscriptionImpl implements Subscription {
    private final Query query;
    private final BindingSetHandler handler;
    private boolean active;

    private static long maxId = 0;

    private final String id;

    public SubscriptionImpl(final Query query,
                            final BindingSetHandler handler) {
        this.query = query;
        this.handler = handler;

        this.active = true;
        this.id = "" + ++maxId;
    }

    public String getId() {
        return id;
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

    public boolean isActive() {
        return active;
    }

    public void cancel() {
        active = false;

        // TODO: in future, also free up the resources (esp. in TripleIndex) occupied by this subscription
    }
}
