package edu.rpi.twc.sesamestream;

/**
 * An object which associates a SPARQL query with a handler for the query's results
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Subscription {
    private final Query query;
    private final BindingSetHandler handler;

    public Subscription(final Query query,
                        final BindingSetHandler handler) {
        this.query = query;
        this.handler = handler;
    }

    public Query getQuery() {
        return query;
    }

    public BindingSetHandler getHandler() {
        return handler;
    }
}
