package net.fortytwo.stream;

import java.io.IOException;

/**
 * @param <R> the raw query type. A raw query is parsed to a constraint.
 * @param <C> the constraint type. A constraint is translated into a native query.
 * @param <Q> the native query type
 * @param <I> the input type
 * @param <S> the solution type
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class BasicStreamProcessor<R, C, Q, I, S> implements StreamProcessor<R, I, S> {

    /**
     * Frees up the resources occupied by the provided subscription and prevents it from matching future inputs
     *
     * @param subscription a query subscription to deactivate and remove
     */
    public abstract void unregister(final BasicSubscription<C, Q, S> subscription);

    /**
     * Refreshes a subscription which has expired or is about to expire
     *
     * @param subscription the subscription to renew
     * @param ttl          a new time-to-live for the subscription, in seconds
     * @return whether the subscription is successfully renewed.
     * Renewal may or may not be possible, depending on the state of the subscription and the query engine.
     */
    public abstract boolean renew(BasicSubscription<C, Q, S> subscription, int ttl);
}
