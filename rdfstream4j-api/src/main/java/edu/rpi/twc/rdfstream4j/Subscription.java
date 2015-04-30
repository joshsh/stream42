package edu.rpi.twc.rdfstream4j;

/**
 * An object which manages a subscription to a continuous query
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Subscription {

    /**
     * @return a globally unique id for this subscription
     */
    String getId();

    /**
     * @return whether this subscription is still registered with the {@link QueryEngine}
     * (occupying resources and potentially receiving and handling query results)
     */
    boolean isActive();

    /**
     * marks this subscription as inactive, so that the query no longer receives results
     */
    void cancel();

    /**
     * Renews the subscription for another ttl milliseconds (or indefinitely, if ttl=0),
     * provided the renewal operation is accepted by the query engine.
     *
     * @param ttl the new time-to-live in milliseconds, or 0 for inifinite time-to-live
     * @return whether the subscription was successfully renewed
     */
    boolean renew(int ttl);
}
