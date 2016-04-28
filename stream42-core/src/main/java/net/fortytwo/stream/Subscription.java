package net.fortytwo.stream;

import java.io.IOException;

/**
 * An object which manages the connection to a continuous query and its solutions
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Subscription {
    /**
     * @return a globally unique id for this subscription
     */
    String getId();

    /**
     * @return whether this subscription is still registered with the {@link StreamProcessor}
     * (occupying resources and potentially receiving and handling query results)
     */
    boolean isActive();

    /**
     * Marks this subscription as inactive, so that the query no longer receives results
     *
     * @throws java.io.IOException if there is a problem communicating with this query engine
     *                     (for example, if there are network operations involved)
     */
    void cancel() throws IOException;

    /**
     * Renews the subscription for another ttl milliseconds (or indefinitely, if ttl=0),
     * provided the renewal operation is accepted by the query engine.
     *
     * @param ttl the new time-to-live in seconds, or 0 for infinite time-to-live
     * @return whether the subscription was successfully renewed
     * @throws java.io.IOException if there is a problem communicating with this query engine
     *                     (for example, if there are network operations involved)
     */
    boolean renew(int ttl) throws IOException;
}
