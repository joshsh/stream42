package edu.rpi.twc.sesamestream;

/**
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
}
