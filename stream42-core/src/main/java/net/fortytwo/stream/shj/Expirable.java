package net.fortytwo.stream.shj;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Expirable {
    /**
     * Gets the expiration time for this object
     *
     * @return an expiration time, in milliseconds since the Unix epoch.
     * The special value 0 indicates that the object is valid indefinitely.
     */
    long getExpirationTime();

    /**
     * An operation to be executed when an item expires.
     * Turns the object into a "tombstone" which occupies minimal memory in the expiration heap.
     * This is an alternative to removing an object from the heap, which is expensive;
     * the tombstone exists until it expires, but must not be used.
     */
     void expire();

    /**
     * Tests the object for "tombstone" status
     *
     * @return whether this object is a tombstone which must not be used
     */
     boolean isExpired();
}
