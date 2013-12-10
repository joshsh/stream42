package edu.rpi.twc.sesamestream.impl;

/**
 * A generic visitor for objects in a collection.  Each object is to be visited once.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Visitor<T> {
    /**
     * @param t the object to visit
     * @return whether to continue visiting other objects in the collection
     */
    boolean visit(T t);
}
