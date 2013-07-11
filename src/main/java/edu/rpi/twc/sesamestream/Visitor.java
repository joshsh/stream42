package edu.rpi.twc.sesamestream;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Visitor<T> {
    /**
     * @param t the object to visit
     * @return whether to continue visiting other objects in the collection
     */
    boolean visit(T t);
}
