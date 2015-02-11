package edu.rpi.twc.sesamestream.tuple;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Tuple<T> {
    private final T[] elements;

    public Tuple(final T[] elements) {
        this.elements = elements;
    }

    public T[] getElements() {
        return elements;
    }
}
