package net.fortytwo.stream.shj;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface Index<T> {
    /**
     * Adds the given item to this index
     *
     * @param toAdd the item to add
     */
    void add(T toAdd);

    /**
     * Removes the given item from this index
     *
     * @param toRemove the item to remove
     * @return whether an item was removed
     */
    boolean remove(T toRemove);

    /**
     * Removes all data at once.
     * It can be assumed that the data is no longer needed; this operation resets the index
     * and releases the data for garbage collection.
     */
    void clear();

    boolean isEmpty();
}
