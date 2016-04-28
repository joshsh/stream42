package net.fortytwo.stream.shj;

import net.fortytwo.stream.StreamProcessor;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A manager for items which may expire over time.
 * It is assumed that the items (i.e. both their hashCodes and their expiration times) are immutable;
 * the only exception is the action of turning a solution into a "tombstone".
 * It is also assumed that identical solutions will not be added; a solution must be removed, or must
 * expire, before being added again (e.g. to update its expiration time).
 * Finally, only non-tombstone items may be added.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class ExpirationManager<T extends Expirable> implements Index<T> {

    private static final Logger logger = Logger.getLogger(ExpirationManager.class.getName());

    protected abstract long getNow();

    private boolean stopped = true;
    private boolean verbose = false;

    private final Object waitLock = "";

    @Override
    public boolean isEmpty() {
        // must catch up with the current time and remove any tombstones from the top of the heap
        // before we can say whether the heap is empty
        evictExpired();

        return heap.isEmpty();
    }

    /*
    PriorityQueue complexity:
      O(log(n)) time for the enqueing and dequeing methods (offer, poll, remove() and add)
      linear time for the remove(Object) and contains(Object) methods
      constant time for the retrieval methods (peek, element, and size)
    See: https://docs.oracle.com/javase/7/docs/api/java/util/PriorityQueue.html
     */
    private final AbstractQueue<T> heap = new PriorityQueue<>(new Comparator<T>() {
        @Override
        public int compare(T o1, T o2) {
            long t1 = o1.getExpirationTime();
            long t2 = o2.getExpirationTime();

            // note: it is assumed that no items in the heap have the special infinite TTL timestamp
            return ((Long) t1).compareTo(t2);
        }
    });

    public void clear() {
        // simply clear the heap. Do not evict individual heap items.
        heap.clear();
    }

    /**
     * Adds an item. It is assumed that the item is not already present.
     * The time complexity of this operation is O(log(n)), from PriorityQueue.add
     *
     * @param toAdd the item to add
     */
    public void add(T toAdd) {
        // non-expiring items are ignored
        if (isFinite(toAdd)) {
            synchronized (this) {
                heap.add(toAdd);
            }
        }
    }

    // note: no need for synchronization here
    public boolean remove(T toRemove) {
        // non-expiring items should not be in the heap, and are ignored
        if (isFinite(toRemove)) {
            // PriorityQueue.remove is O(n) in time, so we leave the item in the heap, but make it a "tombstone"
            toRemove.expire();
            return true;
        }

        return false;
    }

    public void notifyFinishedAdding() {
        // the eviction thread needs to know when data has been added
        synchronized (waitLock) {
            waitLock.notify();
        }
    }

    private long getClockTime() {
       return System.currentTimeMillis();
    }

    public synchronized int evictExpired() {
        long startTime = 0;
        int startSize = 0;

        long now = getNow();

        if (verbose) {
            startSize = getHeapSize();
            startTime = getClockTime();
        }

        int count = 0;
        try {
            while (!heap.isEmpty()) {
                T first = heap.peek();

                if (first.isExpired()) {
                    // discard tombstones without counting
                    heap.poll();
                } else if (first.getExpirationTime() > now) {
                    // top of the heap is unexpired, therefore the rest of the heap is also unexpired.
                    return count;
                } else {
                    heap.poll().expire();
                    count++;
                }
            }

            return count;
        } finally {
            if (verbose && count > 0) {
                long after = getClockTime();
                logger.info("evicted " + count + " of " + startSize + " items in " + (after - startTime) + " ms");
            }
        }
    }

    private boolean isFinite(T toCheck) {
        return toCheck.getExpirationTime() != StreamProcessor.NEVER_EXPIRE;
    }

    // note: only for real-time (not for simulated time) use; the thread uses expiration timestamps for waiting
    public synchronized void start() {
        if (!stopped) return;

        stopped = false;

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    logger.info(ExpirationManager.class.getSimpleName() + " thread started");

                    // repeatedly evict all expired data, pausing appropriately
                    while (!stopped) {
                        long firstExpiringTimestamp = heap.isEmpty() ? 0 : heap.peek().getExpirationTime();

                        try {
                            synchronized (waitLock) {
                                if (0 == firstExpiringTimestamp) {
                                    // wait indefinitely, or until notified
                                    waitLock.wait();
                                } else {
                                    // wait until the currently known first expiring time stamp, or until notified
                                    long now = getNow();
                                    if (now < firstExpiringTimestamp) {
                                        waitLock.wait(firstExpiringTimestamp - now);
                                    }
                                }
                            }
                        } catch (InterruptedException e) {
                            logger.warning(ExpirationManager.class.getSimpleName()
                                    + " thread interrupted while waiting");
                        }

                        evictExpired();
                    }
                    logger.info(ExpirationManager.class.getSimpleName() + " thread stopped");
                } catch (Exception e) {
                    logger.log(Level.SEVERE, ExpirationManager.class.getSimpleName() + " thread died with error", e);
                }
            }
        }).start();
    }

    public synchronized void stop() {
        stopped = true;
    }

    public int getHeapSize() {
        return heap.size();
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }
}
