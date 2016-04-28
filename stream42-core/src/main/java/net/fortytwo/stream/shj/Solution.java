package net.fortytwo.stream.shj;

import net.fortytwo.stream.StreamProcessor;

import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Solution<V> implements Expirable {
    private static final Logger logger = Logger.getLogger(Solution.class.getName());

    private V[] values;
    private Integer hashCode;

    // TODO: consider using a heap of heaps (one per solution index) in the expiration manager in order to
    // avoid this extra space taken up by each solution
    private final Index<Solution<V>> expirationIndex;

    // TODO: consider using int to save space, and chunk timestamps accordingly
    private final long expirationTime;

    public Solution(V[] values, long expirationTime, Index<Solution<V>> expirationIndex) {
        this.values = values;
        this.expirationTime = expirationTime;
        this.expirationIndex = expirationIndex;
    }

    public Solution(V[] values, Index<Solution<V>> expirationIndex) {
        this(values, StreamProcessor.NEVER_EXPIRE, expirationIndex);
    }

    public V[] getValues() {
        return values;
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public void expire() {
        expirationIndex.remove(this);

        values = null;
        hashCode = null;
    }

    @Override
    public boolean isExpired() {
        return null == values;
    }

    public int compareByExpirationTime(Solution<V> other) {
        return expirationTime == StreamProcessor.NEVER_EXPIRE
                ? (other.expirationTime == StreamProcessor.NEVER_EXPIRE ? 0 : 1)
                : (other.expirationTime == StreamProcessor.NEVER_EXPIRE
                ? -1 : ((Long) expirationTime).compareTo(other.expirationTime));
    }

    /*
     * Tests for compatibility of solutions.
     *
     * Two solution mappings m1 and m2 are compatible if,
     * for every variable v in dom(m1) and in dom(m2), m1(v) = m2(v).
     * See https://www.w3.org/TR/2013/REC-sparql11-query-20130321/
     *
     * @param other the other solution
     * @param commonKeys a pre-determined set of shared keys.
     *                   Both solutions must have non-null values for each key.
     * @return whether this solution is compatible with the other
     */
    /*
    public boolean isCompatibleWith(Solution<V> other, Set<K> commonKeys) {
        for (K key : commonKeys) {
            if (!mapping.get(key).equals(other.mapping.get(key))) {
                return false;
            }
        }

        for (Map.Entry<String, T> e1 : mapping.entrySet()) {
            T v2 = other.mapping.get(e1.getKey());
            if (null != v2 && !v2.equals(e1.getValue())) {
                return false;
            }
        }

        for (Map.Entry<String, T> e2 : other.mapping.entrySet()) {
            T v1 = mapping.get(e2.getKey());
            if (null != v1 && !v1.equals(e2.getValue())) {
                return false;
            }
        }

        return true;
    }
    */

    @Override
    public int hashCode() {
        if (null == hashCode) {
            hashCode = Arrays.hashCode(values);
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object other) {
        // note: this method does not check for a mismatching type variable.
        return this == other || (other instanceof Solution && equals(this, ((Solution<V>) other)));
    }

    private static <V> boolean equals(Solution<V> s1, Solution<V> s2) {
        if (s1.hashCode() != s2.hashCode() || s1.values.length != s2.values.length) {
            return false;
        }

        if (logger.isLoggable(Level.FINE)) {
            // hashing operations on solutions execute in constant time so long as there are few identical solutions
            logger.fine("solution equality or hash collision. Performing (expensive) element-wise comparison");
        }

        for (int i = 0; i < s1.values.length; i++) {
            if (!s2.values[i].equals(s1.values[i])) {
                return false;
            }
        }

        return true;
    }
}
