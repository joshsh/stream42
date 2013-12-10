package edu.rpi.twc.sesamestream;

/**
 * A collection of global flags which influence SesameStream's behavior
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SesameStream {
    private static boolean PERFORMANCE_METRICS = false;
    private static boolean DEBUG = false;
    private static boolean USE_COMPACT_LOG_FORMAT = true;

    private static long REDUCED_MODIFIER_CAPACITY = 1000;

    /**
     * @return whether to output performance metadata as new statements are added to the query engine and solutions are found.
     * If enabled, this allows fine details query latency and throughput, memory usage,
     * and other performance variables to be studied.
     */
    public static boolean getDoPerformanceMetrics() {
        return PERFORMANCE_METRICS;
    }

    public static void setDoPerformanceMetrics(final boolean b) {
        PERFORMANCE_METRICS = b;
    }

    /**
     * @return whether additional sanity checks should be performed during computation.
     * Normally unnecessary.
     */
    public static boolean getDoDebug() {
        return DEBUG;
    }

    public static void setDoDebug(final boolean b) {
        DEBUG = b;
    }

    /**
     * @return whether performance metadata (if enabled) should be output only when new solutions are computed,
     * and not every time a statement is added.
     * This makes the log much smaller.
     */
    public static boolean getDoUseCompactLogFormat() {
        return USE_COMPACT_LOG_FORMAT;
    }

    public static void setDoUseCompactLogFormat(final boolean b) {
        USE_COMPACT_LOG_FORMAT = b;
    }

    /**
     * @return the number of distinct solutions which each query subscription can store before it begins recycling them.
     * For a SELECT DISTINCT query, the set of distinct solution grows without bound, but a duplicate answer will never appear
     * in the output stream.
     * However, for a SELECT REDUCED query, the set of distinct solutions is limited in size().
     * This is much safer with respect to memory consumption,
     * although duplicate solutions may eventually appear in the output stream.
     */
    public static long getReducedModifierCapacity() {
        return REDUCED_MODIFIER_CAPACITY;
    }

    public static void setReducedModifierCapacity(final long capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("unreasonable REDUCED capacity value: " + capacity);
        }

        REDUCED_MODIFIER_CAPACITY = capacity;
    }
}
