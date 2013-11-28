package edu.rpi.twc.sesamestream;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SesameStream {
    private static boolean PERFORMANCE_METRICS = false;
    private static boolean DEBUG = false;
    private static boolean USE_COMPACT_LOG_FORMAT = true;

    public static boolean getDoPerformanceMetrics() {
        return PERFORMANCE_METRICS;
    }

    public static void setDoPerformanceMetrics(final boolean b) {
        PERFORMANCE_METRICS = b;
    }

    public static boolean getDoDebug() {
        return DEBUG;
    }

    public static void setDoDebug(final boolean b) {
        DEBUG = b;
    }

    public static boolean getDoUseCompactLogFormat() {
        return USE_COMPACT_LOG_FORMAT;
    }

    public static void setDoUseCompactLogFormat(final boolean b) {
        USE_COMPACT_LOG_FORMAT = b;
    }
}
