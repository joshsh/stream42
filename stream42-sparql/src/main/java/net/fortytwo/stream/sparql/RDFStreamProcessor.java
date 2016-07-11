package net.fortytwo.stream.sparql;

import net.fortytwo.stream.BasicStreamProcessor;
import net.fortytwo.stream.BasicSubscription;
import net.fortytwo.stream.StreamProcessor;
import net.fortytwo.stream.Subscription;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * A base class for streaming SPARQL 1.1 implementations.
 * Optionally, performance data is generated in the process of query processing.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public abstract class RDFStreamProcessor<C, Q> extends BasicStreamProcessor<String, C, Q, Statement, BindingSet> {

    protected static final Logger logger = Logger.getLogger(RDFStreamProcessor.class.getName());

    public enum Quantity {
        Queries, Inputs, Solutions,
    }

    private Supplier<Long> clock;
    private boolean logHasChanged;
    private final Map<Quantity, Counter> counters;
    private long startTime;
    private final Counter
            countQueries = new Counter(),
            countStatements = new Counter(),
            countSolutions = new Counter();

    private boolean useCompactLogFormat = true;
    private boolean performanceMetrics = false;

    private boolean active = true;

    protected boolean isActive() {
        return active;
    }

    public void shutDown() {
        active = false;
    }

    protected RDFStreamProcessor() {
        counters = new LinkedHashMap<>();

        counters.put(Quantity.Queries, countQueries);
        counters.put(Quantity.Inputs, countStatements);
        counters.put(Quantity.Solutions, countSolutions);

        clock = new Supplier<Long>() {
            @Override
            public Long get() {
                return System.currentTimeMillis();
            }
        };
    }

    protected abstract C parseQuery(String queryStr) throws InvalidQueryException, IncompatibleQueryException;

    protected abstract BasicSubscription<C, Q, BindingSet> createSubscription(
            int ttl,
            final C sparqlQuery,
            BiConsumer<BindingSet, Long> consumer) throws IOException;

    /**
     * Adds a tuple to an internal index, possibly generating new solutions
     *
     * @param tuple the tuple to add
     * @param ttl   the time-to-live in seconds
     * @param now   the current time in milliseconds
     * @return whether the addition of the tuple changed the state of the index
     */
    protected abstract boolean addTuple(Value[] tuple, int ttl, long now);

    @Override
    public void setClock(final Supplier<Long> clock) {
        this.clock = clock;
    }

    /**
     * @param b whether performance metadata (if enabled) should be output only when new solutions are computed,
     *          and not every time a statement is added.
     *          This makes the log much smaller.
     */
    public void setDoUseCompactLogFormat(final boolean b) {
        useCompactLogFormat = b;
    }

    /**
     * @param b whether to output performance metadata
     *          as new statements are added to the query engine and solutions are found
     *          If enabled, this allows fine details query latency and throughput, memory usage,
     *          and other performance variables to be studied.
     */
    public void setDoPerformanceMetrics(final boolean b) {
        performanceMetrics = b;
    }

    @Override
    public Subscription addQuery(final int ttl,
                                 final String queryStr,
                                 final BiConsumer<BindingSet, Long> consumer)
            throws IncompatibleQueryException, InvalidQueryException, IOException {

        return addQueryNative(ttl, parseQuery(queryStr), consumer);
    }

    protected Subscription addQueryNative(final int ttl,
                                          final C sparqlQuery,
                                          final BiConsumer<BindingSet, Long> consumer)
            throws IOException {

        incrementQueries();

        BasicSubscription<C, Q, BindingSet> sub = createSubscription(ttl, sparqlQuery, consumer);

        logEntry();

        return sub;
    }

    /**
     * Retrieves a quantity tracked when "performance metrics" are enabled
     *
     * @param quantity the quantity to retrieve
     * @return the current value of the retrieved quantity
     */
    public long get(final Quantity quantity) {
        if (!performanceMetrics) {
            throw new IllegalStateException("performance metrics are disabled; quantities are not counted");
        }

        Counter counter = counters.get(quantity);
        if (null == counter) {
            throw new IllegalArgumentException("no counter for quantity: " + quantity);
        }

        return counter.count;
    }

    @Override
    public boolean addInputs(final int ttl, final Statement... inputs) throws IOException {
        boolean changed = false;
        for (Statement s : inputs) {
            incrementStatements();
            long now = beginOperation();

            Value[] tuple = toNative(s);
            changed |= addTuple(tuple, ttl, now);

            logEntry();
        }

        return changed;
    }

    private Value[] toNative(final Statement s) {
        // note: assumes tupleSize==3
        return new Value[]{s.getSubject(), s.getPredicate(), s.getObject()};
    }

    protected void increment(final Counter counter,
                             final boolean logChange) {
        if (performanceMetrics) {
            counter.increment();
            if (logChange) {
                logHasChanged = true;
            }
        }
    }

    protected void incrementQueries() {
        increment(countQueries, true);
    }

    protected void incrementStatements() {
        increment(countStatements, false);
    }

    protected void incrementSolutions() {
        increment(countSolutions, true);
    }

    protected long beginOperation() {
        long now = getNow();
        startTime = now;
        return now;
    }

    protected long getNow() {
        return clock.get();
    }

    protected void logEntry() {
        if (performanceMetrics) {
            if (!useCompactLogFormat || logHasChanged) {
                StringBuilder sb = new StringBuilder("LOG\t");
                sb.append(startTime).append(",").append(getNow());
                for (Map.Entry<Quantity, Counter> entry : counters.entrySet()) {
                    sb.append(",").append(entry.getValue().count);
                }
                System.out.println(sb.toString());

                logHasChanged = false;
            }
        }
    }

    protected void logHeader() {
        if (performanceMetrics) {
            StringBuilder sb = new StringBuilder("LOG\ttime1,time2");
            for (Quantity q : counters.keySet()) {
                sb.append(",").append(q.name());
            }
            System.out.println(sb.toString());
        }
    }

    protected long toExpirationTime(int ttl, long now) {
        return StreamProcessor.INFINITE_TTL == ttl ? StreamProcessor.NEVER_EXPIRE : now + 1000L * ttl;
    }

    private static String toString(final BindingSet b) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String n : b.getBindingNames()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }

            sb.append(n).append(":").append(b.getValue(n));
        }

        return sb.toString();
    }

    protected void handleSolution(final BiConsumer<BindingSet, Long> consumer,
                                  final BindingSet solution,
                                  final long expirationTime) {
        incrementSolutions();

        if (performanceMetrics) {
            System.out.println("SOLUTION\t" + getNow() + "\t"
                    + toString(solution));
        }

        consumer.accept(solution, expirationTime);
    }

    protected void clearCounters() {
        countQueries.reset();
        countStatements.reset();
        countSolutions.reset();
        logHeader();
    }

    protected static class Counter {
        private long count = 0;

        public void increment() {
            count++;
        }

        public void reset() {
            count = 0;
        }

        public long getCount() {
            return count;
        }
    }
}
