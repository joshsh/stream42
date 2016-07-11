package net.fortytwo.stream;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Interface for a continuous query engine with a publish/subscribe API.
 * A query engine receives queries in advance of input data, computing solutions incrementally.
 *
 * @param <Q> the query type
 * @param <I> the input data type
 * @param <S> the solution data type
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface StreamProcessor<Q, I, S> {

    /**
     * A special time-to-live value for statements or queries which are valid indefinitely.
     */
    public static final int INFINITE_TTL = 0;

    /**
     * A special value representing an infinitely remote expiration time
     */
    public static final long NEVER_EXPIRE = Long.MAX_VALUE;

    /**
     * Removes all triple patterns along with their associated partial solutions, queries, and subscriptions
     */
    void clear();

    /**
     * Adds a new query subscription to this query engine
     *
     * @param ttl      the time-to-live of the generated subscription, in seconds.
     *                 If ttl is greater than 0, the query has a finite lifetime and will be
     *                 automatically unregistered at the end of that lifetime.
     *                 Note: the query engine may be configured to reject a ttl which is too high, including 0 (infinite).
     * @param query    the query to add
     * @param consumer a handler for future query solutions together with their expiration time
     * @return a subscription for computation of future query solutions
     * @throws InvalidQueryException      if the query is not valid
     * @throws IncompatibleQueryException if the query is valid,
     *                                    but it is not supported by this query engine
     * @throws IOException                if there is a problem communicating with this query engine
     *                                    (for example, if there are network operations involved)
     */
    Subscription addQuery(int ttl, Q query, BiConsumer<S, Long> consumer)
            throws IOException, IncompatibleQueryException, InvalidQueryException;

    /**
     * Adds new input data to this query engine.
     * Depending on the queries registered with this engine,
     * the inputs will either be discarded as irrelevant to the queries,
     * trigger the creation of partial solutions which are stored in anticipation of further inputs,
     * or trigger the production query solutions.
     *
     * @param ttl    the time-to-live of the inputs, in seconds.
     *               If ttl is greater than 0, any partial solutions which are computed in response to those inputs
     *               have a finite lifetime, and will expire from the index at the end of that lifetime.
     * @param inputs the input data to add.  Inputs are added in array order
     * @return whether the addition of the inputs changed the state of the stream processor;
     * some inputs may pass through and have no effect, while others are used in query answering
     */
    boolean addInputs(int ttl, I... inputs);

    /**
     * Sets the clock used by this query engine to determine the expiration of queries and solutions,
     * overriding a default clock (e.g. based on System.currentTimeMillis())
     * The supplied clock must supply the real or virtual current time, in milliseconds.
     * Values produced must be non-negative and monotonically non-decreasing.
     *
     * @param clock the new clock
     */
    void setClock(Supplier<Long> clock);

    /**
     * An exception thrown when a query is not syntactically valid
     */
    static class InvalidQueryException extends Exception {
        public InvalidQueryException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception thrown when a valid query is incompatible with a QueryEngine implementation
     */
    static class IncompatibleQueryException extends Exception {
        public IncompatibleQueryException(String message) {
            super(message);
        }
    }
}
