package edu.rpi.twc.rdfstream4j;

import org.openrdf.model.Statement;

import java.io.IOException;

/**
 * A continuous SPARQL query engine with a publish/subscribe API
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface QueryEngine {

    /**
     * Removes all triple patterns along with their associated partial solutions, queries, and subscriptions
     */
    void clear();

    /**
     * Adds a new query subscription to this query engine
     *
     * @param ttl     the time-to-live of the generated subscription, in seconds.
     *                If ttl > 0, the query has a finite lifetime and will be automatically unregistered at the end
     *                of that lifetime.
     *                Note: the query engine may be configured to reject a ttl which is too high, including 0 (infinite).
     * @param query   the query to add
     * @param handler a handler for future query answers
     * @return a subscription for computation of future query answers
     * @throws InvalidQueryException      if the query is not valid SPARQL
     * @throws IncompatibleQueryException if the query is valid SPARQL,
     *                                    but is not supported by this query engine
     * @throws IOException                if there is a problem communicating with this query engine
     *                                    (for example, if there are network operations involved)
     */
    Subscription addQuery(int ttl, String query, BindingSetHandler handler)
            throws IOException, IncompatibleQueryException, InvalidQueryException;

    /**
     * Adds new statements to this query engine.
     * Depending on the queries registered with this engine,
     * the statements will either be discarded as irrelevant to the queries,
     * trigger the creation of partial solutions which are stored in anticipation of further statements,
     * or trigger the production query answers.
     *
     * @param ttl        the time-to-live of the added statements, in seconds.
     *                   If ttl > 0, any partial solutions which are computed in response to those statements have a finite
     *                   lifetime, and will expire from the index at the end of that lifetime.
     * @param statements the statements to add.  Statements are added in array order
     * @throws IOException if there is a problem communicating with this query engine
     *                     (for example, if there are network operations involved)
     */
    void addStatements(int ttl, Statement... statements) throws IOException;

    /**
     * Sets the clock used by this query engine to determine expiration of queries and solutions,
     * overriding a default clock based on System.currentTimeMillis()
     *
     * @param clock the new clock
     */
    void setClock(Clock clock);

    /**
     * Sets a policy for removing expired solutions from this query engine.
     * The policy is triggered after every query or set of statements is added to the query engine.
     * @param policy a policy which determines when a cleanup operation is executed.
     *               The default policy executes every 30 seconds, provided that new statements are added.
     */
    void setCleanupPolicy(CleanupPolicy policy);

    /**
     * A clock used to determine expiration of queries and solutions
     */
    public static interface Clock {
        /**
         * Gets the real or virtual current time, in milliseconds
         *
         * @return the real or virtual current time, in milliseconds.
         * Values produced must be non-negative and monotonically non-decreasing.
         * The default implementation uses System.currentTimeMillis().
         */
        long getTime();
    }

    /**
     * A policy for removing expired solutions from a query engine
     */
    public static interface CleanupPolicy {
        /**
         * Determines whether a cleanup operation should be executed
         * @param secondsElapsedSinceLast the number of seconds elapsed since the last cleanup operation
         * @param queriesAddedSinceLast the number of queries added to the query engine since the last
         *                              cleanup operation
         * @param statementsAddedSinceLast the number of statements added to the query engine since the last
         *                                 cleanup operation
         * @return whether a cleanup operation should be executed
         */
        boolean doCleanup(int secondsElapsedSinceLast,
                          int queriesAddedSinceLast,
                          int statementsAddedSinceLast);
    }

    /**
     * An exception thrown when a query is not valid SPARQL
     */
    public static class InvalidQueryException extends Exception {
        public InvalidQueryException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception thrown when a valid SPARQL query is incompatible with a QueryEngine implementation
     */
    public static class IncompatibleQueryException extends Exception {
        public IncompatibleQueryException(String message) {
            super(message);
        }
    }
}
