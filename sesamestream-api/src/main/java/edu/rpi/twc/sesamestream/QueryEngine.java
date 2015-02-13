package edu.rpi.twc.sesamestream;

import org.openrdf.model.Statement;

import java.io.IOException;
import java.util.Collection;

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
     * @param ttl the time-to-live of the generated subscription, in seconds.
     *            If ttl > 0, the query has a finite lifetime and will be automatically unregistered at the end
     *            of that lifetime.
     *            Note: the query engine may be configured to reject a ttl which is too high, including 0 (infinite).
     * @param query the query to add
     * @param handler a handler for future query answers
     * @return a subscription for computation of future query answers
     * @throws InvalidQueryException if the query is not valid SPARQL
     * @throws IncompatibleQueryException if the query is valid SPARQL,
     * but is not supported by this query engine
     * @throws IOException if there is a problem communicating with this query engine
     * (for example, if there are network operations involved)
     */
    Subscription addQuery(int ttl, String query, BindingSetHandler handler)
            throws IOException, IncompatibleQueryException, InvalidQueryException;

    /**
     * Adds a new statement to this query engine.
     * Depending on the queries registered with this engine,
     * the statement will either be discarded as irrelevant to the queries,
     * trigger the creation of partial solutions which are stored in anticipation of further statements,
     * or trigger the production query answers.
     *
     * @param ttl the time-to-live of the added statement, in seconds.
     *            If ttl > 0, any partial solutions which are computed in response to the statement have a finite
     *            lifetime, and will expire from the index at the end of that lifetime.
     * @param statement the statement to add
     * @throws IOException if there is a problem communicating with this query engine
     * (for example, if there are network operations involved)
     */
    void addStatement(int ttl, Statement statement) throws IOException;

    /**
     * Adds new statements to this query engine.
     * Depending on the queries registered with this engine,
     * the statements will either be discarded as irrelevant to the queries,
     * trigger the creation of partial solutions which are stored in anticipation of further statements,
     * or trigger the production query answers.
     *
     * @param ttl the time-to-live of the added statements, in seconds.
     *            If ttl > 0, any partial solutions which are computed in response to those statements have a finite
     *            lifetime, and will expire from the index at the end of that lifetime.
     * @param statements the statements to add.  Statements are added in array order
     * @throws IOException if there is a problem communicating with this query engine
     * (for example, if there are network operations involved)
     */
    void addStatements(int ttl, Statement... statements) throws IOException;

    /**
     * Adds new statements to this query engine.
     * Depending on the queries registered with this engine,
     * the statements will either be discarded as irrelevant to the queries,
     * trigger the creation of partial solutions which are stored in anticipation of further statements,
     * or trigger the production query answers.
     *
     * @param ttl the time-to-live of the added statements, in seconds.
     *            If ttl > 0, any partial solutions which are computed in response to those statements have a finite
     *            lifetime, and will expire from the index at the end of that lifetime.
     * @param statements the statements to add.  Statements are added in the iterator order of the collection
     * @throws IOException if there is a problem communicating with this query engine
     * (for example, if there are network operations involved)
     */
    void addStatements(int ttl, Collection<Statement> statements) throws IOException;

    /**
     * An exception thrown when a query is not valid SPARQL
     */
    class InvalidQueryException extends Exception {
        public InvalidQueryException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception thrown when a valid SPARQL query is incompatible with a QueryEngine implementation
     */
    class IncompatibleQueryException extends Exception {
        public IncompatibleQueryException(String message) {
            super(message);
        }
    }
}
