package edu.rpi.twc.sesamestream;

import org.openrdf.model.Statement;

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
     * @param q the query to add
     * @param h a handler for future query answers
     * @return a subscription for computation of future query answers
     * @throws IncompatibleQueryException if the syntax of the query is not supported by this engine
     */
    Subscription addQuery(String q,
                          BindingSetHandler h) throws IncompatibleQueryException, InvalidQueryException;

    /**
     * Adds a new statement to this query engine.
     * Depending on the queries registered with this engine,
     * the statement will either be discarded as irrelevant to the queries,
     * trigger the creation of partial solutions which are stored in anticipation of further statements,
     * or trigger the production query answers.
     *
     * @param s the statement to add
     */
    void addStatement(Statement s);

    /**
     * Adds new statements to this query engine.
     * Depending on the queries registered with this engine,
     * the statements will either be discarded as irrelevant to the queries,
     * trigger the creation of partial solutions which are stored in anticipation of further statements,
     * or trigger the production query answers.
     *
     * @param statements the statements to add.  Statements are added in array order
     */
    void addStatements(Statement... statements);

    /**
     * Adds new statements to this query engine.
     * Depending on the queries registered with this engine,
     * the statements will either be discarded as irrelevant to the queries,
     * trigger the creation of partial solutions which are stored in anticipation of further statements,
     * or trigger the production query answers.
     *
     * @param statements the statements to add.  Statements are added in the iterator order of the collection
     */
    void addStatements(Collection<Statement> statements);

    /**
     * An exception thrown when a query is not valid SPARQL
     */
    class InvalidQueryException extends Exception {
        public InvalidQueryException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * An exception thrown when a valid SPARQL query is incompatible with SesameStream.
     * Only a subset of the SPARQL standard is supported.
     */
    class IncompatibleQueryException extends Exception {
        public IncompatibleQueryException(String message) {
            super(message);
        }
    }
}
