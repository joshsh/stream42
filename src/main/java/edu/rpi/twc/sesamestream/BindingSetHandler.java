package edu.rpi.twc.sesamestream;

import org.openrdf.query.BindingSet;

/**
 * A handler for <code>BindingSet</code>s such as SPARQL query results
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface BindingSetHandler {
    /**
     * Perform some action on a BindingSet (for example, printing a line of a CSV or generating an audio notification)
     * @param result the BindingSet which has been pushed for handling
     */
    void handle(BindingSet result);
}
