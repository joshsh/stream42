package edu.rpi.twc.sesamestream;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * An <code>RDFHandler</code> which adds handled <code>Statements</code> to a designated {@link QueryEngine}
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
class QueryEngineAdder implements RDFHandler {
    private final QueryEngine queryEngine;

    public QueryEngineAdder(final QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    public void startRDF() throws RDFHandlerException {
    }

    public void endRDF() throws RDFHandlerException {
    }

    public void handleNamespace(String s, String s1) throws RDFHandlerException {
    }

    public void handleStatement(Statement s) throws RDFHandlerException {
        queryEngine.addStatement(s);
    }

    public void handleComment(String s) throws RDFHandlerException {
    }
}
