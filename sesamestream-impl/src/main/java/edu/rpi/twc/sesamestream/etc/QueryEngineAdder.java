package edu.rpi.twc.sesamestream.etc;

import edu.rpi.twc.sesamestream.QueryEngine;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import java.io.IOException;

/**
 * An <code>RDFHandler</code> which adds handled <code>Statements</code>
 * to a designated {@link edu.rpi.twc.sesamestream.QueryEngine}
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryEngineAdder implements RDFHandler {
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
        try {
            queryEngine.addStatement(s);
        } catch (IOException e) {
            throw new RDFHandlerException(e);
        }
    }

    public void handleComment(String s) throws RDFHandlerException {
    }
}
