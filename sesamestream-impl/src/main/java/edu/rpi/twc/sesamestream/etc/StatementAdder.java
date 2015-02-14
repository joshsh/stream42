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
public class StatementAdder implements RDFHandler {
    private final QueryEngine queryEngine;
    private final int ttl;

    public StatementAdder(final QueryEngine queryEngine,
                          final int ttl) {
        this.queryEngine = queryEngine;
        this.ttl = ttl;
    }

    public void startRDF() throws RDFHandlerException {
        // do nothing
    }

    public void endRDF() throws RDFHandlerException {
        // do nothing
    }

    public void handleNamespace(String s, String s1) throws RDFHandlerException {
        // do nothing
    }

    public void handleStatement(Statement s) throws RDFHandlerException {
        try {
            queryEngine.addStatements(ttl, s);
        } catch (IOException e) {
            throw new RDFHandlerException(e);
        }
    }

    public void handleComment(String s) throws RDFHandlerException {
        // do nothing
    }
}
