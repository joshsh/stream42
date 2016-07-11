package net.fortytwo.stream.sparql.etc;

import net.fortytwo.stream.StreamProcessor;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * An <code>RDFHandler</code> which adds handled <code>Statements</code>
 * to a designated {@link net.fortytwo.stream.StreamProcessor}
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class StatementAdder implements RDFHandler {
    private final StreamProcessor streamProcessor;
    private final int ttl;

    public StatementAdder(final StreamProcessor streamProcessor,
                          final int ttl) {
        this.streamProcessor = streamProcessor;
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
        streamProcessor.addInputs(ttl, s);
    }

    public void handleComment(String s) throws RDFHandlerException {
        // do nothing
    }
}
