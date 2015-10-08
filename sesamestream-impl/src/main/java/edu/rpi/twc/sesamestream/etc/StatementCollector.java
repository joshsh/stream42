package edu.rpi.twc.sesamestream.etc;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import java.util.Collection;

/**
 * An <code>RDFHandler</code> which adds handled <code>Statement</code>s to a designated <code>Collection</code>
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class StatementCollector implements RDFHandler {
    private Collection<Statement> coll;

    public StatementCollector(final Collection<Statement> coll) {
        this.coll = coll;
    }

    public void startRDF() throws RDFHandlerException {
    }

    public void endRDF() throws RDFHandlerException {
    }

    public void handleNamespace(String s, String s1) throws RDFHandlerException {
    }

    public void handleStatement(Statement s) throws RDFHandlerException {
        coll.add(s);
    }

    public void handleComment(String s) throws RDFHandlerException {
    }
}
