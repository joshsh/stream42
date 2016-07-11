package net.fortytwo.stream.sparql.etc;

import org.openrdf.model.BNode;
import org.openrdf.model.IRI;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.AbstractValueFactory;
import org.openrdf.model.impl.SimpleValueFactory;

/**
 * A ValueFactory which will accept bad IRIs (e.g. in input files), replacing them with valid IRIs
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class ErrorTolerantValueFactory extends AbstractValueFactory {
    private static final IRI GOOD_IRI = SimpleValueFactory.getInstance()
            .createIRI("http://example.org/substitute-for-bad-iri");

    private final ValueFactory base;

    public ErrorTolerantValueFactory(ValueFactory base) {
        this.base = base;
    }

    @Override
    public IRI createIRI(String s) {
        try {
            return base.createIRI(s);
        } catch (IllegalArgumentException e) {
            return GOOD_IRI;
        }
    }

    @Override
    public IRI createIRI(String s, String s1) {
        try {
            return base.createIRI(s, s1);
        } catch (IllegalArgumentException e) {
            return GOOD_IRI;
        }
    }

    @Override
    public BNode createBNode(String s) {
        return base.createBNode(s);
    }

    @Override
    public Literal createLiteral(String s) {
        return base.createLiteral(s);
    }

    @Override
    public Literal createLiteral(String s, String s1) {
        return base.createLiteral(s, s1);
    }

    @Override
    public Literal createLiteral(String s, IRI iri) {
        return base.createLiteral(s, iri);
    }

    @Override
    public Statement createStatement(Resource resource, IRI iri, Value value) {
        return base.createStatement(resource, iri, value);
    }

    @Override
    public Statement createStatement(Resource resource, IRI iri, Value value, Resource resource1) {
        return base.createStatement(resource, iri, value, resource1);
    }
}
