package net.fortytwo.stream.sparql.etc;

import info.aduna.iteration.CloseableIteration;
import net.fortytwo.sesametools.EmptyCloseableIteration;
import org.openrdf.model.IRI;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class FilterEvaluator {
    private final ValueFactory valueFactory;
    private final EvaluationStrategy eval;

    public FilterEvaluator(ValueFactory valueFactory) {
        this.valueFactory = valueFactory;

        eval = new EvaluationStrategyImpl(new EmptyTripleSource(), null, null);
    }

    public boolean applyFilter(final Filter f,
                               final BindingSet bindings) throws QueryEvaluationException {
        return eval.isTrue(f.getCondition(), bindings);
    }

    // a trivial TripleSource to satisfy EvaluationStrategyImpl
    private class EmptyTripleSource implements TripleSource {
        public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
                Resource resource, IRI iri, Value value, Resource... resources) throws QueryEvaluationException {

            return new EmptyCloseableIteration<>();
        }

        public ValueFactory getValueFactory() {
            return valueFactory;
        }
    }
}
