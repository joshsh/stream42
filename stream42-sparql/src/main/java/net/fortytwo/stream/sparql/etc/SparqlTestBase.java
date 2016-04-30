package net.fortytwo.stream.sparql.etc;

import net.fortytwo.stream.sparql.SparqlStreamProcessor;
import net.fortytwo.stream.sparql.RDFStreamProcessor;
import info.aduna.io.IOUtil;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

/**
 * A base class for unit tests, included here for ease of use in downstream projects.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SparqlTestBase {
    private static final String BASE_URI = "http://example.org/base/";

    protected static final Logger logger = Logger.getLogger(SparqlTestBase.class.getName());

    private final QueryParser queryParser = new SPARQLParser();
    protected Sail sail;
    protected SparqlStreamProcessor queryEngine;
    protected final ValueFactory vf = new ValueFactoryImpl();

    protected final int TUPLE_TTL = 0, QUERY_TTL = 0;

    protected final BiConsumer<BindingSet, Long> simpleConsumer = new BiConsumer<BindingSet, Long>() {
        @Override
        public void accept(final BindingSet result, final Long expirationTime) {
            System.out.println("result: " + result + ", expires at " + expirationTime);
        }
    };

    protected TupleExpr loadQuery(final String fileName) throws Exception {
        InputStream in = RDFStreamProcessor.class.getResourceAsStream(fileName);
        String query = IOUtil.readString(in);
        in.close();

        ParsedQuery pq = queryParser.parseQuery(query, BASE_URI);

        return pq.getTupleExpr();
    }

    protected List<Statement> loadData(final String fileName) throws Exception {
        RDFFormat format = RDFFormat.forFileName(fileName);

        if (null == format) {
            throw new IllegalStateException("unsupported file extension");
        }

        RDFParser p = Rio.createParser(format);

        p.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

        List<Statement> c = new LinkedList<>();
        p.setRDFHandler(new StatementCollector(c));

        InputStream in = fileName.startsWith("/")
                ? new FileInputStream(new File(fileName))
                : RDFStreamProcessor.class.getResourceAsStream(fileName);
        p.parse(in, BASE_URI);
        in.close();

        return c;
    }

    protected Set<BindingSet>[] distinctStaticQueryAnswers(final List<Statement> data,
                                                           final TupleExpr... queries) throws Exception {
        Set<BindingSet>[] answers = new Set[queries.length];

        int i = 0;
        for (TupleExpr query : queries) {
            Set<BindingSet> results = new HashSet<>();
            answers[i] = results;

            SailConnection sc = sail.getConnection();
            try {
                sc.begin();

                for (Statement s : data) {
                    sc.addStatement(s.getSubject(), s.getPredicate(), s.getObject(), s.getContext());
                }

                CloseableIteration<? extends BindingSet, QueryEvaluationException> iter
                        = sc.evaluate(query, new DatasetImpl(), new EmptyBindingSet(), false);
                try {
                    while (iter.hasNext()) {
                        results.add(iter.next());
                    }
                } finally {
                    iter.close();
                }
            } finally {
                sc.rollback();
                sc.close();
            }

            i++;
        }

        return answers;
    }

    protected Set<BindingSet>[] distinctContinuousQueryAnswers(final List<Statement> data,
                                                               final TupleExpr... queries) throws Exception {
        final Set<BindingSet>[] answers = new Set[queries.length];

        queryEngine.clear();

        int i = 0;
        for (TupleExpr tupleExpr : queries) {
            answers[i] = new HashSet<>();

            final int id = i;
            BiConsumer<BindingSet, Long> solutionConsumer = new BiConsumer<BindingSet, Long>() {
                private final Set<BindingSet> a = answers[id];

                @Override
                public void accept(final BindingSet result, final Long expirationTime) {
                    System.out.println("result: " + result);
                    a.add(result);
                }
            };

            queryEngine.addQuery(QUERY_TTL, tupleExpr, solutionConsumer);

            i++;
        }

        for (Statement s : data) {
            queryEngine.addInputs(TUPLE_TTL, s);
        }

        return answers;
    }

    protected Collection<BindingSet> continuousQueryAnswers(final List<Statement> data,
                                                            final TupleExpr query,
                                                            final boolean debug) throws Exception {
        final Collection<BindingSet> answers = new LinkedList<>();

        queryEngine.clear();

        BiConsumer<BindingSet, Long> solutionConsumer = new BiConsumer<BindingSet, Long>() {
            @Override
            public void accept(final BindingSet result, final Long expirationTime) {
                if (debug) {
                    System.out.println("result: " + result + ", expires at " + expirationTime);
                }
                answers.add(result);
            }
        };

        queryEngine.addQuery(QUERY_TTL, query, solutionConsumer);

        for (Statement s : data) {
            queryEngine.addInputs(TUPLE_TTL, s);
        }

        /*
        if (debug) {
            Set<BindingSet> distinct = new HashSet<BindingSet>();
            distinct.addAll(answers);

            System.out.println("" + answers.size() + " solutions ("
                    + distinct.size() + " distinct, " + countPartialSolutions()
                    + " partial) from " + data.size() + " statements");
        }
        */

        return answers;
    }
}
