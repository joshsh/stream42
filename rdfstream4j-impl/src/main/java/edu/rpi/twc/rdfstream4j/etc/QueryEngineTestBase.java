package edu.rpi.twc.rdfstream4j.etc;

import edu.rpi.twc.rdfstream4j.BindingSetHandler;
import edu.rpi.twc.rdfstream4j.RDFStream4j;
import edu.rpi.twc.rdfstream4j.impl.QueryEngineImpl;
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

/**
 * A base class for unit tests, included here for ease of use in downstream projects.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryEngineTestBase {
    protected static final String BASE_URI = "http://example.org/base/";

    protected QueryParser queryParser = new SPARQLParser();
    protected Sail sail;
    protected QueryEngineImpl queryEngine;
    protected ValueFactory vf = new ValueFactoryImpl();

    protected final int TUPLE_TTL = 0, QUERY_TTL = 0;

    protected BindingSetHandler simpleBindingSetHandler = new BindingSetHandler() {
        public void handle(final BindingSet result) {
            System.out.println("result: " + result);
        }
    };

    protected TupleExpr loadQuery(final String fileName) throws Exception {
        InputStream in = RDFStream4j.class.getResourceAsStream(fileName);
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

        List<Statement> c = new LinkedList<Statement>();
        p.setRDFHandler(new StatementCollector(c));

        InputStream in = fileName.startsWith("/")
                ? new FileInputStream(new File(fileName))
                : RDFStream4j.class.getResourceAsStream(fileName);
        p.parse(in, BASE_URI);
        in.close();

        return c;
    }

    protected Set<BindingSet>[] distinctStaticQueryAnswers(final List<Statement> data,
                                                           final TupleExpr... queries) throws Exception {
        Set<BindingSet>[] answers = new Set[queries.length];

        int i = 0;
        for (TupleExpr query : queries) {
            Set<BindingSet> results = new HashSet<BindingSet>();
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
        for (TupleExpr t : queries) {
            answers[i] = new HashSet<BindingSet>();

            final int id = i;
            BindingSetHandler h = new BindingSetHandler() {
                private final Set<BindingSet> a = answers[id];

                public void handle(final BindingSet result) {
                    //System.out.println("result: " + result);
                    a.add(result);
                }
            };

            queryEngine.addQuery(QUERY_TTL, t, h);

            i++;
        }

        for (Statement s : data) {
            queryEngine.addStatements(TUPLE_TTL, s);
        }

        return answers;
    }

    protected Collection<BindingSet> continuousQueryAnswers(final List<Statement> data,
                                                            final TupleExpr query,
                                                            final boolean debug) throws Exception {
        final Collection<BindingSet> answers = new LinkedList<BindingSet>();

        queryEngine.clear();

        BindingSetHandler h = new BindingSetHandler() {
            public void handle(final BindingSet result) {
                if (debug) {
                    System.out.println("result: " + result);
                }
                answers.add(result);
            }
        };

        queryEngine.addQuery(QUERY_TTL, query, h);

        for (Statement s : data) {
            queryEngine.addStatements(TUPLE_TTL, s);
        }

        Set<BindingSet> distinct = new HashSet<BindingSet>();
        distinct.addAll(answers);

        /*
        if (debug) {
            System.out.println("" + answers.size() + " solutions ("
                    + distinct.size() + " distinct, " + countPartialSolutions()
                    + " partial) from " + data.size() + " statements");
        }
        */

        return answers;
    }

    private long count;

    /*
    protected synchronized long countPartialSolutions() {
        count = 0;

        queryEngine.getIndex().visitPartialSolutions(new Visitor<PartialSolution>() {
            public boolean visit(final PartialSolution ps) {
                count++;
                return true;
            }
        });

        return count;
    }
    */

    public class NullBindingSetHandler implements BindingSetHandler {
        public void handle(final BindingSet result) {
        }
    }
}
