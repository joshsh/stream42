package net.fortytwo.stream;

import net.fortytwo.stream.sparql.RDFStreamProcessor;
import net.fortytwo.stream.sparql.SparqlStreamProcessor;
import net.fortytwo.stream.sparql.etc.SparqlTestBase;
import net.fortytwo.stream.sparql.impl.caching.CachingSparqlStreamProcessor;
import net.fortytwo.stream.sparql.impl.shj.SHJSparqlStreamProcessor;
import info.aduna.io.IOUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.sail.memory.MemoryStore;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SparqlStreamProcessorTest extends SparqlTestBase {
    private static final int QUERY_TTL = 0;

    private static final String[] LUBM_QUERIES
            = {"q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q11", "q12", "q13", "q14"};

    private static final String[] INVALID_QUERIES = {"construct-incomplete"};

    private static final String[] INCOMPATIBLE_QUERIES = {"not-equal-filters"};

    private final String ex = "http://example.org/";
    private final String foaf = "http://xmlns.com/foaf/0.1/";
    private URI arthur = vf.createURI(ex + "arthur");
    private URI zaphod = vf.createURI(ex + "zaphod");
    private URI ford = vf.createURI(ex + "ford");
    private URI knows = vf.createURI(foaf + "knows");

    @Before
    public void setUp() throws Exception {
        sail = new MemoryStore();
        sail.initialize();

        //queryEngine = new CachingSparqlStreamProcessor();
        queryEngine = new SHJSparqlStreamProcessor();
    }

    @After
    public void tearDown() throws Exception {
        sail.shutDown();
    }

    @Test
    public void testAddLubmQueries() throws Exception {
        CachingSparqlStreamProcessor engine = new CachingSparqlStreamProcessor();

        BiConsumer<BindingSet, Long> bsh = new BiConsumer<BindingSet, Long>() {
            @Override
            public void accept(BindingSet result, Long expirationTime) {
                // Do nothing.
            }
        };

        for (String q : LUBM_QUERIES) {
            String name = "lubm/" + q + ".rq";
            try (InputStream in = RDFStreamProcessor.class.getResourceAsStream(name)) {
                String query = IOUtil.readString(in);
                engine.addQuery(QUERY_TTL, query, bsh);
            }
        }
    }

    @Test
    public void testInvalidQueries() throws Exception {
        CachingSparqlStreamProcessor engine = new CachingSparqlStreamProcessor();

        BiConsumer<BindingSet, Long> bsh = new BiConsumer<BindingSet, Long>() {
            @Override
            public void accept(BindingSet result, Long expirationTime) {
                // Do nothing.
            }
        };

        for (String q : INVALID_QUERIES) {
            String name = "invalid/" + q + ".rq";
            boolean invalid = false;
            try (InputStream in = RDFStreamProcessor.class.getResourceAsStream(name)) {
                String query = IOUtil.readString(in);
                try {
                    engine.addQuery(QUERY_TTL, query, bsh);
                } catch (StreamProcessor.InvalidQueryException e) {
                    invalid = true;
                }
                assertTrue(invalid);
            }
        }
    }

    @Test
    public void testIncompatibleQueries() throws Exception {
        CachingSparqlStreamProcessor engine = new CachingSparqlStreamProcessor();

        BiConsumer<BindingSet, Long> bsh = new BiConsumer<BindingSet, Long>() {
            @Override
            public void accept(BindingSet result, Long expirationTime) {
                // Do nothing.
            }
        };

        for (String q : INCOMPATIBLE_QUERIES) {
            String name = "incompatible/" + q + ".rq";
            boolean incompatible = false;
            try (InputStream in = RDFStreamProcessor.class.getResourceAsStream(name)) {
                String query = IOUtil.readString(in);
                try {
                    engine.addQuery(QUERY_TTL, query, bsh);
                } catch (StreamProcessor.IncompatibleQueryException e) {
                    incompatible = true;
                }
                assertTrue(incompatible);
            }
        }
    }

    @Test
    public void testSimple() throws Exception {
        compareAnswers(
                loadData("example.nq"),
                loadQuery("simple-sp.rq"));
    }

    @Test
    public void testSingleJoinIn() throws Exception {
        compareAnswers(
                loadData("example.nq"),
                loadQuery("simple-join-in.rq"));
    }

    @Test
    public void testSingleJoinOut() throws Exception {
        compareAnswers(
                loadData("example.nq"),
                loadQuery("simple-join-out.rq"));
    }

    @Test
    public void testUnselectedVariables() throws Exception {
        compareAnswers(
                loadData("example.nq"),
                loadQuery("unselected-variables.rq"));
    }

    @Test
    public void testProjection() throws Exception {
        compareAnswers(
                loadData("example.nq"),
                loadQuery("simple-sp-with-proj.rq"));

        compareAnswers(
                loadData("example.nq"),
                loadQuery("simple-join-out-with-proj.rq"));
    }

    @Test
    public void testSimultaneousQueries() throws Exception {
        compareAnswers(
                loadData("example.nq"),
                loadQuery("simple-join-in.rq"),
                loadQuery("simple-join-out.rq"));
    }

    @Test
    public void testMultipleJoins() throws Exception {
        compareAnswers(
                loadData("example.nq"),
                loadQuery("multiple-join-1.rq"));
    }

    /*
    solution sets in R:

    edges <- c("AT", "AJ", "AF", "AZ", "FA", "FZ", "ZF")
    g1 <- data.frame(v1=substr(edges, 1, 1), v2=substr(edges, 2, 2))
    g2 <- merge(g1, data.frame(v1=g1$v1, v3=g1$v2), by.x="v2", by.y="v1")[,c(2,1,3)]
    g3 <- merge(g2, data.frame(v1=g1$v1, v4=g1$v2), by.x="v3", by.y="v1")[,c(2,3,1,4)]
    g4 <- merge(g3, data.frame(v1=g1$v1, v5=g1$v2), by.x="v4", by.y="v1")[,c(2,3,4,1,5)]
     */
    @Test
    public void testDistinct() throws Exception {
        Collection<BindingSet> answers;

        answers = distinctContinuousQueryAnswers(
                loadData("example.nq"), loadQuery("exponential-join-all-nodistinct.rq"))[0];
//        assertTrue(answers.size() >= 14);
        /*
        System.out.println("continuous:");
        for (BindingSet bs : answers) {
            System.out.println("\t" + bs);
        }
        */

        // There is no simple relationship between SesameStream's and MemoryStore's answers in the presence of loops.
        // SesameStream finds additional correct answers not produced by MemoryStore, but MemoryStore finds additional
        // answers in which statements are matched by more than one triple pattern.
        // SesameStream produces some such "pathological" answers as a by-product of query evaluation, but it does
        // not attempt to produce a complete set of such answers.
        answers = distinctStaticQueryAnswers(
                loadData("example.nq"), loadQuery("exponential-join-all-nodistinct.rq"))[0];
//        assertTrue(answers.size() >= 14);
      /*
        System.out.println("static:");
        for (BindingSet bs : answers) {
            System.out.println("\t" + bs);
        }
        */

        answers = continuousQueryAnswers(
                loadData("example.nq"), loadQuery("exponential-join-nodistinct.rq"), false);
//        assertTrue(answers.size() >= 14);

        answers = continuousQueryAnswers(
                loadData("example.nq"), loadQuery("exponential-join-distinct.rq"), false);
        // note: the number of distinct "non-pathological" solutions is 9
        // There are 27 distinct solutions
        assertEquals(13, answers.size());
    }

    @Test
    public void testReduced() throws Exception {
        long overflow = SparqlStreamProcessor.getReducedModifierCapacity();
        SparqlStreamProcessor.setReducedModifierCapacity(2);
        try {
            int raw = continuousQueryAnswers(
                    loadData("example.nq"), loadQuery("exponential-join-nodistinct.rq"), false).size();

            int distinct = continuousQueryAnswers(
                    loadData("example.nq"), loadQuery("exponential-join-distinct.rq"), false).size();

            int reduced = continuousQueryAnswers(
                    loadData("example.nq"), loadQuery("exponential-join-reduced.rq"), false).size();

            assertTrue(reduced <= raw);

            assertTrue(reduced > distinct);
        } finally {
            SparqlStreamProcessor.setReducedModifierCapacity(overflow);
        }
    }

    @Test
    public void testCircleJoins() throws Exception {
//        compareAnswers(
//                loadData("example.nq"),
//                loadQuery("simple-cycle.rq"));
        compareAnswers(
                loadData("simple-cycle.nq"),
                loadQuery("circle-join.rq"));
        compareAnswers(
                loadData("example.nq"),
                loadQuery("circle-join.rq"));
    }

    @Test
    public void testFilters() throws Exception {
        compareAnswers(
                loadData("example.nq"),
                loadQuery("filter-regex.rq"));

        // projection + FILTER
        compareAnswers(
                loadData("example.nq"),
                loadQuery("filter-with-projection.rq"));

        // SesameStream inherits its filter function implementations from Sesame,
        // so these don't need to be exhaustively tested here.
        // For each of the categories in the SPARQL spec, just try one or two functions.
        // EXISTS is the only function not assumed to be supported (see dedicated test case).
        Set<BindingSet> answers;

        // try a filter with Functional Forms
        answers = distinctContinuousQueryAnswers(loadData("example.nq"),
                loadQuery("filter-equal.rq"))[0];
        // 9 solutions without the filter, 6 with
        assertEquals(6, answers.size());
        // TODO: if/when OPTIONAL is supported, try the BOUND filter

        // try an RDF term filter
        answers = distinctContinuousQueryAnswers(loadData("example.nq"),
                loadQuery("filter-isLiteral.rq"))[0];
        assertEquals(10, answers.size());

        // try a string filter
        answers = distinctContinuousQueryAnswers(loadData("example.nq"),
                loadQuery("filter-regex.rq"))[0];
        assertEquals(1, answers.size());
        assertEquals("Zaphod Beeblebrox", answers.iterator().next().getValue("name").stringValue());

        // try a numeric filter
        answers = distinctContinuousQueryAnswers(loadData("example.nq"),
                loadQuery("filter-ceil.rq"))[0];
        assertEquals(2, answers.size());
        assertEquals("3.1415926", answers.iterator().next().getValue("v").stringValue());
        assertEquals("3.1415926", answers.iterator().next().getValue("v").stringValue());

        // try a date/time filter
        answers = distinctContinuousQueryAnswers(loadData("example.nq"),
                loadQuery("filter-year.rq"))[0];
        // there are 2 dateTime values which would match without the filter (one in 2002, and one in 2013)
        assertEquals(1, answers.size());

        // try a hash filter
        answers = distinctContinuousQueryAnswers(loadData("example.nq"),
                loadQuery("filter-md5.rq"))[0];
        // there are 3 literal values with the lexical form "42", md5 hash "a1d0c6e83f027327d8461063f4ac58a6"
        assertEquals(3, answers.size());
    }

    @Test
    public void testLimitAndOffset() throws Exception {
        Collection<BindingSet> answers;

        answers = continuousQueryAnswers(loadData("example.nq"),
                loadQuery("limit.rq"), false);
        // there would be 5 answers without the LIMIT
        assertEquals(2, answers.size());

        answers = continuousQueryAnswers(loadData("example.nq"),
                loadQuery("offset.rq"), false);
        // there would be 5 answers without the OFFSET
        assertEquals(3, answers.size());
    }

    @Test
    public void testAsk() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers(
                loadData("example.nq"), loadQuery("ask-1.rq"), false);
        // this query has 3 answers as a SELECT
        assertEquals(1, answers.size());

        answers = continuousQueryAnswers(
                loadData("example.nq"), loadQuery("ask-2.rq"), false);
        assertEquals(0, answers.size());
    }

    @Test
    public void testConstruct() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers(
                loadData("example.nq"), loadQuery("construct.rq"), false);
        assertTrue(answers.size() >= 5);
        for (BindingSet b : answers) {
            assertNotNull(b.getValue("subject"));
            assertEquals(vf.createURI("http://xmlns.com/foaf/0.1/name"), b.getValue("predicate"));
            assertNotNull(b.getValue("object"));
        }
    }

    @Test(expected = StreamProcessor.IncompatibleQueryException.class)
    public void testDescribe() throws Exception {
        continuousQueryAnswers(
                loadData("example.nq"), loadQuery("describe.rq"), false);
    }

    /* TODO: remove or re-evaluate; partial solutions are no longer counted
    @Test
    public void testIrrelevantStatementsAreNotIndexed() throws Exception {

        queryEngine.addQuery(loadQuery("simple-join-in.rq"), new NullBindingSetHandler());
        assertEquals(2, countPartialSolutions());

        queryEngine.addStatement(vf.createStatement(arthur, knows, zaphod));
        assertEquals(3, countPartialSolutions());

        // irrelevant statement; no change
        queryEngine.addStatement(vf.createStatement(arthur, RDF.TYPE, arthur));
        assertEquals(3, countPartialSolutions());

        // irrelevant statement; no change
        queryEngine.addStatement(vf.createStatement(arthur, RDF.TYPE, ford));
        assertEquals(3, countPartialSolutions());

        // irrelevant statement; no change
        queryEngine.addStatement(vf.createStatement(ford, knows, arthur));
        assertEquals(3, countPartialSolutions());
    }
    */

    /*
    @Test
    public void testExtendoQueries() throws Exception {
        compareAnswers(
                loadData("extendo-gestures.nt"),
                loadQuery("extendo-gestures.rq"));
    }
    */

    @Test
    public void testQueryTime() throws Exception {
        Collection<BindingSet> answers;

        long startTime, endTime;
        startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            answers = distinctStaticQueryAnswers(
                    loadData("example.nq"), loadQuery("exponential-join-nodistinct.rq"))[0];
            //assertEquals(13, answers.size());
        }
        endTime = System.currentTimeMillis();
        System.out.println("MemoryStore query time: " + (endTime - startTime));

        startTime = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            answers = continuousQueryAnswers(
                    loadData("example.nq"), loadQuery("exponential-join-nodistinct.rq"), false);
            //assertEquals(9, answers.size());
            //assertTrue(answers.size() > 13);
        }
        endTime = System.currentTimeMillis();
        System.out.println("SesameStream query time: " + (endTime - startTime));

    }

    @Test(expected = StreamProcessor.IncompatibleQueryException.class)
    public void testNotExistsUnsupported() throws Exception {
        continuousQueryAnswers(loadData("example.nq"), loadQuery("not-exists.rq"), false);
    }

    protected void compareAnswers(final List<Statement> data,
                                  final TupleExpr... queries) throws Exception {
        Set<BindingSet>[] staticResults = distinctStaticQueryAnswers(data, queries);
        Set<BindingSet>[] contResults = distinctContinuousQueryAnswers(data, queries);

        for (int i = 0; i < queries.length; i++) {
            Set<BindingSet> staticR = staticResults[i];
            Set<BindingSet> contR = contResults[i];

            for (BindingSet b : staticR) {
                assertTrue("expected result not found for query " + i + ": " + b, contR.contains(b));
            }

            for (BindingSet b : contR) {
                assertTrue("unexpected result for query " + i + ": " + b, staticR.contains(b));
            }
        }
    }
}
