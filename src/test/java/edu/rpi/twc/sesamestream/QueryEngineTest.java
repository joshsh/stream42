package edu.rpi.twc.sesamestream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.sail.memory.MemoryStore;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryEngineTest extends QueryEngineTestBase {

    @Before
    public void setUp() throws Exception {
        sail = new MemoryStore();
        sail.initialize();

        queryEngine = new QueryEngine();
    }

    @After
    public void tearDown() throws Exception {
        sail.shutDown();
    }

    /*
    @Test
    public void testManually() throws Exception {
        TupleExpr q = loadQuery("simple-join-in.rq");

        System.out.println("########## a");

        queryEngine.printIndex();

        System.out.println("########## s");

        queryEngine.addQuery(q, simpleBindingSetHandler);
        queryEngine.printIndex();

        System.out.println("########## d");

        queryEngine.addStatement(vf.createStatement(arthur, knows, zaphod));
        queryEngine.printIndex();

        System.out.println("########## f");

        queryEngine.addStatement(vf.createStatement(zaphod, knows, ford));
        queryEngine.printIndex();

        System.out.println("########## g");
    }
    */

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

    @Test
    public void testDistinct() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers(
                loadData("example.nq"), loadQuery("exponential-join-nodistinct.rq"), false);
        assertTrue(answers.size() > 5);

        answers = continuousQueryAnswers(
                loadData("example.nq"), loadQuery("exponential-join-distinct.rq"), false);
        assertEquals(5, answers.size());
    }

    @Test
    public void testCircleJoin() throws Exception {
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
    public void testAsk() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers(
                loadData("example.nq"), loadQuery("ask-1.rq"), false);
        // this query has 3 answers as a SELECT
        assertEquals(1, answers.size());

        answers = continuousQueryAnswers(
                loadData("example.nq"), loadQuery("ask-2.rq"), false);
        assertEquals(0, answers.size());
    }

    @Test(expected = Query.IncompatibleQueryException.class)
    public void testConstruct() throws Exception {
        continuousQueryAnswers(
                loadData("example.nq"), loadQuery("construct.rq"), false);
    }

    @Test(expected = Query.IncompatibleQueryException.class)
    public void testDescribe() throws Exception {
        continuousQueryAnswers(
                loadData("example.nq"), loadQuery("describe.rq"), false);
    }

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

    /*
    @Test
    public void testExtendoQueries() throws Exception {
        compareAnswers(
                loadData("extendo-gestures.nt"),
                loadQuery("extendo-gestures.rq"));
    }
    */

    @Test(expected = Query.IncompatibleQueryException.class)
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
