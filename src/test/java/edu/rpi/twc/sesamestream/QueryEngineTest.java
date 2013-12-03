package edu.rpi.twc.sesamestream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.sail.memory.MemoryStore;

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
    public void testCircleJoin() throws Exception {
        compareAnswers(
                loadData("example.nq"),
                loadQuery("circle-join.rq"));
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

    protected void compareAnswers(final List<Statement> data,
                                  final TupleExpr... queries) throws Exception {
        Set<BindingSet>[] staticResults = staticQueryAnswers(data, queries);
        Set<BindingSet>[] contResults = continuousQueryAnswers(data, queries);

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
