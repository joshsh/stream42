package net.fortytwo.stream;

import net.fortytwo.stream.sparql.etc.SparqlTestBase;
import net.fortytwo.stream.sparql.impl.caching.CachingSparqlStreamProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.sail.memory.MemoryStore;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SP2BenchIT extends SparqlTestBase {
    private static final boolean DEBUG = true;

    private final List<Statement> data;

    public SP2BenchIT() throws Exception {
        data = loadData("/tmp/sp2bench-50000.nt");
    }

    @Before
    public void setUp() throws Exception {
        sail = new MemoryStore();
        sail.initialize();

        queryEngine = new CachingSparqlStreamProcessor();
    }

    @After
    public void tearDown() throws Exception {
        sail.shutDown();
    }

    @Test
    public void testQuery1() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers(data, loadQuery("sp2bench/q1.rq"), DEBUG);

        assertEquals(1, answers.size());

        assertEquals("1940", answers.iterator().next().getValue("yr").stringValue());
    }

    // query #2 contains an ORDER BY modifier, which is not compatible with SesameStream's infinite stream model
    @Test(expected = StreamProcessor.IncompatibleQueryException.class)
    public void testQuery2() throws Exception {
        continuousQueryAnswers(data, loadQuery("sp2bench/q2.rq"), false);
    }

    @Test
    public void testQuery3a() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers(data, loadQuery("sp2bench/q3a.rq"), DEBUG);

        assertEquals(3647, answers.size());
    }

    @Test
    public void testQuery3b() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers(data, loadQuery("sp2bench/q3b.rq"), DEBUG);

        assertEquals(25, answers.size());
    }

    @Test
    public void testQuery3c() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers(data, loadQuery("sp2bench/q3c.rq"), DEBUG);

        assertEquals(0, answers.size());
    }

    /* TODO: investigate space complexity of query #4; how much memory required by the most efficient in-memory algo
    @Test
    public void testQuery4() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers("sp2bench/q4.rq");

        assertEquals(104746, answers.size());
    }*/

    /* TODO: space complexity analysis of query #5a
    @Test
    public void testQuery5a() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers("sp2bench/q5a.rq");

        assertEquals(1085, answers.size());
    }
    */

    /* TODO: space complexity analysis of query #5b
    @Test
    public void testQuery5b() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers("sp2bench/q5b.rq");

        assertEquals(1085, answers.size());
    }
    */

    /* TODO: space complexity analysis of query #5a (and therefore query #12a)
    @Test
    public void testQuery12a() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers(data, loadQuery("sp2bench/q12a.rq"), DEBUG);

        assertEquals(1, answers.size());
    }
    */

    @Test
    public void testQuery12c() throws Exception {
        Collection<BindingSet> answers = continuousQueryAnswers(data, loadQuery("sp2bench/q12c.rq"), DEBUG);

        assertEquals(0, answers.size());
    }
}
