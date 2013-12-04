package edu.rpi.twc.sesamestream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.sail.memory.MemoryStore;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SP2BenchTest extends QueryEngineTestBase {
    private final List<Statement> data;

    public SP2BenchTest() throws Exception {
        data = loadData("/tmp/sp2bench-50000.nt");
    }

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

    @Test
    public void testQuery1() throws Exception {
        Set<BindingSet> answers = continuousQueryAnswers("sp2bench/q1.rq");

        assertEquals(1, answers.size());

        assertEquals("1940", answers.iterator().next().getValue("yr").stringValue());
    }

    // query #2 contains an ORDER BY modifier, which is not compatible with SesameStream's infinite stream model
    @Test(expected = Query.IncompatibleQueryException.class)
    public void testQuery2() throws Exception {
        continuousQueryAnswers("sp2bench/q2.rq");
    }

    @Test
    public void testQuery3a() throws Exception {
        Set<BindingSet> answers = continuousQueryAnswers("sp2bench/q3a.rq");

        assertEquals(3647, answers.size());
    }

    @Test
    public void testQuery3b() throws Exception {
        Set<BindingSet> answers = continuousQueryAnswers("sp2bench/q3b.rq");

        assertEquals(25, answers.size());
    }

    @Test
    public void testQuery3c() throws Exception {
        Set<BindingSet> answers = continuousQueryAnswers("sp2bench/q3c.rq");

        assertEquals(0, answers.size());
    }

    private Set<BindingSet> continuousQueryAnswers(final String queryLocation) throws Exception {
        queryEngine.clear();

        Set<BindingSet> answers = continuousQueryAnswers(data,
                loadQuery(queryLocation))[0];

        int count = 0;
        for (BindingSet bs : answers) {
            System.out.println("result: " + bs);

            if (++count >= 10) {
                break;
            }
        }

        System.out.println("" + answers.size() + " distinct solutions (" + countPartialSolutions() + " partial) from " + data.size() + " statements");

        return answers;
    }
}
