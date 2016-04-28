package net.fortytwo.stream;

import net.fortytwo.stream.sparql.etc.SparqlTestBase;
import net.fortytwo.stream.sparql.impl.caching.CachingSparqlStreamProcessor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.sail.memory.MemoryStore;

import java.util.List;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class DBpediaIT extends SparqlTestBase {

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
    public void testQuery3() throws Exception {
        TupleExpr q3 = loadQuery("dbpedia-q3.rq");

        queryEngine.addQuery(QUERY_TTL, q3, simpleConsumer);

        List<Statement> l = loadData("/tmp/dbpedia-singlefile-randomized-100000.nt");
        long i = 0;
        long max = 100000;
        for (Statement st : l) {
            if (i++ >= max) {
                break;
            }

            queryEngine.addInputs(TUPLE_TTL, st);
        }

        //queryEngine.getIndex().print();
    }
}
