package net.fortytwo.stream;

import net.fortytwo.stream.sparql.etc.SparqlTestBase;
import net.fortytwo.stream.sparql.impl.caching.CachingSparqlStreamProcessor;
import net.fortytwo.linkeddata.LinkedDataCache;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Namespace;
import org.openrdf.model.Statement;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.memory.MemoryStore;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class LinkedDataIT extends SparqlTestBase {
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
    public void testSimple() throws Exception {
        LinkedDataCache.DataStore store = sc -> statement -> queryEngine.addInputs(TUPLE_TTL, statement);

        LinkedDataCache cache = LinkedDataCache.createDefault(sail);
        cache.setDataStore(store);
        queryEngine.setLinkedDataCache(cache);

        TupleExpr query = loadQuery("linked-data-join-1.rq");
        queryEngine.addQuery(QUERY_TTL, query, simpleConsumer);
        for (Statement s : loadData("example.nq")) {
            queryEngine.addInputs(TUPLE_TTL, s);
        }
    }
}
