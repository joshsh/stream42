package edu.rpi.twc.sesamestream;

import net.fortytwo.flow.NullSink;
import net.fortytwo.flow.Sink;
import net.fortytwo.flow.rdf.RDFSink;
import net.fortytwo.linkeddata.LinkedDataCache;
import net.fortytwo.ripple.RippleException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Namespace;
import org.openrdf.model.Statement;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.memory.MemoryStore;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class LinkedDataTest extends QueryEngineTest {
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
    public void testSimple() throws Exception {
        LinkedDataCache.DataStore store = new LinkedDataCache.DataStore() {
            public RDFSink createInputSink(SailConnection sc) {
                return new RDFSink() {
                    public Sink<Statement> statementSink() {
                        return new Sink<Statement>() {
                            public void put(final Statement s) throws RippleException {
                                queryEngine.addStatement(s);
                            }
                        };
                    }

                    public Sink<Namespace> namespaceSink() {
                        return new NullSink<Namespace>();
                    }

                    public Sink<String> commentSink() {
                        return new NullSink<String>();
                    }
                };
            }
        };

        LinkedDataCache cache = LinkedDataCache.createDefault(sail);
        cache.setDataStore(store);
        queryEngine.setLinkedDataCache(cache, sail);

        TupleExpr query = loadQuery("linked-data-join-1.rq");
        queryEngine.addQuery(query, simpleBindingSetHandler);
        for (Statement s : loadData("example.nq")) {
            queryEngine.addStatement(s);
        }
    }
}
