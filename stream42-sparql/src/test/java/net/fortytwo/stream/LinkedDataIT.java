package net.fortytwo.stream;

import net.fortytwo.flow.NullSink;
import net.fortytwo.flow.rdf.RDFSink;
import net.fortytwo.linkeddata.LinkedDataCache;
import net.fortytwo.stream.sparql.etc.SparqlTestBase;
import net.fortytwo.stream.sparql.impl.caching.CachingSparqlStreamProcessor;
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
        LinkedDataCache.DataStore store = new LinkedDataCache.DataStore() {
            public RDFSink createInputSink(SailConnection sc) {
                return new RDFSink() {
                    public Consumer<Statement> statementSink() {
                        return new Consumer<Statement>() {
                            public void accept(final Statement s) {
                                try {
                                    queryEngine.addInputs(TUPLE_TTL, s);
                                } catch (IOException e) {
                                    logger.warning("failed to add statement: " + s);
                                }
                            }
                        };
                    }

                    public Consumer<Namespace> namespaceSink() {
                        return new NullSink<>();
                    }

                    public Consumer<String> commentSink() {
                        return new NullSink<>();
                    }
                };
            }
        };

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
