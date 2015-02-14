package edu.rpi.twc.sesamestream.examples;

import edu.rpi.twc.sesamestream.BindingSetHandler;
import edu.rpi.twc.sesamestream.Subscription;
import edu.rpi.twc.sesamestream.impl.QueryEngineImpl;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

import java.io.ByteArrayInputStream;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class BasicExample {
    public static void main(final String[] args) throws Exception {
        // A query for things written by Douglas Adams which are referenced with a pointing gesture
        String query = "PREFIX activity: <http://fortytwo.net/2015/extendo/activity#>\n" +
                "PREFIX dbo: <http://dbpedia.org/ontology/>\n" +
                "PREFIX dbr: <http://dbpedia.org/resource/>\n" +
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" +
                "SELECT ?actor ?indicated WHERE {\n" +
                "?a activity:thingIndicated ?indicated .\n" +
                "?a activity:actor ?actor .\n" +
                "?indicated dbo:author dbr:Douglas_Adams .\n" +
                "}";

        // An RDF graph representing an event. Normally, this would come from a dynamic data source.
        // The example is from the Typeatron keyer (see http://github.com/joshsh/extendo)
        String eventData = "@prefix activity: <http://fortytwo.net/2015/extendo/activity#> .\n" +
                "@prefix dbr: <http://dbpedia.org/resource/> .\n" +
                "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n" +
                "@prefix tl: <http://purl.org/NET/c4dm/timeline.owl#> .\n" +
                "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n" +
                "\n" +
                "<urn:uuid:e6f4c759-712c-448c-96f0-c2ecee2ccb97> a activity:Point ;\n" +
                "    activity:actor <http://fortytwo.net/josh/things/JdGwZ4n> ;\n" +
                "    activity:thingIndicated dbr:The_Meaning_of_Liff ;\n" +
                "    activity:recognitionTime <urn:uuid:a4a2fd8c-ea0d-43bb-bcad-6510f4c9b55a> .\n" +
                "\n" +
                "<urn:uuid:a4a2fd8c-ea0d-43bb-bcad-6510f4c9b55a> a tl:Instant ;\n" +
                "    tl:at \"2015-02-13T21:00:12-05:00\"^^xsd:dateTime .";

        // Instantiate the query engine.
        QueryEngineImpl queryEngine = new QueryEngineImpl();

        // Define a time-to-live for the query. It will expire after this many seconds,
        // freeing up resources and ceasing to match statements.
        int queryTtl = 10 * 60;

        // Define a handler for answers to the query.
        BindingSetHandler handler = new BindingSetHandler() {
            public void handle(final BindingSet answer) {
                System.out.println("found an answer to the query: " + answer);
            }
        };

        // Submit the query to the query engine to obtain a subscription.
        Subscription sub = queryEngine.addQuery(queryTtl, query, handler);

        // create subscriptions for additional queries at any time; queries match in parallel

        // Add some data with infinite (= 0) time-to-live.
        // Results derived from this data will never expire.
        int staticTtl = 0;

        // Add some static background knowledge.  Alternatively, let SesameStream discover this
        // information as Linked Data (see LinkedDataExample.java).
        Statement st = new StatementImpl(
                new URIImpl("http://dbpedia.org/resource/The_Meaning_of_Liff"),
                new URIImpl("http://dbpedia.org/ontology/author"),
                new URIImpl("http://dbpedia.org/resource/Douglas_Adams"));
        queryEngine.addStatements(staticTtl, st);

        // Now define a finite time-to-live of 30 seconds.
        // This will be used for the short-lived data of gesture events.
        int eventTtl = 30;

        RDFFormat format = RDFFormat.TURTLE;
        RDFParser parser = Rio.createParser(format);
        parser.setRDFHandler(queryEngine.createRDFHandler(eventTtl));
        // as new statements are added, computed query answers will be pushed to the BindingSetHandler
        parser.parse(new ByteArrayInputStream(eventData.getBytes()), "");

        // cancel the query subscription at any time;
        // no further answers will be computed/produced for the corresponding query
        sub.cancel();

        // alternatively, renew the subscription for another 10 minutes
        sub.renew(10 * 60);
    }
}
