package edu.rpi.twc.sesamestream.etc;

import edu.rpi.twc.sesamestream.BindingSetHandler;
import edu.rpi.twc.sesamestream.QueryEngine;
import edu.rpi.twc.sesamestream.etc.ErrorTolerantValueFactory;
import edu.rpi.twc.sesamestream.etc.QueryEngineAdder;
import edu.rpi.twc.sesamestream.impl.QueryEngineImpl;
import info.aduna.io.IOUtil;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

/**
 * A command-line utility for executing continuous query (*.rq) files against RDF data (*.nt) files in the N-Triples format.
 * The main method expects two arguments: a file containing a number of queries (one file name per line)
 * and another file containing a number of data sets (one file name per line).
 * The queries are added first, in order, followed by the data sets.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class TestRunner {
    public static void main(final String[] args) throws Exception {
        if (2 != args.length) {
            printUsageAndExit();
        }

        try {
            List<String> queryFiles = getLines(args[0]);
            List<String> dataFiles = getLines(args[1]);

            doRun(queryFiles, dataFiles);
        } catch (Throwable t) {
            t.printStackTrace(System.err);
            System.exit(1);
        }
    }

    private static void doRun(final List<String> queryFiles,
                              final List<String> dataFiles) throws IOException, MalformedQueryException, QueryEngine.IncompatibleQueryException, RDFHandlerException {
        QueryEngineImpl engine = new QueryEngineImpl();
        QueryParser queryParser = new SPARQLParser();
        String baseUri = "http://example.org/base-uri/";

        BindingSetHandler bsh = new BindingSetHandler() {
            public void handle(final BindingSet result) {
                StringBuilder sb = new StringBuilder("RESULT\t" + System.currentTimeMillis() + "\t");

                boolean first = true;
                for (String n : result.getBindingNames()) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append(", ");
                    }
                    sb.append(n).append(":").append(result.getValue(n));
                }

                System.out.println(sb);
            }
        };

        for (String f : queryFiles) {
            System.out.println("RUN\t" + System.currentTimeMillis() + "\tadding query file " + f);
            InputStream in = new FileInputStream(new File(f));
            try {
                String query = IOUtil.readString(in);
                ParsedQuery pq = queryParser.parseQuery(query, baseUri);

                engine.addQuery(pq.getTupleExpr(), bsh);
            } finally {
                in.close();
            }
        }

        for (String f : dataFiles) {
            System.out.println("RUN\t" + System.currentTimeMillis() + "\tadding data file " + f);

            RDFFormat format = RDFFormat.forFileName(f);

            if (null == format) {
                System.err.println("no RDF format matching file name " + f);
                continue;
            }

            //StatementListBuilder h = new StatementListBuilder();
            RDFHandler h = new QueryEngineAdder(engine);

            InputStream in = new FileInputStream(new File(f));
            try {
                RDFParser p = Rio.createParser(format);
                p.setValueFactory(new ErrorTolerantValueFactory(new ValueFactoryImpl()));
                p.setStopAtFirstError(false);
                p.setVerifyData(false);
                p.setRDFHandler(h);
                try {
                    p.parse(in, baseUri);
                } catch (RDFParseException e) {
                    System.err.println("RUN ERROR: parse error: " + e.getMessage());
                }
            } finally {
                in.close();
            }

            //for (Statement s : h.getStatements()) {
            //    engine.addStatement(s);
            //}
        }

        System.out.println("RUN\t" + System.currentTimeMillis() + "\tfinished");
    }

    private static List<String> getLines(final String fileName) throws IOException {
        List<String> lines = new LinkedList<String>();
        InputStream in = new FileInputStream(new File(fileName));
        try {
            BufferedReader b = new BufferedReader(new InputStreamReader(in));
            String line;
            while (null != (line = b.readLine())) {
                lines.add(line.trim());
            }
        } finally {
            in.close();
        }

        return lines;
    }

    private static void printUsageAndExit() {
        System.err.println("Usage: TestRunner list-of-queries.txt list-of-data-files.txt");
        System.exit(1);
    }
}
