package net.fortytwo.stream.sparql.etc;

import info.aduna.io.IOUtil;
import net.fortytwo.stream.StreamProcessor;
import net.fortytwo.stream.sparql.impl.caching.CachingSparqlStreamProcessor;
import org.openrdf.model.impl.SimpleValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserRegistry;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.BasicParserSettings;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * A command-line utility for executing continuous query (*.rq) files
 * against RDF data (*.nt) files in the N-Triples format.
 * The main method expects two arguments: a file containing a number of queries (one file name per line)
 * and another file containing a number of data sets (one file name per line).
 * The queries are added first, in order, followed by the data sets.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class TestRunner {
    private static final int TUPLE_TTL = 0, QUERY_TTL = 0;

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
                              final List<String> dataFiles)
            throws IOException, MalformedQueryException, StreamProcessor.IncompatibleQueryException, RDFHandlerException, StreamProcessor.InvalidQueryException {

        CachingSparqlStreamProcessor engine = new CachingSparqlStreamProcessor();
        String baseIri = "http://example.org/base-iri/";

        BiConsumer<BindingSet, Long> solutionConsumer = (result, expirationTime) -> {
            StringBuilder sb = new StringBuilder("RESULT\t" + System.currentTimeMillis() + "\t"
                    + expirationTime + "\t");

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
        };

        for (String f : queryFiles) {
            System.out.println("RUN\t" + System.currentTimeMillis() + "\tadding query file " + f);
            try (InputStream in = new FileInputStream(new File(f))) {
                String query = IOUtil.readString(in);
                engine.addQuery(QUERY_TTL, query, solutionConsumer);
            }
        }

        for (String f : dataFiles) {
            System.out.println("RUN\t" + System.currentTimeMillis() + "\tadding data file " + f);

            Optional<RDFFormat> format = RDFParserRegistry.getInstance().getFileFormatForFileName(f);

            if (!format.isPresent()) {
                System.err.println("no RDF format matching file name " + f);
                continue;
            }

            RDFHandler handler = engine.createRDFHandler(TUPLE_TTL);

            try (InputStream in = new FileInputStream(new File(f))) {
                RDFParser p = Rio.createParser(format.get());
                p.setValueFactory(new ErrorTolerantValueFactory(SimpleValueFactory.getInstance()));
                //p.setStopAtFirstError(false);
                p.getParserConfig().set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
                p.setRDFHandler(handler);
                try {
                    p.parse(in, baseIri);
                } catch (RDFParseException e) {
                    System.err.println("RUN ERROR: parse error: " + e.getMessage());
                }
            }
        }

        engine.shutDown();
        System.out.println("RUN\t" + System.currentTimeMillis() + "\tfinished");
    }

    private static List<String> getLines(final String fileName) throws IOException {
        List<String> lines = new LinkedList<>();
        try (InputStream in = new FileInputStream(new File(fileName))) {
            BufferedReader b = new BufferedReader(new InputStreamReader(in));
            String line;
            while (null != (line = b.readLine())) {
                lines.add(line.trim());
            }
        }

        return lines;
    }

    private static void printUsageAndExit() {
        System.err.println("Usage: TestRunner list-of-queries.txt list-of-data-files.txt");
        System.exit(1);
    }
}
