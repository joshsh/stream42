package net.fortytwo.stream.shj;

import net.fortytwo.stream.StreamProcessor;
import net.fortytwo.stream.model.VariableOrConstant;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SHJTestBase {

    protected ExpirationManager<Solution<String>> solutionExpirationManager;
    protected ExpirationManager<Query<String, String>> queryExpirationManager;

    private long simTime;

    protected Random random = new Random();

    protected QueryContext<String, String> context;

    protected QueryIndex<String, String> queryIndex;

    private Map<String, Set<CompleteSolution<String, String>>> solutionsByName
            = new HashMap<>();

    protected Index<Solution<String>> noopIndex = new Index<Solution<String>>() {
        @Override
        public void add(Solution<String> toAdd) {
            // do nothing
        }

        @Override
        public boolean remove(Solution<String> toRemove) {
            return false;
        }

        @Override
        public void clear() {
            // do nothing
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    };

    @Before
    public void setUp() {
        solutionsByName.clear();

        queryExpirationManager = new ExpirationManager<Query<String, String>>() {
            @Override
            protected long getNow() {
                return simTime;
            }
        };
        queryExpirationManager.setVerbose(true);

        solutionExpirationManager = new ExpirationManager<Solution<String>>() {
            @Override
            protected long getNow() {
                return simTime;
            }
        };
        solutionExpirationManager.setVerbose(true);

        context = new QueryContext<>(queryExpirationManager, solutionExpirationManager);

        queryIndex = new QueryIndex<>(context);
    }

    @After
    public void tearDown() {
        queryExpirationManager.stop();
        solutionExpirationManager.stop();
    }

    protected long setCurrentTime(long now) {
        simTime = now;
        return simTime;
    }

    protected Solution<String> newSolution(String[] values) {
        return new Solution<>(values, StreamProcessor.NEVER_EXPIRE, noopIndex);
    }

    protected Solution<String> newSolution(String[] values, long expTime) {
        return new Solution<>(values, expTime, noopIndex);
    }

    protected Solution<String> randomSolution(int tupleLength, long expTime) {
        String[] values = new String[tupleLength];
        for (int i = 0; i < values.length; i++) {
            values[i] = "value" + random.nextInt();
        }
        return new Solution<>(values, expTime, noopIndex);
    }

    protected GraphPattern<String, String> graphPattern(TuplePattern<String, String>... patterns) {
        if (0 == patterns.length) {
            throw new IllegalArgumentException();
        }

        return new GraphPattern<>(patterns);
    }

    protected TuplePattern<String, String> tuplePattern(String... values) {
        VariableOrConstant<String, String>[] pattern = new VariableOrConstant[values.length];
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            pattern[i] = value.startsWith("?")
                    ? new VariableOrConstant<String, String>(value, null)
                    : new VariableOrConstant<String, String>(null, value);
        }

        return new TuplePattern<>(pattern);
    }

    protected String[] tuple(String... values) {
        return values;
    }

    protected BiConsumer<Map<String, String>, Long> createConsumer(final String name) {
        return new BiConsumer<Map<String, String>, Long>() {
            @Override
            public void accept(Map<String, String> mapping, Long expTime) {
                Set<CompleteSolution<String, String>> set = solutionsByName.get(name);
                if (null == set) {
                    set = new HashSet<>();
                    solutionsByName.put(name, set);
                }
                CompleteSolution<String, String> comp = new CompleteSolution<>(mapping, expTime);
                set.add(comp);
            }
        };
    }

    protected static class CompleteSolution<K, V> {
        public final Map<K, V> mapping;
        public final long expirationTime;

        private CompleteSolution(Map<K, V> mapping, long expirationTime) {
            this.mapping = mapping;
            this.expirationTime = expirationTime;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (!(other instanceof CompleteSolution)) return false;

            CompleteSolution<K, V> otherCS = (CompleteSolution<K, V>) other;
            if (mapping.size() != otherCS.mapping.size()) return false;
            if (hashCode() != otherCS.hashCode()) return false;

            for (Map.Entry<K, V> e : mapping.entrySet()) {
                V val = otherCS.mapping.get(e.getKey());
                if (null == val || !val.equals(e.getValue())) return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int hc = 0;
            for (Map.Entry<K, V> e : mapping.entrySet()) {
                hc += (e.getKey().hashCode() * e.getValue().hashCode());
            }
            return hc;
        }

        public String debug() {
            List keyList = new LinkedList();
            keyList.addAll(mapping.keySet());
            Collections.sort(keyList);
            StringBuilder sb = new StringBuilder();
            for (Object key : keyList) {
                sb.append(((String) mapping.get(key)).substring(0, 1));
            }

            return sb.toString() + " --> " + hashCode();
        }
    }

    protected Query<String, String> addQuery(String name, GraphPattern<String, String> pattern) {
        Query<String, String> query;
        List<JoinHelper<String, String>> consumers;

        query = new Query<>(
                pattern, StreamProcessor.NEVER_EXPIRE,
                context.getQueryExpirationManager(),
                createConsumer(name));
        queryIndex.add(query);
        consumers = query.getAllHelpers();
        assertEquals(pattern.getPatterns().length, consumers.size());
        return query;
    }

    protected void expectQuerySolutions(String queryName, int expectedCount) {
        // note: the set eliminates duplicate solutions; any redundancy is ignored
        Set<CompleteSolution<String, String>> sols = solutionsByName.get(queryName);
        if (0 == expectedCount) {
            assertNull(sols);
        } else {
            assertNotNull(sols);

            /*
            System.out.println("### solutions:");
            for (CompleteSolution<String, String> sol : sols) {
                System.out.println("\t" + sol.debug());
            }
            System.out.println("### solution map:");
            Map<String, CompleteSolution<String, String>> byDebug = new HashMap<String, CompleteSolution<String, String>>();
            for (CompleteSolution<String, String> s : sols) {
                String d = s.debug();
                CompleteSolution<String, String> existing = byDebug.get(d);
                if (null != existing && existing != s) {
                    System.out.println("\t duplicate: " + s + ", "  + existing);
                } else {
                    byDebug.put(d, s);
                }
            }*/

            assertEquals(expectedCount, sols.size());
        }
    }

    protected void expectIndexSolutions(
            JoinHelper<String, String> consumer, int expectedCount) {

        Set<Solution<String>> solutions = consumer.getSolutions();
        assertEquals(expectedCount, solutions.size());
    }

    protected void expectSolutions(
            JoinHelper<String, String> consumer, int expectedCount, String key, String value) {

        Set<Solution<String>> solutions = consumer.getSolutions(key, value);
        if (0 == expectedCount) {
            assertNull(solutions);
        } else {
            assertNotNull(solutions);
            assertEquals(expectedCount, solutions.size());
        }
    }
}
