package edu.rpi.twc.sesamestream.tuple;

import edu.rpi.twc.sesamestream.Subscription;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.fail;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryIndexTest {
    private GraphPattern.QueryVariables vars;
    private QueryIndex<String> queryIndex;
    private Subscription subscription1;

    @Before
    public void setUp() {
        subscription1 = new Subscription() {
            @Override
            public String getId() {
                return "query1";
            }

            @Override
            public boolean isActive() {
                return true;
            }

            @Override
            public void cancel() {
            }
        };

        vars = new GraphPattern.QueryVariables(Arrays.asList("x", "y", "z"));
        int tupleSize = 3;
        queryIndex = new QueryIndex<String>(tupleSize);
    }

    @Test
    public void testSimpleJoin() throws Exception {
        TuplePattern<String> pattern1 = new TuplePattern<String>(new Term[]{
                new Term(null, "x"), new Term("isRedderThan", null), new Term(null, "z")});
        TuplePattern<String> pattern2 = new TuplePattern<String>(new Term[]{
                new Term(null, "x"), new Term("htmlValue", null), new Term("ff0000", null)});
        List<TuplePattern<String>> patterns = new LinkedList<TuplePattern<String>>();
        patterns.add(pattern1);
        patterns.add(pattern2);
        GraphPattern<String> graphPattern = new GraphPattern<String>(subscription1, patterns);
        queryIndex.add(graphPattern);

        HashMap<String, String> map = new HashMap<String, String>();
        Tuple<String> tuple;

        // irrelevant tuples have no effect
        tuple = new Tuple<String>(new String[]{"puce", "isRedderThan", "khaki"});
        assertSolutions(tuple);
        tuple = new Tuple<String>(new String[]{"kazoo", "isKindOf", "instrument"});
        assertSolutions(tuple);

        // this contributes to a complete solution
        tuple = new Tuple<String>(new String[]{"red", "isRedderThan", "blue"});
        assertSolutions(tuple);

        // the other half of a complete solution
        tuple = new Tuple<String>(new String[]{"red", "htmlValue", "ff0000"});
        map.clear();
        map.put("x", "red");
        map.put("z", "blue");
        assertSolutions(tuple, map);
    }

    @Test
    public void testLength2Cycle() throws Exception {
        TuplePattern<String> pattern1 = new TuplePattern<String>(new Term[]{
                new Term(null, "x"), new Term("knows", null), new Term(null, "y")});
        TuplePattern<String> pattern2 = new TuplePattern<String>(new Term[]{
                new Term(null, "y"), new Term("knows", null), new Term(null, "x")});
        List<TuplePattern<String>> patterns = new LinkedList<TuplePattern<String>>();
        patterns.add(pattern1);
        patterns.add(pattern2);
        GraphPattern<String> graphPattern = new GraphPattern<String>(subscription1, patterns);
        queryIndex.add(graphPattern);

        HashMap<String, String> map1 = new HashMap<String, String>(), map2 = new HashMap<String, String>();
        Tuple<String> tuple;

        tuple = new Tuple<String>(new String[]{"arthur", "knows", "ford"});
        assertSolutions(tuple);

        tuple = new Tuple<String>(new String[]{"ford", "knows", "arthur"});
        map1.clear();
        map1.put("x", "arthur");
        map1.put("y", "ford");
        map2.clear();
        map2.put("y", "arthur");
        map2.put("x", "ford");
        assertSolutions(tuple, map1, map2);
    }

    @Test
    public void testLength3Cycle() throws Exception {
        TuplePattern<String> pattern1 = new TuplePattern<String>(new Term[]{
                new Term(null, "x"), new Term("knows", null), new Term(null, "y")});
        TuplePattern<String> pattern2 = new TuplePattern<String>(new Term[]{
                new Term(null, "y"), new Term("knows", null), new Term(null, "z")});
        TuplePattern<String> pattern3 = new TuplePattern<String>(new Term[]{
                new Term(null, "z"), new Term("knows", null), new Term(null, "x")});
        List<TuplePattern<String>> patterns = new LinkedList<TuplePattern<String>>();
        patterns.add(pattern1);
        patterns.add(pattern2);
        patterns.add(pattern3);
        GraphPattern<String> graphPattern = new GraphPattern<String>(subscription1, patterns);
        queryIndex.add(graphPattern);

        HashMap<String, String>
                map1 = new HashMap<String, String>(),
                map2 = new HashMap<String, String>(),
                map3 = new HashMap<String, String>();
        Tuple<String> tuple;

        tuple = new Tuple<String>(new String[]{"arthur", "knows", "ford"});
        assertSolutions(tuple);

        tuple = new Tuple<String>(new String[]{"ford", "knows", "zaphod"});
        assertSolutions(tuple);

        // add some potentially confusing tuples which do not form a solution
        tuple = new Tuple<String>(new String[]{"ford", "knows", "arthur"});
        assertSolutions(tuple);
        tuple = new Tuple<String>(new String[]{"zaphod", "knows", "trillian"});
        assertSolutions(tuple);
        tuple = new Tuple<String>(new String[]{"trillian", "knows", "marvin"});
        assertSolutions(tuple);
        tuple = new Tuple<String>(new String[]{"marvin", "knows", "marvin"});
        assertSolutions(tuple);
        tuple = new Tuple<String>(new String[]{"zaphod", "mocks", "arthur"});
        assertSolutions(tuple);

        tuple = new Tuple<String>(new String[]{"zaphod", "knows", "arthur"});
        map1.clear();
        map1.put("x", "arthur");
        map1.put("y", "ford");
        map1.put("z", "zaphod");
        map2.clear();
        map2.put("x", "ford");
        map2.put("y", "zaphod");
        map2.put("z", "arthur");
        map3.clear();
        map3.put("x", "zaphod");
        map3.put("y", "arthur");
        map3.put("z", "ford");
        assertSolutions(tuple, map1, map2, map3);
    }

    private <T> void assertSolutions(final Tuple<T> tuple,
                                     final Map<String, T>... expected) {
        final Map<Long, VariableBindings<T>> all = new HashMap<Long, VariableBindings<T>>();

        Map<Long, Integer> expectedCount = new HashMap<Long, Integer>();
        for (Map<String, T> m : expected) {
            VariableBindings<T> bindings = new VariableBindings<T>(m, vars);
            long hash = bindings.getHash();
            all.put(hash, bindings);
            Integer c = expectedCount.get(hash);
            if (null == c) {
                c = 0;
            }
            expectedCount.put(hash, c + 1);
        }

        final Map<Long, Integer> actualCount = new HashMap<Long, Integer>();
        QueryIndex.SolutionHandler<T> handler = new QueryIndex.SolutionHandler<T>() {
            @Override
            public void handle(Subscription subscription, VariableBindings<T> bindings) {
                long hash = bindings.getHash();
                all.put(hash, bindings);
                Integer c = actualCount.get(hash);
                if (null == c) {
                    c = 0;
                }
                actualCount.put(hash, c + 1);
            }
        };

        QueryIndex<T> index = (QueryIndex<T>) queryIndex;
        index.match(tuple, handler);

        for (Map.Entry<Long, VariableBindings<T>> e : all.entrySet()) {
            Integer act = actualCount.get(e.getKey());
            Integer exp = expectedCount.get(e.getKey());
            if (null == act) {
                act = 0;
            }
            if (null == exp) {
                exp = 0;
            }
            int cmp = exp.compareTo(act);
            if (cmp < 0) {
                fail("unexpected solution: " + e.getValue());
            } else if (cmp > 0) {
                fail("expected solution not produced: " + e.getValue());
            }
        }
    }
}
