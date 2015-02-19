package edu.rpi.twc.sesamestream.tuple;

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
    private static int QUERY_TTL = 0;
    private long now;

    private Query.QueryVariables vars;
    private QueryIndex<String> queryIndex;

    @Before
    public void setUp() {
        now = System.currentTimeMillis();

        vars = new Query.QueryVariables(Arrays.asList("x", "y", "z"));
        int tupleSize = 3;
        queryIndex = new QueryIndex<String>(tupleSize);
    }

    @Test
    public void testSimpleJoin() throws Exception {
        Term<String>[] pattern1 = new Term[]{
                new Term(null, "x"), new Term("isRedderThan", null), new Term(null, "z")};
        Term<String>[] pattern2 = new Term[]{
                new Term(null, "x"), new Term("htmlValue", null), new Term("ff0000", null)};
        List<Term<String>[]> patterns = new LinkedList<Term<String>[]>();
        patterns.add(pattern1);
        patterns.add(pattern2);
        Query<String> query = new Query<String>(patterns, QUERY_TTL);
        queryIndex.add(query);

        HashMap<String, String> map = new HashMap<String, String>();
        String[] tuple;

        // irrelevant tuples have no effect
        tuple = new String[]{"puce", "isRedderThan", "khaki"};
        assertSolutions(now, vars, tuple);
        tuple = new String[]{"kazoo", "isKindOf", "instrument"};
        assertSolutions(now, vars, tuple);

        // this contributes to a complete solution
        tuple = new String[]{"red", "isRedderThan", "blue"};
        assertSolutions(now, vars, tuple);

        // the other half of a complete solution
        tuple = new String[]{"red", "htmlValue", "ff0000"};
        map.clear();
        map.put("x", "red");
        map.put("z", "blue");
        assertSolutions(now, vars, tuple, map);
    }

    @Test
    public void testLength2Cycle() throws Exception {
        Term<String>[] pattern1 = new Term[]{
                new Term(null, "x"), new Term("knows", null), new Term(null, "y")};
        Term<String>[] pattern2 = new Term[]{
                new Term(null, "y"), new Term("knows", null), new Term(null, "x")};
        List<Term<String>[]> patterns = new LinkedList<Term<String>[]>();
        patterns.add(pattern1);
        patterns.add(pattern2);
        Query<String> query = new Query<String>(patterns, QUERY_TTL);
        queryIndex.add(query);

        HashMap<String, String> map1 = new HashMap<String, String>(), map2 = new HashMap<String, String>();
        String[] tuple;

        tuple = new String[]{"arthur", "knows", "ford"};
        assertSolutions(now, vars, tuple);

        tuple = new String[]{"ford", "knows", "arthur"};
        map1.clear();
        map1.put("x", "arthur");
        map1.put("y", "ford");
        map2.clear();
        map2.put("y", "arthur");
        map2.put("x", "ford");
        assertSolutions(now, vars, tuple, map1, map2);
    }

    @Test
    public void testLength3Cycle() throws Exception {
        Term<String>[] pattern1 = new Term[]{
                new Term(null, "x"), new Term("knows", null), new Term(null, "y")};
        Term<String>[] pattern2 = new Term[]{
                new Term(null, "y"), new Term("knows", null), new Term(null, "z")};
        Term<String>[] pattern3 = new Term[]{
                new Term(null, "z"), new Term("knows", null), new Term(null, "x")};
        List<Term<String>[]> patterns = new LinkedList<Term<String>[]>();
        patterns.add(pattern1);
        patterns.add(pattern2);
        patterns.add(pattern3);
        Query<String> query = new Query<String>(patterns, QUERY_TTL);
        queryIndex.add(query);

        HashMap<String, String>
                map1 = new HashMap<String, String>(),
                map2 = new HashMap<String, String>(),
                map3 = new HashMap<String, String>();
        String[] tuple;

        tuple = new String[]{"arthur", "knows", "ford"};
        assertSolutions(now, vars, tuple);

        tuple = new String[]{"ford", "knows", "zaphod"};
        assertSolutions(now, vars, tuple);

        // add some potentially confusing tuples which do not form a solution
        tuple = new String[]{"ford", "knows", "arthur"};
        assertSolutions(now, vars, tuple);
        tuple = new String[]{"zaphod", "knows", "trillian"};
        assertSolutions(now, vars, tuple);
        tuple = new String[]{"trillian", "knows", "marvin"};
        assertSolutions(now, vars, tuple);
        tuple = new String[]{"zaphod", "mocks", "arthur"};
        assertSolutions(now, vars, tuple);

        // This tuple *does* produce a complete solution, because a partial solution from one "knows" pattern
        // is available when the next "knows" pattern is processed.
        // Here, the query happens to be symmetrical, but it undefined which pattern is processed first.
        tuple = new String[]{"marvin", "knows", "marvin"};
        map1.clear();
        map1.put("x", "marvin");
        map1.put("y", "marvin");
        map1.put("z", "marvin");
        assertSolutions(now, vars, tuple, map1);

        tuple = new String[]{"zaphod", "knows", "arthur"};
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
        assertSolutions(now, vars, tuple, map1, map2, map3);
    }

    @Test
    public void testRemoveExpired() throws Exception {
        Term<String>[] pattern1, pattern2, pattern3;
        List<Term<String>[]> patterns;
        Query<String> query1, query2, query3;
        String[] tuple;
        HashMap<String, String>
                map = new HashMap<String, String>();

        long now = 42;

        pattern1 = new Term[]{
                new Term(null, "x"), new Term("knows", null), new Term(null, "y")};
        pattern2 = new Term[]{
                new Term(null, "y"), new Term("knows", null), new Term(null, "z")};
        pattern3 = new Term[]{
                new Term(null, "z"), new Term("knows", null), new Term(null, "a")};
        patterns = new LinkedList<Term<String>[]>();
        patterns.add(pattern1);
        patterns.add(pattern2);
        patterns.add(pattern3);
        query1 = new Query<String>(patterns, 0);
        queryIndex.add(query1);

        pattern1 = new Term[]{
                new Term(null, "a"), new Term("name", null), new Term("\"Arthur Dent\"", null)};
        pattern2 = new Term[]{
                new Term(null, "a"), new Term("race", null), new Term("human", null)};
        patterns = new LinkedList<Term<String>[]>();
        patterns.add(pattern1);
        patterns.add(pattern2);
        query2 = new Query<String>(patterns, now + 1000L * 60);
        queryIndex.add(query2);

        pattern1 = new Term[]{
                new Term("Paris", null), new Term("capitalCityOf", null), new Term(null, "country")};
        pattern2 = new Term[]{
                new Term(null, "country"), new Term("name", null), new Term(null, "name")};
        pattern3 = new Term[]{
                new Term(null, "country"), new Term("population", null), new Term(null, "pop")};
        patterns = new LinkedList<Term<String>[]>();
        patterns.add(pattern1);
        patterns.add(pattern2);
        patterns.add(pattern3);
        query3 = new Query<String>(patterns, now + 1000L * 120);
        queryIndex.add(query3);

        queryIndex.removeExpired(now);

        tuple = new String[]{"arthur", "knows", "ford"};
        assertSolutions(now, query1.getVariables(), tuple);
        tuple = new String[]{"ford", "knows", "zaphod"};
        assertSolutions(now, query1.getVariables(), tuple);
        tuple = new String[]{"zaphod", "knows", "trillian"};
        map.clear();
        map.put("x", "arthur");
        map.put("y", "ford");
        map.put("z", "zaphod");
        map.put("a", "trillian");
        assertSolutions(now, query1.getVariables(), tuple, map);

        tuple = new String[]{"arthur", "name", "\"Arthur Dent\""};
        assertSolutions(now, query2.getVariables(), tuple);
        tuple = new String[]{"arthur", "race", "human"};
        map.clear();
        map.put("a", "arthur");
        assertSolutions(now, query2.getVariables(), tuple, map);

        tuple = new String[]{"Paris", "capitalCityOf", "France"};
        assertSolutions(now, query3.getVariables(), tuple);
        tuple = new String[]{"France", "name", "\"France\""};
        assertSolutions(now, query3.getVariables(), tuple);
        tuple = new String[]{"France", "population", "dozens of millions"};
        map.clear();
        map.put("country", "France");
        map.put("name", "\"France\"");
        map.put("pop", "dozens of millions");
        assertSolutions(now, query3.getVariables(), tuple, map);

        now += 1000L * 61;
        queryIndex.removeExpired(now);

        // insert a new, unique tuple to trigger a solution
        tuple = new String[]{"zaphod", "knows", "marvin"};
        map.clear();
        map.put("x", "arthur");
        map.put("y", "ford");
        map.put("z", "zaphod");
        map.put("a", "marvin");
        assertSolutions(now, query1.getVariables(), tuple, map);

        // this pattern has gone
        tuple = new String[]{"arthur", "name", "\"Arthur\""};
        assertSolutions(now, query2.getVariables(), tuple);
        tuple = new String[]{"arthur", "race", "ape-descendant"};
        assertSolutions(now, query2.getVariables(), tuple);

        tuple = new String[]{"France", "population", "tens of millions"};
        map.clear();
        map.put("country", "France");
        map.put("name", "\"France\"");
        map.put("pop", "tens of millions");
        assertSolutions(now, query3.getVariables(), tuple, map);

        now += 1000L * 61;
        queryIndex.removeExpired(now);

        tuple = new String[]{"zaphod", "knows", "eddie"};
        map.clear();
        map.put("x", "arthur");
        map.put("y", "ford");
        map.put("z", "zaphod");
        map.put("a", "eddie");
        assertSolutions(now, query1.getVariables(), tuple, map);

        // now this pattern is gone, as well
        tuple = new String[]{"France", "population", "zillions of millions"};
        assertSolutions(now, query3.getVariables(), tuple);
    }

    // note: assumes infinite ttl
    private <T> void assertSolutions(final long now,
                                     final Query.QueryVariables vars,
                                     final T[] tuple,
                                     final Map<String, T>... expected) {
        int ttl = 0;

        final Map<Long, Bindings<T>> all = new HashMap<Long, Bindings<T>>();

        Map<Long, Integer> expectedCount = new HashMap<Long, Integer>();
        for (Map<String, T> m : expected) {
            Bindings<T> bindings = new Bindings<T>(m, vars);
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
            public void handle(String id, Bindings<T> bindings) {
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
        index.add(tuple, handler, ttl, now);

        for (Map.Entry<Long, Bindings<T>> e : all.entrySet()) {
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
