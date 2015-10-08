package edu.rpi.twc.sesamestream.core;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionIndexTest {
    private long now;
    private long exp = 0;

    private Query.QueryVariables vars;
    private SolutionIndex<String> index;

    @Before
    public void setUp() throws Exception {
        now = System.currentTimeMillis();

        vars = new Query.QueryVariables(Arrays.asList("x", "y", "z"));

        int totalPatterns = 3;
        index = new SolutionIndex<String>(vars, totalPatterns);
    }

    @Test
    public void testGetSolutions() throws Exception {
        Solution<String> ps;
        HashMap<String, String> map1;
        Bindings<String> b1;

        assertEquals(0, count(index.getSolutions("x", "red", now)));

        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        map1.put("y", "green");
        b1 = new Bindings<String>(map1, vars);
        ps = new Solution<String>(3, 0, b1, exp);
        index.add(ps, now);
        assertEquals(1, count(index.getSolutions("x", "red", now)));
        assertEquals(0, count(index.getSolutions("x", "green", now)));
        assertEquals(0, count(index.getSolutions("y", "red", now)));
        assertEquals(1, count(index.getSolutions("y", "green", now)));

        // new solution with new binding
        map1 = new HashMap<String, String>();
        map1.put("z", "blue");
        b1 = new Bindings<String>(map1, vars);
        ps = new Solution<String>(ps, 1, b1);
        index.add(ps, now);
        assertEquals(2, count(index.getSolutions("x", "red", now)));
        assertEquals(2, count(index.getSolutions("y", "green", now)));
        // only one solution so far contains this binding
        assertEquals(1, count(index.getSolutions("z", "blue", now)));

        // new solution with redundant bindings
        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        map1.put("y", "green");
        b1 = new Bindings<String>(map1, vars);
        ps = new Solution<String>(3, 1, b1, exp);
        index.add(ps, now);
        assertEquals(3, count(index.getSolutions("x", "red", now)));
        assertEquals(3, count(index.getSolutions("y", "green", now)));

        // new solution which replaces two existing solutions with the same bindings
        ps = new Solution<String>(ps, 0);
        index.add(ps, now);
        assertEquals(2, count(index.getSolutions("x", "red", now)));
        assertEquals(2, count(index.getSolutions("y", "green", now)));

        // new bindings with patterns and variables identical to previous solutions
        map1 = new HashMap<String, String>();
        map1.put("x", "puce");
        map1.put("y", "green");
        b1 = new Bindings<String>(map1, vars);
        ps = new Solution<String>(3, 0, b1, exp);
        index.add(ps, now);
        assertEquals(2, count(index.getSolutions("x", "red", now)));
        assertEquals(1, count(index.getSolutions("x", "puce", now)));
        assertEquals(3, count(index.getSolutions("y", "green", now)));
    }

    // test that expired solutions are not retrieved, regardless of whether they are removed
    @Test
    public void testGetExpiredSolutions() throws Exception {
        Map<String, String> bindingMap = new HashMap<String, String>();
        bindingMap.put("x", "red");
        bindingMap.put("y", "green");
        bindingMap.put("z", "blue");
        Bindings<String> bindings1 = new Bindings<String>(bindingMap, vars);

        bindingMap = new HashMap<String, String>();
        bindingMap.put("x", "puce");
        bindingMap.put("y", "teal");
        Bindings<String> bindings2 = new Bindings<String>(bindingMap, vars);

        now = 42;

        Solution<String> solExp1_1 = new Solution<String>(5, 0, bindings1, now + 1000L * 10);
        Solution<String> solExp1_2 = new Solution<String>(new Solution<String>(5, 1, bindings1, now + 1000L * 5), 2);
        Solution<String> solNonExp1_1 = new Solution<String>(new Solution<String>(5, 4, bindings1, 0), 1);
        Solution<String> solNonExp1_2 = new Solution<String>(new Solution<String>(5, 3, bindings1, 0), 1);
        Solution<String> solExp1_3 = new Solution<String>(5,
                new Solution<String>(5, 3, bindings1, 0),
                new Solution<String>(5, 4, bindings1, now + 1000L * 20));

        Solution<String> solExp2_1 = new Solution<String>(5, 0, bindings2, now + 1000L * 10);
        Solution<String> solExp2_2 = new Solution<String>(new Solution<String>(5, 1, bindings2, now + 1000L * 5), 2);
        Solution<String> solExp2_3 = new Solution<String>(5,
                new Solution<String>(5, 3, bindings2, 0),
                new Solution<String>(5, 4, bindings2, now + 1000L * 20));

        index.add(solExp1_1, now);
        index.add(solExp1_2, now);
        index.add(solNonExp1_1, now);
        index.add(solNonExp1_2, now);
        index.add(solExp1_3, now);

        index.add(solExp2_1, now);
        index.add(solExp2_2, now);
        index.add(solExp2_3, now);

        // timestamp at which solutions are retrieved
        long then = now;

        assertEquals(5, count(index.getSolutions("x", "red", then)));
        assertEquals(5, count(index.getSolutions("y", "green", then)));
        assertEquals(5, count(index.getSolutions("z", "blue", then)));
        assertEquals(3, count(index.getSolutions("x", "puce", then)));
        assertEquals(3, count(index.getSolutions("y", "teal", then)));

        then = now + 5 * 1000L + 1;

        assertEquals(4, count(index.getSolutions("x", "red", then)));
        assertEquals(4, count(index.getSolutions("y", "green", then)));
        assertEquals(4, count(index.getSolutions("z", "blue", then)));
        assertEquals(2, count(index.getSolutions("x", "puce", then)));
        assertEquals(2, count(index.getSolutions("y", "teal", then)));

        then = now + 10 * 1000L + 1;

        assertEquals(3, count(index.getSolutions("x", "red", then)));
        assertEquals(3, count(index.getSolutions("y", "green", then)));
        assertEquals(3, count(index.getSolutions("z", "blue", then)));
        assertEquals(1, count(index.getSolutions("x", "puce", then)));
        assertEquals(1, count(index.getSolutions("y", "teal", then)));

        then = now + 20 * 1000L + 1;

        assertEquals(2, count(index.getSolutions("x", "red", then)));
        assertEquals(2, count(index.getSolutions("y", "green", then)));
        assertEquals(2, count(index.getSolutions("z", "blue", then)));
        assertEquals(0, count(index.getSolutions("x", "puce", then)));
        assertEquals(0, count(index.getSolutions("y", "teal", then)));
    }

    @Test
    public void testGetComplementarySolutions() throws Exception {
        HashMap<String, String> map1, map2;
        Bindings<String> b1, b2;
        Solution<String> ps1, ps2;

        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        map1.put("z", "blue");
        b1 = new Bindings<String>(map1, vars);
        ps1 = new Solution<String>(3, 0, b1, exp);

        assertEquals(0, count(index.getComposableSolutions("x", "red", ps1, now)));

        // this has no effect on x:red
        map2 = new HashMap<String, String>();
        map2.put("x", "puce");
        map2.put("y", "green");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 1, b2, exp);
        index.add(ps2, now);
        assertEquals(0, count(index.getComposableSolutions("x", "red", ps1, now)));

        // this adds a solution which binds to x:red, but it conflicts with ps1 in z
        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("z", "green");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 1, b2, exp);
        index.add(ps2, now);
        assertEquals(0, count(index.getComposableSolutions("x", "red", ps1, now)));

        // this adds a solution which is compatible in variables, but overlaps in patterns
        map2 = new HashMap<String, String>();
        map2.put("y", "green");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(ps1, 2, b2);
        index.add(ps2, now);
        assertEquals(0, count(index.getComposableSolutions("x", "red", ps1, now)));

        // now we have a solution which does not conflict, and can be joined with the original solution
        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("y", "green");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2, exp);
        index.add(ps2, now);
        assertEquals(1, count(index.getComposableSolutions("x", "red", ps1, now)));
    }

    @Test
    public void testComposeSolutions() throws Exception {
        HashMap<String, String> map1, map2;
        Bindings<String> b1, b2;
        Solution<String> ps1, ps2;

        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        map1.put("z", "blue");
        b1 = new Bindings<String>(map1, vars);
        ps1 = new Solution<String>(3, 0, b1, exp);

        // add an incompatible solution
        map2 = new HashMap<String, String>();
        map2.put("y", "green");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(ps1, 2, b2);
        index.add(ps2, now);
        assertEquals(0, count(index.composeSolutions("x", "red", ps1, now)));

        // add two compatible solutions.  Both appear in the composed iterator.
        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("y", "green");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2, exp);
        index.add(ps2, now);
        assertEquals(1, count(index.composeSolutions("x", "red", ps1, now)));
        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("y", "hazel");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2, exp);
        index.add(ps2, now);
        assertEquals(2, count(index.composeSolutions("x", "red", ps1, now)));
    }

    @Test
    public void testBindAndComposeSolutions() throws Exception {
        HashMap<String, String> map1, map2;
        Bindings<String> b1, b2;
        Solution<String> ps1, ps2;
        Stack<Solution<String>> solutions = new Stack<Solution<String>>();

        Term<String>[] pattern = new Term[]{
                new Term(null, "x"), new Term("isRedderThan", null), new Term(null, "z")};
        String[] tuple = new String[]{"red", "isRedderThan", "blue"};
        Bindings<String> b = vars.bind(pattern, tuple);
        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        map1.put("z", "blue");
        b1 = new Bindings<String>(map1, vars);
        ps1 = new Solution<String>(3, 0, b1, exp);

        solutions.clear();
        index.joinSolutions(ps1, b, solutions, now);
        assertEquals(1, solutions.size());

        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("y", "green");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2, exp);
        index.add(ps2, now);

        solutions.clear();
        index.joinSolutions(ps1, b, solutions, now);
        assertEquals(2, solutions.size());

        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("y", "hazel");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2, exp);
        index.add(ps2, now);

        solutions.clear();
        index.joinSolutions(ps1, b, solutions, now);
        assertEquals(3, solutions.size());

        // add an irrelevant solution
        map2 = new HashMap<String, String>();
        map2.put("y", "green");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2, exp);
        index.add(ps2, now);

        solutions.clear();
        index.joinSolutions(ps1, b, solutions, now);
        assertEquals(3, solutions.size());

        // add a solution which is reached through recursion.
        // It will be composed with a previously added solution to produce yet another solution.
        map2 = new HashMap<String, String>();
        map2.put("y", "green");
        map2.put("z", "blue");
        b2 = new Bindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 1, b2, exp);
        index.add(ps2, now);

        solutions.clear();
        index.joinSolutions(ps1, b, solutions, now);
        assertEquals(5, solutions.size());
        Map<Integer, Solution<String>> solutionMap = toMap(solutions);
        // solutions are not unique
        assertEquals(3, solutionMap.size());
        // joined solution is {x:red,y:green,z:blue}
        map1.clear();
        map1.put("x", "red");
        map1.put("y", "green");
        map1.put("z", "blue");
        b1 = new Bindings<String>(map1, vars);
        Solution<String> ps = solutionMap.get(b1.getHash());
        assertNotNull(ps);
        assertEquals(1, ps.remainingPatterns);
        //assertEquals(3, ps.matchedPatterns);
    }

    @Test
    public void testRemoveExpiredSolutions() throws Exception {
        Map<String, String> bindingMap = new HashMap<String, String>();
        bindingMap.put("x", "red");
        bindingMap.put("y", "green");
        bindingMap.put("z", "blue");
        Bindings<String> bindings1 = new Bindings<String>(bindingMap, vars);

        bindingMap = new HashMap<String, String>();
        bindingMap.put("x", "puce");
        bindingMap.put("y", "teal");
        Bindings<String> bindings2 = new Bindings<String>(bindingMap, vars);

        now = 42;

        // retrieve solutions with respect to a timestamp at the beginning of time,
        // so that expired solutions are not filtered out by the retrieval operation;
        // they are actually gone from the index.
        long then = 1;

        Solution<String> solExp1_1 = new Solution<String>(5, 0, bindings1, now + 1000L * 10);
        Solution<String> solExp1_2 = new Solution<String>(new Solution<String>(5, 1, bindings1, now + 1000L * 5), 2);
        Solution<String> solNonExp1_1 = new Solution<String>(new Solution<String>(5, 4, bindings1, 0), 1);
        Solution<String> solNonExp1_2 = new Solution<String>(new Solution<String>(5, 3, bindings1, 0), 1);
        Solution<String> solExp1_3 = new Solution<String>(5,
                new Solution<String>(5, 3, bindings1, 0),
                new Solution<String>(5, 4, bindings1, now + 1000L * 20));

        Solution<String> solExp2_1 = new Solution<String>(5, 0, bindings2, now + 1000L * 10);
        Solution<String> solExp2_2 = new Solution<String>(new Solution<String>(5, 1, bindings2, now + 1000L * 5), 2);
        Solution<String> solExp2_3 = new Solution<String>(5,
                new Solution<String>(5, 3, bindings2, 0),
                new Solution<String>(5, 4, bindings2, now + 1000L * 20));

        index.add(solExp1_1, now);
        index.add(solExp1_2, now);
        index.add(solNonExp1_1, now);
        index.add(solNonExp1_2, now);
        index.add(solExp1_3, now);

        index.add(solExp2_1, now);
        index.add(solExp2_2, now);
        index.add(solExp2_3, now);

        assertEquals(5, count(index.getSolutions("x", "red", then)));
        assertEquals(5, count(index.getSolutions("y", "green", then)));
        assertEquals(5, count(index.getSolutions("z", "blue", then)));
        assertEquals(3, count(index.getSolutions("x", "puce", then)));
        assertEquals(3, count(index.getSolutions("y", "teal", then)));

        index.removeExpired(now);

        assertEquals(5, count(index.getSolutions("x", "red", then)));
        assertEquals(5, count(index.getSolutions("y", "green", then)));
        assertEquals(5, count(index.getSolutions("z", "blue", then)));
        assertEquals(3, count(index.getSolutions("x", "puce", then)));
        assertEquals(3, count(index.getSolutions("y", "teal", then)));

        now += 1000L * 6;
        index.removeExpired(now);

        assertEquals(4, count(index.getSolutions("x", "red", then)));
        assertEquals(4, count(index.getSolutions("y", "green", then)));
        assertEquals(4, count(index.getSolutions("z", "blue", then)));
        assertEquals(2, count(index.getSolutions("x", "puce", then)));
        assertEquals(2, count(index.getSolutions("y", "teal", then)));

        now += 1000L * 5;
        index.removeExpired(now);

        assertEquals(3, count(index.getSolutions("x", "red", then)));
        assertEquals(3, count(index.getSolutions("y", "green", then)));
        assertEquals(3, count(index.getSolutions("z", "blue", then)));
        assertEquals(1, count(index.getSolutions("x", "puce", then)));
        assertEquals(1, count(index.getSolutions("y", "teal", then)));

        now += 1000L * 10;
        index.removeExpired(now);

        assertEquals(2, count(index.getSolutions("x", "red", then)));
        assertEquals(2, count(index.getSolutions("y", "green", then)));
        assertEquals(2, count(index.getSolutions("z", "blue", then)));
        assertNull(index.getSolutions("x", "puce", then));
        assertNull(index.getSolutions("y", "teal", then));
    }

    private <T> int count(final Iterator<T> iter) {
        int c = 0;
        if (null != iter) {
            while (iter.hasNext()) {
                c++;
                iter.next();
            }
        }
        return c;
    }

    // note: disregards duplicate solutions
    private <T> Map<Integer, Solution<T>> toMap(final Stack<Solution<T>> solutions) {
        Map<Integer, Solution<T>> map = new HashMap<Integer, Solution<T>>();

        for (Solution<T> ps : solutions) {
            int hash = ps.getBindings().getHash();
            map.put(hash, ps);
        }

        return map;
    }
}
