package edu.rpi.twc.sesamestream.tuple;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionIndexTest {
    @Test
    public void testGetSolutions() throws Exception {
        Solution<String> ps;
        GraphPattern.QueryVariables vars
                = new GraphPattern.QueryVariables(Arrays.asList("x", "y", "z"));
        int totalPatterns = 3;
        HashMap<String, String> map1;
        VariableBindings<String> b1;

        SolutionIndex<String> bindex = new SolutionIndex<String>(vars, totalPatterns);
        assertEquals(0, count(bindex.getSolutions("x", "red")));

        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        map1.put("y", "green");
        b1 = new VariableBindings<String>(map1, vars);
        ps = new Solution<String>(3, 0, b1);
        bindex.add(ps);
        assertEquals(1, count(bindex.getSolutions("x", "red")));
        assertEquals(0, count(bindex.getSolutions("x", "green")));
        assertEquals(0, count(bindex.getSolutions("y", "red")));
        assertEquals(1, count(bindex.getSolutions("y", "green")));

        // new solution with new binding
        map1 = new HashMap<String, String>();
        map1.put("z", "blue");
        b1 = new VariableBindings<String>(map1, vars);
        ps = new Solution<String>(ps, 1, b1);
        bindex.add(ps);
        assertEquals(2, count(bindex.getSolutions("x", "red")));
        assertEquals(2, count(bindex.getSolutions("y", "green")));
        // only one solution so far contains this binding
        assertEquals(1, count(bindex.getSolutions("z", "blue")));

        // new solution with redundant bindings
        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        map1.put("y", "green");
        b1 = new VariableBindings<String>(map1, vars);
        ps = new Solution<String>(3, 1, b1);
        bindex.add(ps);
        assertEquals(3, count(bindex.getSolutions("x", "red")));
        assertEquals(3, count(bindex.getSolutions("y", "green")));

        // new solution which replaces two existing solutions with the same bindings
        ps = new Solution<String>(ps, 0);
        bindex.add(ps);
        assertEquals(2, count(bindex.getSolutions("x", "red")));
        assertEquals(2, count(bindex.getSolutions("y", "green")));

        // new bindings with patterns and variables identical to previous solutions
        map1 = new HashMap<String, String>();
        map1.put("x", "puce");
        map1.put("y", "green");
        b1 = new VariableBindings<String>(map1, vars);
        ps = new Solution<String>(3, 0, b1);
        bindex.add(ps);
        assertEquals(2, count(bindex.getSolutions("x", "red")));
        assertEquals(1, count(bindex.getSolutions("x", "puce")));
        assertEquals(3, count(bindex.getSolutions("y", "green")));
    }

    @Test
    public void testGetComplementarySolutions() throws Exception {
        GraphPattern.QueryVariables vars
                = new GraphPattern.QueryVariables(Arrays.asList("x", "y", "z"));
        int totalPatterns = 3;
        HashMap<String, String> map1, map2;
        VariableBindings<String> b1, b2;
        Solution<String> ps1, ps2;

        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        map1.put("z", "blue");
        b1 = new VariableBindings<String>(map1, vars);
        ps1 = new Solution<String>(3, 0, b1);

        SolutionIndex<String> bindex = new SolutionIndex<String>(vars, totalPatterns);
        assertEquals(0, count(bindex.getComplementarySolutions("x", "red", ps1)));

        // this has no effect on x:red
        map2 = new HashMap<String, String>();
        map2.put("x", "puce");
        map2.put("y", "green");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 1, b2);
        bindex.add(ps2);
        assertEquals(0, count(bindex.getComplementarySolutions("x", "red", ps1)));

        // this adds a solution which binds to x:red, but it conflicts with ps1 in z
        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("z", "green");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 1, b2);
        bindex.add(ps2);
        assertEquals(0, count(bindex.getComplementarySolutions("x", "red", ps1)));

        // this adds a solution which is compatible in variables, but overlaps in patterns
        map2 = new HashMap<String, String>();
        map2.put("y", "green");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(ps1, 2, b2);
        bindex.add(ps2);
        assertEquals(0, count(bindex.getComplementarySolutions("x", "red", ps1)));

        // now we have a solution which does not conflict, and can be joined with the original solution
        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("y", "green");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2);
        bindex.add(ps2);
        assertEquals(1, count(bindex.getComplementarySolutions("x", "red", ps1)));
    }

    @Test
    public void testComposeSolutions() throws Exception {
        GraphPattern.QueryVariables vars
                = new GraphPattern.QueryVariables(Arrays.asList("x", "y", "z"));
        int totalPatterns = 3;
        HashMap<String, String> map1, map2;
        VariableBindings<String> b1, b2;
        Solution<String> ps1, ps2;
        SolutionIndex<String> bindex = new SolutionIndex<String>(vars, totalPatterns);

        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        map1.put("z", "blue");
        b1 = new VariableBindings<String>(map1, vars);
        ps1 = new Solution<String>(3, 0, b1);

        // add an incompatible solution
        map2 = new HashMap<String, String>();
        map2.put("y", "green");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(ps1, 2, b2);
        bindex.add(ps2);
        assertEquals(0, count(bindex.composeSolutions("x", "red", ps1)));

        // add two compatible solutions.  Both appear in the composed iterator.
        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("y", "green");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2);
        bindex.add(ps2);
        assertEquals(1, count(bindex.composeSolutions("x", "red", ps1)));
        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("y", "hazel");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2);
        bindex.add(ps2);
        assertEquals(2, count(bindex.composeSolutions("x", "red", ps1)));
    }

    @Test
    public void testBindAndComposeSolutions() throws Exception {
        GraphPattern.QueryVariables vars
                = new GraphPattern.QueryVariables(Arrays.asList("x", "y", "z"));
        int totalPatterns = 3;
        HashMap<String, String> map1, map2;
        VariableBindings<String> b1, b2;
        Solution<String> ps1, ps2;
        SolutionIndex<String> bindex = new SolutionIndex<String>(vars, totalPatterns);
        Stack<Solution<String>> solutions = new Stack<Solution<String>>();

        TuplePattern<String> pattern = new TuplePattern<String>(new Term[]{
                new Term(null, "x"), new Term("isRedderThan", null), new Term(null, "z")});
        Tuple<String> tuple = new Tuple<String>(new String[]{"red", "isRedderThan", "blue"});

        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        map1.put("z", "blue");
        b1 = new VariableBindings<String>(map1, vars);
        ps1 = new Solution<String>(3, 0, b1);

        solutions.clear();
        bindex.bindAndSolve(ps1, pattern, tuple, 0, 3, solutions);
        assertEquals(0, solutions.size());

        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("y", "green");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2);
        bindex.add(ps2);

        solutions.clear();
        bindex.bindAndSolve(ps1, pattern, tuple, 0, 3, solutions);
        assertEquals(1, solutions.size());

        map2 = new HashMap<String, String>();
        map2.put("x", "red");
        map2.put("y", "hazel");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2);
        bindex.add(ps2);

        solutions.clear();
        bindex.bindAndSolve(ps1, pattern, tuple, 0, 3, solutions);
        assertEquals(2, solutions.size());

        // add an irrelevant solution
        map2 = new HashMap<String, String>();
        map2.put("y", "green");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 2, b2);
        bindex.add(ps2);

        solutions.clear();
        bindex.bindAndSolve(ps1, pattern, tuple, 0, 3, solutions);
        assertEquals(2, solutions.size());

        // add a solution which is reached through recursion.
        // It will be composed with a previously added solution to produce yet another solution.
        map2 = new HashMap<String, String>();
        map2.put("y", "green");
        map2.put("z", "blue");
        b2 = new VariableBindings<String>(map2, vars);
        ps2 = new Solution<String>(3, 1, b2);
        bindex.add(ps2);

        solutions.clear();
        bindex.bindAndSolve(ps1, pattern, tuple, 0, 3, solutions);
        assertEquals(4, solutions.size());
        Map<Long, Solution<String>> solutionMap = toMap(solutions);
        // solutions are unique
        assertEquals(4, solutionMap.size());
        // joined solution is {x:red,y:green,z:blue}
        map1.clear();
        map1.put("x", "red");
        map1.put("y", "green");
        map1.put("z", "blue");
        b1 = new VariableBindings<String>(map1, vars);
        Solution<String> ps = solutionMap.get(b1.getHash());
        assertNotNull(ps);
        assertEquals(1, ps.getRemainingPatterns());
        assertEquals(6, ps.matchedPatterns);
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
    private <T> Map<Long, Solution<T>> toMap(final Stack<Solution<T>> solutions) {
        Map<Long, Solution<T>> map = new HashMap<Long, Solution<T>>();

        for (Solution<T> ps : solutions) {
            long hash = ps.getBindings().getHash();
            map.put(hash, ps);
        }

        return map;
    }
}
