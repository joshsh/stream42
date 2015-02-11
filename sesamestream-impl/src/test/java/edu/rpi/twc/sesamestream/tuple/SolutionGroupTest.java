package edu.rpi.twc.sesamestream.tuple;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionGroupTest {
    private long now;
    private long exp = 0;
    GraphPattern.QueryVariables vars;
    Map<String, String> bindingMap;
    VariableBindings<String> bindings;

    @Before
    public void setUp() throws Exception {
        now = System.currentTimeMillis();

        vars = new GraphPattern.QueryVariables(Arrays.asList("x", "y", "z"));

        bindingMap = new HashMap<String, String>();
        bindingMap.put("x", "red");
        bindingMap.put("y", "green");
        bindingMap.put("z", "blue");
        bindings = new VariableBindings<String>(bindingMap, vars);
    }

    @Test
    public void testAll() throws Exception {
        Solution<String> ps = new Solution<String>(3, 0, bindings, exp);

        SolutionGroup<String> g = new SolutionGroup<String>(bindings);
        assertEquals(0, g.getSolutions().length());

        g.add(ps, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(2, g.getSolutions().getValue().remainingPatterns);

        // duplicate solutions are ignored
        g.add(ps, now);
        assertEquals(1, g.getSolutions().length());

        // new solutions contains the old, and replaces it
        ps = new Solution<String>(ps, 1);
        g.add(ps, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(1, g.getSolutions().getValue().remainingPatterns);

        // new solution is disjoint with previous solutions, so is added
        ps = new Solution<String>(3, 2, bindings, exp);
        g.add(ps, now);
        assertEquals(2, g.getSolutions().length());
        assertEquals(2, g.getSolutions().getValue().remainingPatterns);
        assertEquals(4, g.getSolutions().getValue().matchedPatterns);

        // new solution replaces a previous solution and overlaps with another
        ps = new Solution<String>(ps, 1);
        g.add(ps, now);
        assertEquals(2, g.getSolutions().length());
        assertEquals(1, g.getSolutions().getValue().remainingPatterns);
        assertEquals(6, g.getSolutions().getValue().matchedPatterns);

        // new solution contains all previous solutions, and replaces both solutions in the list
        ps = new Solution<String>(ps, 0);
        g.add(ps, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(0, g.getSolutions().getValue().remainingPatterns);
        assertEquals(7, g.getSolutions().getValue().matchedPatterns);
    }

    @Test
    public void cantAddExpired() throws Exception {
        Solution<String> solExp1 = new Solution<String>(3, 0, bindings, 42);

        SolutionGroup<String> g = new SolutionGroup<String>(bindings);
        g.add(solExp1, now);
        assertTrue(g.getSolutions().isNil());
    }

    @Test
    public void testRemoveExpired() throws  Exception {
        now = 42;

        Solution<String> solExp1 = new Solution<String>(5, 0, bindings, now + 10);
        Solution<String> solExp2 = new Solution<String>(new Solution<String>(5, 1, bindings, now + 5), 2);
        Solution<String> solNonExp1 = new Solution<String>(new Solution<String>(5, 4, bindings, 0), 1);
        Solution<String> solNonExp2 = new Solution<String>(new Solution<String>(5, 3, bindings, 0), 1);
        // this solution inherits the short lifetime of its second component solution,
        // rather than the unlimited lifetime of the first
        Solution<String> solExp3 = new Solution<String>(5,
                new Solution<String>(5, 3, bindings, 0),
                new Solution<String>(5, 4, bindings, now + 20));

        SolutionGroup<String> g = new SolutionGroup<String>(bindings);
        g.add(solExp1, now);
        g.add(solExp2, now);
        g.add(solNonExp1, now);
        g.add(solNonExp2, now);
        g.add(solExp3, now);
        assertEquals(5, g.getSolutions().length());

        g.removeExpired(now);
        assertEquals(5, g.getSolutions().length());

        now += 6;
        g.removeExpired(now);
        assertEquals(4, g.getSolutions().length());

        now += 5;
        g.removeExpired(now);
        assertEquals(3, g.getSolutions().length());

        now += 10;
        g.removeExpired(now);
        assertEquals(2, g.getSolutions().length());
    }
}
