package edu.rpi.twc.sesamestream.tuple;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionGroupTest {
    @Test
    public void testAll() throws Exception {
        GraphPattern.QueryVariables vars
                = new GraphPattern.QueryVariables(Arrays.asList("x", "y", "z"));

        Map<String, String> bindings = new HashMap<String, String>();
        bindings.put("x", "red");
        bindings.put("y", "green");
        bindings.put("z", "blue");
        VariableBindings<String> b = new VariableBindings<String>(bindings, vars);

        Solution<String> ps = new Solution<String>(3, 0, b);

        SolutionGroup<String> g = new SolutionGroup<String>(b);
        assertEquals(0, g.getSolutions().length());

        g.add(ps);
        assertEquals(1, g.getSolutions().length());
        assertEquals(2, g.getSolutions().getValue().remainingPatterns);

        // duplicate solutions are ignored
        g.add(ps);
        assertEquals(1, g.getSolutions().length());

        // new solutions contains the old, and replaces it
        ps = new Solution<String>(ps, 1);
        g.add(ps);
        assertEquals(1, g.getSolutions().length());
        assertEquals(1, g.getSolutions().getValue().remainingPatterns);

        // new solution is disjoint with previous solutions, so is added
        ps = new Solution<String>(3, 2, b);
        g.add(ps);
        assertEquals(2, g.getSolutions().length());
        assertEquals(2, g.getSolutions().getValue().remainingPatterns);
        assertEquals(4, g.getSolutions().getValue().matchedPatterns);

        // new solution replaces a previous solution and overlaps with another
        ps = new Solution<String>(ps, 1);
        g.add(ps);
        assertEquals(2, g.getSolutions().length());
        assertEquals(1, g.getSolutions().getValue().remainingPatterns);
        assertEquals(6, g.getSolutions().getValue().matchedPatterns);

        // new solution contains all previous solutions, and replaces both solutions in the list
        ps = new Solution<String>(ps, 0);
        g.add(ps);
        assertEquals(1, g.getSolutions().length());
        assertEquals(0, g.getSolutions().getValue().remainingPatterns);
        assertEquals(7, g.getSolutions().getValue().matchedPatterns);
    }
}
