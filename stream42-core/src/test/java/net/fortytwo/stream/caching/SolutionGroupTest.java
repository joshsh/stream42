package net.fortytwo.stream.caching;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionGroupTest {
    private long now;
    Query.QueryVariables vars;
    Map<String, String> bindingMap;
    Bindings<String> bindings;

    @Before
    public void setUp() throws Exception {
        now = System.currentTimeMillis();

        vars = new Query.QueryVariables(Arrays.asList("x", "y", "z"));

        bindingMap = new HashMap<>();
        bindingMap.put("x", "red");
        bindingMap.put("y", "green");
        bindingMap.put("z", "blue");
        bindings = new Bindings<>(bindingMap, vars);
    }

    @Test
    public void testAll() throws Exception {
        long exp = 0;
        Solution<String> ps = new Solution<>(3, 0, bindings, exp);

        SolutionGroup<String> g = new SolutionGroup<>(bindings);
        assertEquals(0, g.getSolutions().length());

        g.add(ps, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(2, g.getSolutions().getValue().remainingPatterns);

        // duplicate solutions are ignored
        g.add(ps, now);
        assertEquals(1, g.getSolutions().length());

        // new solutions contains the old, and replaces it
        ps = new Solution<>(ps, 1);
        g.add(ps, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(1, g.getSolutions().getValue().remainingPatterns);

        // new solution is disjoint with previous solutions, so is added
        ps = new Solution<>(3, 2, bindings, exp);
        g.add(ps, now);
        assertEquals(2, g.getSolutions().length());
        assertEquals(2, g.getSolutions().getValue().remainingPatterns);
        assertEquals(4, g.getSolutions().getValue().matchedPatterns);

        // new solution replaces a previous solution and overlaps with another
        ps = new Solution<>(ps, 1);
        g.add(ps, now);
        assertEquals(2, g.getSolutions().length());
        assertEquals(1, g.getSolutions().getValue().remainingPatterns);
        assertEquals(6, g.getSolutions().getValue().matchedPatterns);

        // new solution contains all previous solutions, and replaces both solutions in the list
        ps = new Solution<>(ps, 0);
        g.add(ps, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(0, g.getSolutions().getValue().remainingPatterns);
        assertEquals(7, g.getSolutions().getValue().matchedPatterns);
    }

    @Test(expected = IllegalStateException.class)
    public void testCantAddExpired() throws Exception {
        Solution<String> solExp1 = new Solution<>(3, 0, bindings, 42);

        SolutionGroup<String> g = new SolutionGroup<>(bindings);
        g.add(solExp1, now);
    }

    @Test
    public void testRemoveExpired() throws Exception {
        now = 42;

        Solution<String> solExp1 = new Solution<>(5, 0, bindings, now + 1000L * 10);
        Solution<String> solExp2 = new Solution<>(new Solution<>(5, 1, bindings, now + 1000L * 5), 2);
        Solution<String> solNonExp1 = new Solution<>(new Solution<>(5, 4, bindings, 0), 1);
        Solution<String> solNonExp2 = new Solution<>(new Solution<>(5, 3, bindings, 0), 1);
        // this solution inherits the short lifetime of its second component solution,
        // rather than the unlimited lifetime of the first
        Solution<String> solExp3 = new Solution<>(5,
                new Solution<>(5, 3, bindings, 0),
                new Solution<>(5, 4, bindings, now + 1000L * 20));

        SolutionGroup<String> g = new SolutionGroup<>(bindings);
        g.add(solExp1, now);
        g.add(solExp2, now);
        g.add(solNonExp1, now);
        g.add(solNonExp2, now);
        g.add(solExp3, now);
        assertEquals(5, g.getSolutions().length());

        g.removeExpired(now);
        assertEquals(5, g.getSolutions().length());

        now += 1000L * 6;
        g.removeExpired(now);
        assertEquals(4, g.getSolutions().length());

        now += 1000L * 5;
        g.removeExpired(now);
        assertEquals(3, g.getSolutions().length());

        now += 1000L * 10;
        g.removeExpired(now);
        assertEquals(2, g.getSolutions().length());
    }

    @Test
    public void testExpirationLogicOnInsertion() throws Exception {
        SolutionGroup<String> g = new SolutionGroup<>(bindings);
        Solution<String> sol1;

        now = 42;

        sol1 = new Solution<>(new Solution<>(5, 0, bindings, now + 1000L * 10), 1);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 10, g.getSolutions().getValue().expirationTime);

        // adding an identical solution has no effect
        sol1 = new Solution<>(new Solution<>(5, 0, bindings, now + 1000L * 10), 1);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 10, g.getSolutions().getValue().expirationTime);

        // adding an equal solution with a lesser expiration time also has no effect
        sol1 = new Solution<>(new Solution<>(5, 0, bindings, now + 1000L * 5), 1);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 10, g.getSolutions().getValue().expirationTime);

        // adding an equal solution with a greater expiration time causes the time to be replaced
        sol1 = new Solution<>(new Solution<>(5, 0, bindings, now + 1000L * 20), 1);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 20, g.getSolutions().getValue().expirationTime);

        // likewise for infinite expiration time
        sol1 = new Solution<>(new Solution<>(5, 0, bindings, 0), 1);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(0, g.getSolutions().getValue().expirationTime);

        // adding a solution contained within an existing solution has no effect if expiration is equal
        sol1 = new Solution<>(5, 0, bindings, 0);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(0, g.getSolutions().getValue().expirationTime);

        // likewise if the expiration of the contained solution is lower
        sol1 = new Solution<>(5, 0, bindings, now + 1000L * 10);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(0, g.getSolutions().getValue().expirationTime);

        // if the expiration of the contained solution is higher, it must be added as a separate solution
        g = new SolutionGroup<>(bindings);
        sol1 = new Solution<>(new Solution<>(5, 0, bindings, now + 1000L * 10), 1);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 10, g.getSolutions().getValue().expirationTime);
        sol1 = new Solution<>(5, 0, bindings, now + 1000L * 20);
        g.add(sol1, now);
        assertEquals(2, g.getSolutions().length());
        assertEquals(4, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 20, g.getSolutions().getValue().expirationTime);

        // a new solution which contains an old solution replaces it if expiration is equal
        g = new SolutionGroup<>(bindings);
        sol1 = new Solution<>(5, 0, bindings, now + 1000L * 10);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(4, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 10, g.getSolutions().getValue().expirationTime);
        sol1 = new Solution<>(new Solution<>(5, 0, bindings, now + 1000L * 10), 1);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 10, g.getSolutions().getValue().expirationTime);

        // likewise if the expiration of the new solution is higher...
        g = new SolutionGroup<>(bindings);
        sol1 = new Solution<>(5, 0, bindings, now + 1000L * 10);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(4, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 10, g.getSolutions().getValue().expirationTime);
        sol1 = new Solution<>(new Solution<>(5, 0, bindings, now + 1000L * 20), 1);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 20, g.getSolutions().getValue().expirationTime);

        // ...or infinite
        g = new SolutionGroup<>(bindings);
        sol1 = new Solution<>(5, 0, bindings, now + 1000L * 10);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(4, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 10, g.getSolutions().getValue().expirationTime);
        sol1 = new Solution<>(new Solution<>(5, 0, bindings, 0), 1);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(0, g.getSolutions().getValue().expirationTime);

        // a new solution which contains an old one but has a lower expiration time is added as a separate solution
        g = new SolutionGroup<>(bindings);
        sol1 = new Solution<>(5, 0, bindings, now + 1000L * 10);
        g.add(sol1, now);
        assertEquals(1, g.getSolutions().length());
        assertEquals(4, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 10, g.getSolutions().getValue().expirationTime);
        sol1 = new Solution<>(new Solution<>(5, 0, bindings, now + 1000L * 5), 1);
        g.add(sol1, now);
        assertEquals(2, g.getSolutions().length());
        assertEquals(3, g.getSolutions().getValue().remainingPatterns);
        assertEquals(now + 1000L * 5, g.getSolutions().getValue().expirationTime);

    }
}
