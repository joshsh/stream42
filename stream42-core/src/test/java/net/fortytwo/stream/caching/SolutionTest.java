package net.fortytwo.stream.caching;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionTest {
    private final long exp = 0;

    @Test
    public void testCompatibility() throws Exception {
        Solution<String> ps1, ps2;
        Query.QueryVariables vars
                = new Query.QueryVariables(Arrays.asList("x", "y", "z"));
        HashMap<String, String> map1, map2;
        Bindings<String> b1, b2;

        map1 = new HashMap<>();
        map1.put("x", "red");
        map1.put("y", "green");
        b1 = new Bindings<>(map1, vars);
        ps1 = new Solution<>(3, 0, b1, exp);

        map2 = new HashMap<>();
        map2.put("x", "red");
        map2.put("y", "green");
        b2 = new Bindings<>(map2, vars);
        ps2 = new Solution<>(3, 1, b2, exp);
        assertTrue(ps1.composableWith(ps2, vars));
        assertTrue(ps2.composableWith(ps1, vars));

        map2 = new HashMap<>();
        map2.put("x", "red");
        map2.put("y", "blue");
        b2 = new Bindings<>(map2, vars);
        ps2 = new Solution<>(3, 1, b2, exp);
        assertFalse(ps1.composableWith(ps2, vars));
        assertFalse(ps2.composableWith(ps1, vars));

        map2 = new HashMap<>();
        map2.put("x", "red");
        map2.put("z", "blue");
        b2 = new Bindings<>(map2, vars);
        ps2 = new Solution<>(3, 1, b2, exp);
        assertTrue(ps1.composableWith(ps2, vars));
        assertTrue(ps2.composableWith(ps1, vars));

        // ps1 and ps2 are disjoint in terms of variables
        map2 = new HashMap<>();
        map2.put("z", "blue");
        b2 = new Bindings<>(map2, vars);
        ps2 = new Solution<>(3, 1, b2, exp);
        assertTrue(ps1.composableWith(ps2, vars));
        assertTrue(ps2.composableWith(ps1, vars));

        // ps1 and ps2 share a pattern, and are therefore not complementary
        map2 = new HashMap<>();
        map2.put("z", "blue");
        b2 = new Bindings<>(map2, vars);
        ps2 = new Solution<>(3, 0, b2, exp);
        assertFalse(ps1.composableWith(ps2, vars));
        assertFalse(ps2.composableWith(ps1, vars));
    }

    @Test(expected = NoSuchElementException.class)
    public void testInvalidVariables() {
        Solution<String> ps1, ps2;
        Query.QueryVariables vars
                = new Query.QueryVariables(Arrays.asList("x", "y", "z"));
        HashMap<String, String> map1, map2;
        Bindings<String> b1, b2;

        map1 = new HashMap<>();
        map1.put("x", "red");
        map1.put("y", "green");
        b1 = new Bindings<>(map1, vars);
        ps1 = new Solution<>(3, 0, b1, exp);

        // "a" and "b" do not belong to the set of query variables
        map2 = new HashMap<>();
        map2.put("a", "red");
        map2.put("b", "green");
        b2 = new Bindings<>(map2, vars);
        ps2 = new Solution<>(3, 0, b2, exp);

        // this fails with an exception
        ps1.composableWith(ps2, vars);
    }
}
