package edu.rpi.twc.sesamestream.tuple;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class BindingsTest {
    @Test
    public void testHash() throws Exception {
        Query.QueryVariables vars
                = new Query.QueryVariables(Arrays.asList("x", "y", "z"));
        Map<String, String> map = new HashMap<String, String>();
        Set<Long> hashes = new HashSet<Long>();
        long hash;

        map.clear();
        map.put("x", "red");
        hash = new Bindings<String>(map, vars).getHash();
        hashes.add(hash);

        map.clear();
        map.put("x", "red");
        map.put("y", "green");
        hash = new Bindings<String>(map, vars).getHash();
        hashes.add(hash);

        map.clear();
        map.put("x", "green");
        map.put("y", "red");
        hash = new Bindings<String>(map, vars).getHash();
        hashes.add(hash);

        map.clear();
        map.put("x", "blue");
        hash = new Bindings<String>(map, vars).getHash();
        hashes.add(hash);

        assertEquals(4, hashes.size());
    }

    @Test
    public void testCompatibility() throws Exception {
        Map<String, String> map1, map2;
        Bindings<String> b1, b2, b3;

        Query.QueryVariables vars
                = new Query.QueryVariables(Arrays.asList("x", "y", "z"));

        map1 = new HashMap<String, String>();
        map1.put("x", "red");
        b1 = new Bindings<String>(map1, vars);
        assertEquals("red", b1.get("x"));

        map2 = new HashMap<String, String>();
        map2.put("y", "green");
        b2 = new Bindings<String>(map2, vars);
        assertNull(b2.get("x"));
        assertEquals("green", b2.get("y"));

        assertTrue(b1.compatibleWith(b2, vars));
        assertTrue(b2.compatibleWith(b1, vars));

        b3 = Bindings.from(b1, b2);
        assertEquals("red", b3.get("x"));
        assertEquals("green", b3.get("y"));

        assertTrue(b1.compatibleWith(b3, vars));
        assertTrue(b2.compatibleWith(b3, vars));
        assertTrue(b3.compatibleWith(b2, vars));
        assertTrue(b3.compatibleWith(b1, vars));

        map1 = new HashMap<String, String>();
        map1.put("x", "puce");
        b1 = new Bindings<String>(map1, vars);
        assertEquals("puce", b1.get("x"));

        assertFalse(b1.compatibleWith(b3, vars));
        assertFalse(b3.compatibleWith(b1, vars));
        assertTrue(b1.compatibleWith(b2, vars));
        assertTrue(b2.compatibleWith(b1, vars));
    }
}
