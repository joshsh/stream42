package net.fortytwo.stream.shj;

import net.fortytwo.stream.StreamProcessor;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryIndexTest extends SHJTestBase {

    @Test
    public void testRemoveQuery() {
        Query<String, String> query1, query2, query3, query4;

        assertTrue(queryIndex.isEmpty());

        query1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "likes", "Everything")));
        query2 = addQuery("query2", graphPattern(
                tuplePattern("?x", "knows", "?y")));
        query3 = addQuery("query3", graphPattern(
                tuplePattern("?a", "knows", "?b")));
        query4 = addQuery("query4", graphPattern(
                tuplePattern("?z", "likes", "?z")));

        assertFalse(queryIndex.isEmpty());

        assertTrue(queryIndex.remove(query2));
        assertFalse(queryIndex.isEmpty());
        assertTrue(queryIndex.remove(query3));
        assertFalse(queryIndex.isEmpty());
        assertTrue(queryIndex.remove(query1));
        assertFalse(queryIndex.isEmpty());
        assertTrue(queryIndex.remove(query4));
        assertTrue(queryIndex.isEmpty());

        // removal is idempotent
        assertFalse(queryIndex.remove(query2));
        assertTrue(queryIndex.isEmpty());

        // TODO: test that solutions are no longer computed as queries are removed
    }

    @Test
    public void testRemoveTuples() {
        List<JoinHelper<String, String>> consumers1, consumers2;
        JoinHelper<String, String> consumer1a, consumer1b, consumer2a;

        assertTrue(queryIndex.isEmpty());
        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "likes", "Everything"))).getAllHelpers();
        consumer1a = consumers1.get(0);
        consumer1b = consumers1.get(1);
        consumers2 = addQuery("query2", graphPattern(
                tuplePattern("?a", "knows", "?b"))).getAllHelpers();
        consumer2a = consumers2.get(0);
        assertFalse(queryIndex.isEmpty());
        assertEquals(0, consumer1a.getSolutions().size());
        assertEquals(0, consumer1b.getSolutions().size());
        assertEquals(0, consumer2a.getSolutions().size());

        // removing non-existent tuples has no effect
        assertFalse(queryIndex.remove(tuple("Arthur", "knows", "Ford")));

        queryIndex.add(tuple("Arthur", "knows", "Ford"), StreamProcessor.NEVER_EXPIRE);
        assertEquals(1, consumer1a.getSolutions().size());
        assertEquals(0, consumer1b.getSolutions().size());
        assertEquals(1, consumer2a.getSolutions().size());
        expectQuerySolutions("query1", 0);
        expectQuerySolutions("query2", 1);

        assertFalse(queryIndex.remove(tuple("Arthur", "knows", "Zaphod")));
        assertFalse(queryIndex.remove(tuple("Ford", "likes", "Everything")));
        assertFalse(queryIndex.remove(tuple("Arthur", "hasFriend", "Ford")));
        assertEquals(1, consumer1a.getSolutions().size());
        assertEquals(0, consumer1b.getSolutions().size());
        assertEquals(1, consumer2a.getSolutions().size());

        assertTrue(queryIndex.remove(tuple("Arthur", "knows", "Ford")));
        assertEquals(0, consumer1a.getSolutions().size());
        assertEquals(0, consumer1b.getSolutions().size());
        assertEquals(0, consumer2a.getSolutions().size());

        // this tuple's 3-prefix matches and removes an indexed tuple
        assertFalse(queryIndex.remove(tuple("Arthur", "knows", "Ford", "g1")));
        queryIndex.add(tuple("Arthur", "knows", "Ford"), StreamProcessor.NEVER_EXPIRE);
        expectQuerySolutions("query1", 0);
        expectQuerySolutions("query2", 1);
        assertTrue(queryIndex.remove(tuple("Arthur", "knows", "Ford", "g1")));
    }
}
