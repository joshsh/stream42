package net.fortytwo.stream.shj;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphPatternTest extends SHJTestBase {

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyTuplePatternArrayIsRejected() {
        new GraphPattern<>();
    }

    @Test
    public void testTuplePatternsWithDifferingLengthsAreAccepted() {
        GraphPattern<String, String> pattern = new GraphPattern<>(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?x", "age", "21", "2016graph")
        );
        assertEquals(2, pattern.getPatterns().length);
    }

    @Test
    public void testTuplePatternsAreOrdered() {
        GraphPattern<String, String> pattern = new GraphPattern<>(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?x", "age", "21")
        );
        assertEquals(2, pattern.getPatterns().length);
        assertEquals(3, pattern.getPatterns()[0].getPattern().length);
        assertEquals(3, pattern.getPatterns()[1].getPattern().length);
        assertEquals("?x", pattern.getPatterns()[0].getPattern()[0].getVariable());
        assertNull(pattern.getPatterns()[0].getPattern()[0].getConstant());
        assertNull(pattern.getPatterns()[0].getPattern()[1].getVariable());
        assertEquals("knows", pattern.getPatterns()[0].getPattern()[1].getConstant());
        assertEquals("?y", pattern.getPatterns()[0].getPattern()[2].getVariable());
        assertNull(pattern.getPatterns()[0].getPattern()[2].getConstant());
        assertEquals("?x", pattern.getPatterns()[1].getPattern()[0].getVariable());
        assertNull(pattern.getPatterns()[1].getPattern()[0].getConstant());
        assertNull(pattern.getPatterns()[1].getPattern()[1].getVariable());
        assertEquals("age", pattern.getPatterns()[1].getPattern()[1].getConstant());
        assertNull(pattern.getPatterns()[1].getPattern()[2].getVariable());
        assertEquals("21", pattern.getPatterns()[1].getPattern()[2].getConstant());
    }

    @Test
    public void testIsFullyConnected() {
        GraphPattern<String, String> pattern;

        pattern = graphPattern(
                tuplePattern("?x", "knows", "?y"));
        assertTrue(pattern.isFullyConnected());

        pattern = graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "knows", "Arthur"));
        assertTrue(pattern.isFullyConnected());

        pattern = graphPattern(
                tuplePattern("Ford", "knows", "Arthur"));
        assertTrue(pattern.isFullyConnected());

        pattern = graphPattern(
                tuplePattern("Ford", "knows", "Arthur"),
                tuplePattern("Arthur", "knows", "Trillian"));
        assertFalse(pattern.isFullyConnected());

        pattern = graphPattern(
                tuplePattern("Ford", "knows", "Arthur"),
                tuplePattern("?x", "knows", "Arthur"));
        assertFalse(pattern.isFullyConnected());

        pattern = graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "knows", "Arthur"),
                tuplePattern("?z", "likes", "Beer"));
        assertFalse(pattern.isFullyConnected());
    }
}
