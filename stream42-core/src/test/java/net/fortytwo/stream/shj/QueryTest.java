package net.fortytwo.stream.shj;

import net.fortytwo.stream.StreamProcessor;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class QueryTest extends SHJTestBase {

    @Test
    public void testQueriesWithSinglePattern() {
        List<JoinHelper<String, String>> consumers1, consumers2;
        JoinHelper<String, String> consumer1a, consumer2a;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "?p", "?y"))).getAllHelpers();
        consumers2 = addQuery("query2", graphPattern(
                tuplePattern("?x", "knows", "Ford"))).getAllHelpers();
        consumer1a = consumers1.get(0);
        consumer2a = consumers2.get(0);
        assertArrayEquals(new String[]{"?x", "?p", "?y"}, consumer1a.getKeys());
        assertArrayEquals(new String[]{"?x"}, consumer2a.getKeys());

        queryIndex.add(tuple("Arthur", "knows", "Ford"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 1);
        expectSolutions(consumer1a, 1, "?x", "Arthur");
        expectSolutions(consumer1a, 1, "?p", "knows");
        expectSolutions(consumer1a, 1, "?y", "Ford");
        expectQuerySolutions("query1", 1);
        expectIndexSolutions(consumer2a, 1);
        expectSolutions(consumer2a, 1, "?x", "Arthur");
        expectSolutions(consumer2a, 0, "?p", "knows");
        expectSolutions(consumer2a, 0, "?y", "Ford");
        expectQuerySolutions("query2", 1);
    }

    @Test
    public void testQueryWithMultiplePatterns() {
        List<JoinHelper<String, String>> consumers1;
        JoinHelper<String, String> consumer1a, consumer1b, consumer1c;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "name", "'Ford Prefect'"),
                tuplePattern("?y", "nickname", "'Ix'"))).getAllHelpers();
        consumer1a = consumers1.get(0);
        consumer1b = consumers1.get(1);
        consumer1c = consumers1.get(2);
        assertArrayEquals(new String[]{"?x", "?y"}, consumer1a.getKeys());
        assertArrayEquals(new String[]{"?y"}, consumer1b.getKeys());
        assertArrayEquals(new String[]{"?y"}, consumer1c.getKeys());

        queryIndex.add(tuple("Arthur", "knows", "Ford"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 1);
        expectSolutions(consumer1a, 1, "?x", "Arthur");
        expectSolutions(consumer1a, 1, "?y", "Ford");
        expectIndexSolutions(consumer1b, 0);
        expectIndexSolutions(consumer1c, 0);
        expectQuerySolutions("query1", 0);

        queryIndex.add(tuple("Ford", "nickname", "'Ix'"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 1);
        expectIndexSolutions(consumer1b, 0);
        expectIndexSolutions(consumer1c, 1);
        expectSolutions(consumer1c, 1, "?y", "Ford");
        expectQuerySolutions("query1", 0);

        queryIndex.add(tuple("Ford", "name", "'Ford Prefect'"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 1);
        expectIndexSolutions(consumer1b, 1);
        expectSolutions(consumer1b, 1, "?y", "Ford");
        expectIndexSolutions(consumer1c, 1);
        expectQuerySolutions("query1", 1);
    }

    @Test
    public void testQueryWithVaryingTupleAndPatternLengths() {
        List<JoinHelper<String, String>> consumers1;
        JoinHelper<String, String> consumer1a, consumer1b;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "name", "'Ford Prefect'", "g1"))).getAllHelpers();
        consumer1a = consumers1.get(0);
        consumer1b = consumers1.get(1);
        assertArrayEquals(new String[]{"?x", "?y"}, consumer1a.getKeys());
        assertArrayEquals(new String[]{"?y"}, consumer1b.getKeys());

        // fully matching tuple of length 3
        queryIndex.add(tuple("Arthur", "knows", "Ford"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 1);
        expectSolutions(consumer1a, 1, "?x", "Arthur");
        expectSolutions(consumer1a, 1, "?y", "Ford");
        expectIndexSolutions(consumer1b, 0);
        expectQuerySolutions("query1", 0);

        // tuple of length 3 is too short; doesn't match
        queryIndex.add(tuple("Ford", "name", "'Ford Prefect'"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1b, 0);
        expectQuerySolutions("query1", 0);

        // fully matching tuple of length 4
        queryIndex.add(tuple("Ford", "name", "'Ford Prefect'", "g1"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 1);
        expectIndexSolutions(consumer1b, 1);
        expectSolutions(consumer1b, 1, "?y", "Ford");
        expectQuerySolutions("query1", 1);

        // tuple of length 5 is longer than needed; the first four terms match
        queryIndex.add(tuple("Zaphod", "knows", "Ford", "g1"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 2);
        expectSolutions(consumer1a, 1, "?x", "Arthur");
        expectSolutions(consumer1a, 1, "?x", "Zaphod");
        expectSolutions(consumer1a, 2, "?y", "Ford");
        expectIndexSolutions(consumer1b, 1);
        expectQuerySolutions("query1", 2);
    }

    @Test
    public void testEquivalentQueries() {
        List<JoinHelper<String, String>> consumers1, consumers2;
        JoinHelper<String, String> consumer1, consumer2;

        // add the first query
        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y", "g"))).getAllHelpers();
        consumer1 = consumers1.get(0);
        assertArrayEquals(new String[]{"?x", "?y"}, consumer1.getKeys());

        // add the second query
        // it is identical to the first except for the variable names
        consumers2 = addQuery("query2", graphPattern(
                tuplePattern("?a", "knows", "?b", "g"))).getAllHelpers();
        consumer2 = consumers2.get(0);
        assertArrayEquals(new String[]{"?a", "?b"}, consumer2.getKeys());

        queryIndex.add(tuple("Arthur", "knows", "Ford", "g"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1, 1);
        expectSolutions(consumer1, 1, "?x", "Arthur");
        expectSolutions(consumer1, 0, "?x", "Ford");
        expectSolutions(consumer1, 1, "?y", "Ford");
        expectSolutions(consumer1, 0, "?z", "Arthur");
        expectQuerySolutions("query1", 1);
        expectQuerySolutions("query2", 1);

        queryIndex.add(tuple("Zaphod", "knows", "Ford", "g"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1, 2);
        expectSolutions(consumer1, 2, "?y", "Ford");
        expectSolutions(consumer1, 0, "?x", "Ford");
        expectSolutions(consumer1, 0, "?y", "Arthur");
        expectIndexSolutions(consumer2, 2);
        expectSolutions(consumer2, 2, "?b", "Ford");
        expectQuerySolutions("query1", 2);
        expectQuerySolutions("query2", 2);
    }

    @Test
    public void testTuplePatternWithRepeatedVariables() {
        List<JoinHelper<String, String>> consumers1;
        JoinHelper<String, String> consumer1;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "likes", "?x", "?g"))).getAllHelpers();
        consumer1 = consumers1.get(0);
        assertArrayEquals(new String[]{"?x", "?g"}, consumer1.getKeys());

        queryIndex.add(tuple("Eddie", "likes", "Eddie", "g1"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1, 1);
        expectSolutions(consumer1, 1, "?x", "Eddie");
        expectQuerySolutions("query1", 1);
        queryIndex.add(tuple("Zaphod", "likes", "Zaphod", "g1"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1, 2);
        expectSolutions(consumer1, 1, "?x", "Zaphod");
        expectQuerySolutions("query1", 2);

        // no new solutions from a non-matching tuple
        queryIndex.add(tuple("Eddie", "likes", "Arthur", "g1"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1, 2);
        expectQuerySolutions("query1", 2);
        queryIndex.add(tuple("Arthur", "likes", "Eddie", "g1"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1, 2);
        expectSolutions(consumer1, 1, "?x", "Eddie");
        expectSolutions(consumer1, 0, "?x", "Arthur");
        expectQuerySolutions("query1", 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyQueryIsRejected() {
        addQuery("query1", graphPattern());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryWithEmptyTuplePatternIsRejected() {
        addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern()));
    }

    /**
     * Tests a query with tuple patterns not fully connected with variables or constants
     */
    @Test(expected = IllegalArgumentException.class)
    public void testQueryWithUnconnectedPatternsIsRejected() {
        addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?z", "knows", "?a"),
                tuplePattern("?z", "nickname", "'Ix")));
    }

    // TODO: such queries are legal, and not uncommon

    /**
     * Tests a query with tuple patterns not fully connected with variables,
     * although connected with variables and conrtants
     */
    @Test(expected = IllegalArgumentException.class)
    public void testQueryWithVariableUnconnectedPatternsIsRejected() {
        addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "Ford"),
                tuplePattern("?y", "knows", "Ford")));
    }

    /**
     * Tests queries in which a single input tuple may match more than one pattern.
     */
    @Test
    public void testQueriesWithOverlappingPatterns() {
        List<JoinHelper<String, String>> consumers1, consumers2, consumers3;
        JoinHelper<String, String> consumer1a, consumer2a, consumer2b, consumer3a, consumer3b;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"))).getAllHelpers();
        consumer1a = consumers1.get(0);

        // query2 contains query1
        consumers2 = addQuery("query2", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "likes", "Everything"))).getAllHelpers();
        consumer2a = consumers2.get(0);
        consumer2b = consumers2.get(1);

        // query3 intersects with query2, and also contains query1
        consumers3 = addQuery("query3", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?x", "name", "'Arthur Dent'"))).getAllHelpers();
        consumer3a = consumers3.get(0);
        consumer3b = consumers3.get(1);

        // irrelevant tuple
        queryIndex.add(tuple("France", "capitalCity", "Paris"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 0);
        expectIndexSolutions(consumer2a, 0);
        expectIndexSolutions(consumer2b, 0);
        expectIndexSolutions(consumer3a, 0);
        expectIndexSolutions(consumer3b, 0);
        expectQuerySolutions("query1", 0);
        expectQuerySolutions("query2", 0);
        expectQuerySolutions("query3", 0);

        // tuple only affects query3
        queryIndex.add(tuple("Arthur", "name", "'Arthur Dent'"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 0);
        expectIndexSolutions(consumer2a, 0);
        expectIndexSolutions(consumer2b, 0);
        expectIndexSolutions(consumer3a, 0);
        expectIndexSolutions(consumer3b, 1);
        expectQuerySolutions("query1", 0);
        expectQuerySolutions("query2", 0);
        expectQuerySolutions("query3", 0);

        // tuple affects all three queries, and completes a solution for query1
        queryIndex.add(tuple("Caesar", "knows", "Brutus"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 1);
        expectIndexSolutions(consumer2a, 1);
        expectIndexSolutions(consumer2b, 0);
        expectIndexSolutions(consumer3a, 1);
        expectIndexSolutions(consumer3b, 1);
        expectQuerySolutions("query1", 1);
        expectQuerySolutions("query2", 0);
        expectQuerySolutions("query3", 0);

        // tuple affects all three queries, and completes a solution for query1 and query3
        queryIndex.add(tuple("Arthur", "knows", "Ford"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 2);
        expectIndexSolutions(consumer2a, 2);
        expectIndexSolutions(consumer2b, 0);
        expectIndexSolutions(consumer3a, 2);
        expectIndexSolutions(consumer3b, 1);
        expectQuerySolutions("query1", 2);
        expectQuerySolutions("query2", 0);
        expectQuerySolutions("query3", 1);
    }

    /**
     * Tests a query with redundant tuple patterns.
     * Redundant patterns are tolerated and do not cause duplicate results.
     */
    @Test
    public void testQueriesWithIdenticalPatterns() {
        List<JoinHelper<String, String>> consumers1;
        JoinHelper<String, String> consumer1a, consumer1b, consumer1c, consumer1d;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "likes", "?y", "?g"),
                tuplePattern("?x", "likes", "?y"),
                tuplePattern("?x", "likes", "?y"),
                tuplePattern("?y", "name", "'Tricia McMillan'"))).getAllHelpers();
        consumer1a = consumers1.get(0);
        consumer1b = consumers1.get(1);
        consumer1c = consumers1.get(2);
        consumer1d = consumers1.get(3);
        assertArrayEquals(new String[]{"?x", "?y", "?g"}, consumer1a.getKeys());
        assertArrayEquals(new String[]{"?x", "?y"}, consumer1b.getKeys());
        assertArrayEquals(new String[]{"?x", "?y"}, consumer1c.getKeys());
        assertArrayEquals(new String[]{"?y"}, consumer1d.getKeys());

        // irrelevant tuple
        queryIndex.add(tuple("Eddie", "name", "'Eddie'"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 0);
        expectIndexSolutions(consumer1b, 0);
        expectIndexSolutions(consumer1c, 0);
        expectIndexSolutions(consumer1d, 0);
        expectQuerySolutions("query1", 0);

        // relevant tuple
        queryIndex.add(tuple("Trillian", "name", "'Tricia McMillan'"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 0);
        expectIndexSolutions(consumer1b, 0);
        expectIndexSolutions(consumer1c, 0);
        expectIndexSolutions(consumer1d, 1);
        expectQuerySolutions("query1", 0);

        // tuple matches patterns b and c, but not a, which requires a graph
        queryIndex.add(tuple("Arthur", "likes", "Trillian"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 0);
        expectIndexSolutions(consumer1b, 1);
        expectIndexSolutions(consumer1c, 1);
        expectIndexSolutions(consumer1d, 1);
        expectQuerySolutions("query1", 0);

        // tuple matches patterns a, b, and c, and completes a single solution
        queryIndex.add(tuple("Zaphod", "likes", "Trillian", "g1"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 1);
        expectIndexSolutions(consumer1b, 2);
        expectIndexSolutions(consumer1c, 2);
        expectIndexSolutions(consumer1d, 1);
        expectQuerySolutions("query1", 1);

        // tuple matches patterns a, b, and c, but does not create duplicate solutions at b and c.
        // Another single solution is completed.
        queryIndex.add(tuple("Arthur", "likes", "Trillian", "g1"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 2);
        expectIndexSolutions(consumer1b, 2);
        expectIndexSolutions(consumer1c, 2);
        expectIndexSolutions(consumer1d, 1);
        expectQuerySolutions("query1", 2);
    }

    /**
     * Tests queries in which a pattern has no variables.
     * Such queries are currently rejected, as solution indices are based on variable bindings.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testQueriesWithNoVariablePatterns() {
        addQuery("query1", graphPattern(
                tuplePattern("Arthur", "likes", "Trillian")));
    }

    @Test
    public void testCircularQuery() {
        List<JoinHelper<String, String>> consumers1;
        JoinHelper<String, String> consumer1a, consumer1b, consumer1c, consumer1d;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "knows", "?z"),
                tuplePattern("?z", "knows", "?a"),
                tuplePattern("?a", "knows", "?x"))).getAllHelpers();
        consumer1a = consumers1.get(0);
        consumer1b = consumers1.get(1);
        consumer1c = consumers1.get(2);
        consumer1d = consumers1.get(3);
        assertArrayEquals(new String[]{"?x", "?y"}, consumer1a.getKeys());
        assertArrayEquals(new String[]{"?y", "?z"}, consumer1b.getKeys());
        assertArrayEquals(new String[]{"?z", "?a"}, consumer1c.getKeys());
        assertArrayEquals(new String[]{"?a", "?x"}, consumer1d.getKeys());

        // irrelevant tuple
        queryIndex.add(tuple("Arthur", "likes", "Trillian"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 0);
        expectIndexSolutions(consumer1b, 0);
        expectIndexSolutions(consumer1c, 0);
        expectIndexSolutions(consumer1d, 0);
        expectQuerySolutions("query1", 0);

        // relevant tuple #1 of 4
        queryIndex.add(tuple("Arthur", "knows", "Ford"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 1);
        expectIndexSolutions(consumer1b, 1);
        expectIndexSolutions(consumer1c, 1);
        expectIndexSolutions(consumer1d, 1);
        expectQuerySolutions("query1", 0);

        // relevant tuple #2 of 4
        queryIndex.add(tuple("Ford", "knows", "Zaphod"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 2);
        expectIndexSolutions(consumer1b, 2);
        expectIndexSolutions(consumer1c, 2);
        expectIndexSolutions(consumer1d, 2);
        expectQuerySolutions("query1", 0);

        // relevant tuple #3 of 4
        queryIndex.add(tuple("Zaphod", "knows", "Trillian"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 3);
        expectIndexSolutions(consumer1b, 3);
        expectIndexSolutions(consumer1c, 3);
        expectIndexSolutions(consumer1d, 3);
        expectQuerySolutions("query1", 0);

        // relevant tuple #4 of 4. There are now four distinct solutions.
        queryIndex.add(tuple("Trillian", "knows", "Arthur"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 4);
        expectIndexSolutions(consumer1b, 4);
        expectIndexSolutions(consumer1c, 4);
        expectIndexSolutions(consumer1d, 4);
        expectQuerySolutions("query1", 4);

        // duplicate tuple has no effect on the solution indices, but triggers the four solutions again.
        queryIndex.add(tuple("Trillian", "knows", "Arthur"), StreamProcessor.NEVER_EXPIRE);
        expectIndexSolutions(consumer1a, 4);
        expectIndexSolutions(consumer1b, 4);
        expectIndexSolutions(consumer1c, 4);
        expectIndexSolutions(consumer1d, 4);
        expectQuerySolutions("query1", 4);
    }

    @Test
    public void testPathQueries1() {
        List<JoinHelper<String, String>> consumers1;
        JoinHelper<String, String> consumer1a;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"))).getAllHelpers();
        consumer1a = consumers1.get(0);

        addExampleTuples();
        expectIndexSolutions(consumer1a, 7);
        expectQuerySolutions("query1", 7);
    }

    @Test
    public void testPathQueries2() {
        List<JoinHelper<String, String>> consumers1;
        JoinHelper<String, String> consumer1a, consumer1b;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "knows", "?z"))).getAllHelpers();
        consumer1a = consumers1.get(0);
        consumer1b = consumers1.get(1);

        addExampleTuples();
        expectIndexSolutions(consumer1a, 7);
        expectIndexSolutions(consumer1b, 7);
        expectQuerySolutions("query1", 10);
    }

    @Test
    public void testPathQueries3() {
        List<JoinHelper<String, String>> consumers1;
        JoinHelper<String, String> consumer1a, consumer1b, consumer1c;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "knows", "?z"),
                tuplePattern("?z", "knows", "?a"))).getAllHelpers();
        consumer1a = consumers1.get(0);
        consumer1b = consumers1.get(1);
        consumer1c = consumers1.get(2);

        addExampleTuples();
        expectIndexSolutions(consumer1a, 7);
        expectIndexSolutions(consumer1b, 7);
        expectIndexSolutions(consumer1c, 7);
        expectQuerySolutions("query1", 17);

        /*
        addLessExampleTuples();
        expectIndexSolutions(consumer1a, 2);
        expectIndexSolutions(consumer1b, 2);
        expectIndexSolutions(consumer1c, 2);
        expectQuerySolutions("query1", 2);
        */
    }

    @Test
    public void testPathQueries4() {
        List<JoinHelper<String, String>> consumers1;
        JoinHelper<String, String> consumer1a, consumer1b, consumer1c, consumer1d;

        consumers1 = addQuery("query1", graphPattern(
                tuplePattern("?x", "knows", "?y"),
                tuplePattern("?y", "knows", "?z"),
                tuplePattern("?z", "knows", "?a"),
                tuplePattern("?a", "knows", "?b"))).getAllHelpers();
        consumer1a = consumers1.get(0);
        consumer1b = consumers1.get(1);
        consumer1c = consumers1.get(2);
        consumer1d = consumers1.get(3);

        addExampleTuples();
        expectIndexSolutions(consumer1a, 7);
        expectIndexSolutions(consumer1b, 7);
        expectIndexSolutions(consumer1c, 7);
        expectIndexSolutions(consumer1d, 7);
        expectQuerySolutions("query1", 27);
    }

    private void addExampleTuples() {
        for (String[] tuple : new String[][]{
                {"Arthur", "knows", "Trillian"},
                {"Arthur", "knows", "Marvin"},
                {"Arthur", "knows", "Ford"},
                {"Arthur", "knows", "Zaphod"},
                {"Ford", "knows", "Arthur"},
                {"Ford", "knows", "Zaphod"},
                {"Zaphod", "knows", "Ford"}
        }) {
            queryIndex.add(tuple, StreamProcessor.NEVER_EXPIRE);
        }
    }

    private void addLessExampleTuples() {
        for (String[] tuple : new String[][]{
                {"Arthur", "knows", "Ford"},
                {"Ford", "knows", "Arthur"},
        }) {
            queryIndex.add(tuple, StreamProcessor.NEVER_EXPIRE);
        }
    }
}
