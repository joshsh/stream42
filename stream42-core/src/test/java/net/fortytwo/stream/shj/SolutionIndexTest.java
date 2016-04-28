package net.fortytwo.stream.shj;

import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionIndexTest extends SHJTestBase {

    @Test
    public void testAll() {
        String[] values;
        Solution<String> solution;
        SolutionIndex<String> solutionIndex = new SolutionIndex<>(context, 2);

        /* TODO: use pattern
        VariableOrConstant<String, String>[] pattern = new VariableOrConstant[]{
                new VariableOrConstant<String, String>("a", null),
                new VariableOrConstant<String, String>(null, "knows"),
                new VariableOrConstant<String, String>("b", null),
        };
        */

        assertTrue(solutionIndex.isEmpty());

        solutionIndex.getConsumerIndex().add(new TestVisitor());
        assertTrue(solutionIndex.isEmpty());

        values = new String[]{"Arthur", "Ford"};

        solution = new Solution<>(values, solutionIndex);
        solutionIndex.add(solution);
        assertFalse(solutionIndex.isEmpty());
    }

    private static class TestVisitor implements Consumer<Solution<String>> {
        @Override
        public void accept(Solution<String> solution) {
            System.out.println("### solution " + solution
                    + " valid until " + solution.getExpirationTime());
        }
    }
}
