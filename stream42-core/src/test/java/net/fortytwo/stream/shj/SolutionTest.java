package net.fortytwo.stream.shj;

import net.fortytwo.stream.StreamProcessor;
import org.junit.Before;
import org.junit.Test;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SolutionTest extends SHJTestBase {

    private static final Logger logger = Logger.getLogger(SolutionTest.class.getName());

    @Before
    public void setUp() {
        setGlobalLogLevel(Level.FINE);
        super.setUp();
    }

    private void setGlobalLogLevel(Level level) {
        Logger log = logger;
        while (null != log) {
            log.setLevel(level);
            for (Handler h : log.getHandlers()) {
                h.setLevel(level);
            }
            log = log.getParent();
        }
    }

    @Test
    public void testSolutionSets() {
        Solution<String> s1, s2;
        long expTime = StreamProcessor.NEVER_EXPIRE;
        s1 = newSolution(new String[]{}, expTime);
        s2 = newSolution(new String[]{}, expTime);

        assertTrue(s1.equals(s1));
        assertTrue(s1.equals(s2));

        s2 = newSolution(new String[]{"v1"}, expTime);
        assertTrue(s2.equals(s2));
        assertFalse(s1.equals(s2));

        s1 = newSolution(new String[]{"v2"}, expTime);
        assertFalse(s1.equals(s2));

        s1 = newSolution(new String[]{"v1", "v2"}, expTime);
        assertFalse(s1.equals(s2));

        s2 = newSolution(new String[]{"v1", "v2"}, expTime);
        assertTrue(s1.equals(s2));

        s2 = newSolution(new String[]{"v1", "v3"}, expTime);
        assertFalse(s1.equals(s2));
    }
}
