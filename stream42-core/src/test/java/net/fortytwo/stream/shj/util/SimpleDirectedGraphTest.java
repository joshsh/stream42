package net.fortytwo.stream.shj.util;

import net.fortytwo.stream.model.SimpleDirectedGraph;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class SimpleDirectedGraphTest {
    @Test
    public void testIsFullyConnected() {
        SimpleDirectedGraph<Integer> graph = new SimpleDirectedGraph<>();
        assertFalse(graph.isFullyConnected());

        graph = new SimpleDirectedGraph<>();
        graph.addEdge(1, 1);
        assertTrue(graph.isFullyConnected());

        graph = new SimpleDirectedGraph<>();
        graph.addEdge(1, 2);
        assertTrue(graph.isFullyConnected());

        graph = new SimpleDirectedGraph<>();
        graph.addEdge(1, 2);
        graph.addEdge(2, 3);
        graph.addEdge(3, 4);
        assertTrue(graph.isFullyConnected());

        graph = new SimpleDirectedGraph<>();
        graph.addEdge(1, 2);
        graph.addEdge(2, 3);
        graph.addEdge(3, 1);
        assertTrue(graph.isFullyConnected());

        graph = new SimpleDirectedGraph<>();
        graph.addEdge(1, 2);
        graph.addEdge(3, 4);
        assertFalse(graph.isFullyConnected());

        graph = new SimpleDirectedGraph<>();
        graph.addEdge(1, 2);
        graph.addEdge(2, 3);
        graph.addEdge(1, 4);
        graph.addEdge(5, 6);
        graph.addEdge(6, 5);
        assertFalse(graph.isFullyConnected());
    }
}
