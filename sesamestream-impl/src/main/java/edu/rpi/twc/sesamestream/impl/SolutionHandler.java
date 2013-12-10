package edu.rpi.twc.sesamestream.impl;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public interface SolutionHandler {
    void handle(PartialSolution ps,
                TriplePattern matched,
                VarList newBindings);
}
