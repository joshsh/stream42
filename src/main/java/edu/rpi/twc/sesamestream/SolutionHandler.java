package edu.rpi.twc.sesamestream;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
interface SolutionHandler {
    void handle(PartialSolution ps,
                TriplePattern matched,
                VarList newBindings);
}
