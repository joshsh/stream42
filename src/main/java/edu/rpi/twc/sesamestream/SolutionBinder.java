package edu.rpi.twc.sesamestream;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
interface SolutionBinder {
    void bind(PartialSolution ps,
              TriplePattern matched,
              VarList newBindings);
}
