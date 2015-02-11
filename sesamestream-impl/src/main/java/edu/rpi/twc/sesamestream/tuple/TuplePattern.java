package edu.rpi.twc.sesamestream.tuple;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class TuplePattern<T> {
    private final Term<T>[] terms;
    private GraphPattern<T> graphPattern;
    private int index;

    public TuplePattern(final Term<T>[] terms) {
        this.terms = terms;
    }

    public Term<T>[] getTerms() {
        return terms;
    }

    public GraphPattern<T> getGraphPattern() {
        return graphPattern;
    }

    public int getIndex() {
        return index;
    }

    public void setGraphPattern(GraphPattern<T> graphPattern) {
        this.graphPattern = graphPattern;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
