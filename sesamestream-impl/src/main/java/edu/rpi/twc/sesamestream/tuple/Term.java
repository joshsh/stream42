package edu.rpi.twc.sesamestream.tuple;

/**
 * A value or variable, analogous to an RDF Term or query variable in the sense of SPARQL
 */
public class Term<T> {
    private final T value;
    private final String variable;

    public Term(T value, String variable) {
        this.value = value;
        this.variable = variable;
    }

    public T getValue() {
        return value;
    }

    public String getVariable() {
        return variable;
    }
}
