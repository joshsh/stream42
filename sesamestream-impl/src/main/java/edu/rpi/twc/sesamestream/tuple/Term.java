package edu.rpi.twc.sesamestream.tuple;

/**
 * A value or variable, analogous to an RDF Term or query variable in the sense of SPARQL
 */
public class Term<T> {
    private final T value;
    private final String variable;

    /**
     * Constructs a new term.
     * Either a non-null variable and a null value, or a null variable and a non-null value must be supplied
     * as arguments.
     *
     * @param value the value of the term
     * @param variable the variable of the term
     */
    public Term(T value, String variable) {
        if (null == value && null == variable) {
            throw new IllegalArgumentException("both variable and value are null");
        } if (null != value && null != variable) {
            throw new IllegalArgumentException("both variable and value are non-null");
        }

        this.value = value;
        this.variable = variable;
    }

    /**
     * Gets the value of the term
     *
     * @return the value of the term
     */
    public T getValue() {
        return value;
    }

    /**
     * Gets the variable of the term
     *
     * @return the variable of the term
     */
    public String getVariable() {
        return variable;
    }
}
