package net.fortytwo.stream.model;

/**
 * A variable or constant, e.g. a query variable an RDF Term or in the sense of SPARQL
* @author Joshua Shinavier (http://fortytwo.net)
*/
public class VariableOrConstant<K, V> {
    private final K variable;
    private final V constant;

    public VariableOrConstant(K variable, V constant) {
        this.variable = variable;
        this.constant = constant;

        if (null == constant && null == variable) {
            throw new IllegalArgumentException("both variable and constant are null");
        } if (null != constant && null != variable) {
            throw new IllegalArgumentException("both variable and constant are non-null");
        }
    }

    public K getVariable() {
        return variable;
    }

    public V getConstant() {
        return constant;
    }
}
