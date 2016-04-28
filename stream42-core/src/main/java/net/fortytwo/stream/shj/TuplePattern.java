package net.fortytwo.stream.shj;

import net.fortytwo.stream.model.VariableOrConstant;

/**
 * A more general form of a SPARQL triple pattern
 * <p/>
 * See https://www.w3.org/TR/sparql11-query/#defn_TriplePattern
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class TuplePattern<K, V> {
    private final VariableOrConstant<K, V>[] pattern;
    private JoinHelper<K, V> joinHelper;

    public TuplePattern(VariableOrConstant<K, V>[] pattern) {
        if (null == pattern || 0 == pattern.length) {
            throw new IllegalArgumentException("null or empty pattern");
        }

        this.pattern = pattern;
    }

    public JoinHelper<K, V> getJoinHelper() {
        return joinHelper;
    }

    public void setJoinHelper(JoinHelper<K, V> consumer) {
        if (null != this.joinHelper) {
            throw new IllegalStateException();
        }

        this.joinHelper = consumer;
    }

    public VariableOrConstant<K, V>[] getPattern() {
        return pattern;
    }

    public int getLength() {
        return pattern.length;
    }
}
