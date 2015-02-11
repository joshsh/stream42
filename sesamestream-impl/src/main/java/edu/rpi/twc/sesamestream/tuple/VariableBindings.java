package edu.rpi.twc.sesamestream.tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class VariableBindings<T> {
    private final Map<String, T> bindings;
    private final int boundVariables;

    private Long hash;

    public VariableBindings(final Map<String, T> bindings,
                            final GraphPattern.QueryVariables vars) {
        this.bindings = bindings;

        int b = 0;
        for (String v : bindings.keySet()) {
            int i = vars.indexOf(v);
            b |= (1 << i);
        }
        boundVariables = b;
    }

    private VariableBindings(final Map<String, T> bindings,
                             final int boundVariables) {
        this.bindings = bindings;
        this.boundVariables = boundVariables;
    }

    public Long getHash() {
        if (null == hash) {
            long h = 0L;

            for (Map.Entry<String, T> e : bindings.entrySet()) {
                h += e.getKey().hashCode() * e.getValue().hashCode();
            }

            hash = h;
        }

        return hash;
    }

    public static <T> VariableBindings<T> from(final VariableBindings<T> first,
                                               final VariableBindings<T> second) {

        int x = first.boundVariables ^ second.boundVariables;

        if (0 == x) {
            if (first.boundVariables == second.boundVariables) {
                return first;
            }
        } else if (0 == (first.boundVariables & x)) {
            return second;
        } else if (0 == (second.boundVariables & x)) {
            return first;
        }

        int boundVariables = first.boundVariables | second.boundVariables;
        Map<String, T> bindings = new HashMap<String, T>();
        bindings.putAll(first.bindings);
        bindings.putAll(second.bindings);
        return new VariableBindings<T>(bindings, boundVariables);
    }

    public T get(final String variable) {
        return bindings.get(variable);
    }

    public Set<Map.Entry<String, T>> entrySet() {
        return bindings.entrySet();
    }

    public int size() {
        return bindings.size();
    }

    public boolean compatibleWith(final VariableBindings<T> other,
                                  final GraphPattern.QueryVariables vars) {
        // compare only those bindings for variables shared between the two solutions
        int b = boundVariables & other.boundVariables;
        String[] a = vars.asArray();
        for (String v : a) {
            if (0 != (b & 1)) {
                if (!bindings.get(v).equals(other.bindings.get(v))) {
                    return false;
                }
            }
            b = b >> 1;
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<String, T> e : bindings.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }

            sb.append(e.getKey()).append(":").append(e.getValue());
        }
        sb.append("}");
        return sb.toString();
    }
}
