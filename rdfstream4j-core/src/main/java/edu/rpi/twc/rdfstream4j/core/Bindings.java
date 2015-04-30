package edu.rpi.twc.rdfstream4j.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A set of variable/value pairs representing a complete or partial query answer.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class Bindings<T> {
    private final Map<String, T> map;

    // bit field; bits for bound variables are 1, unbound variables 0 in order defined by the query
    private final int boundVariables;

    private Integer hash;

    /**
     * Constructs a new set of bindings
     *
     * @param map  a map of keys to values.  The keys must be drawn from the set of query variables
     * @param vars the set of query variables referenced by these bindings
     */
    public Bindings(final Map<String, T> map,
                    final Query.QueryVariables vars) {
        this.map = map;

        int b = 0;
        for (String v : map.keySet()) {
            int i = vars.indexOf(v);
            b |= (1 << i);
        }
        boundVariables = b;
    }

    private Bindings(final Map<String, T> map,
                     final int boundVariables) {
        this.map = map;
        this.boundVariables = boundVariables;
    }

    /**
     * Gets a hashing key guaranteed to be the same for identical bindings
     *
     * @return a hashing key generated from these bindings
     */
    public int getHash() {
        if (null == hash) {
            int h = 0;

            for (Map.Entry<String, T> e : map.entrySet()) {
                // TODO: avoid the expensive multiplication operation
                h += e.getKey().hashCode() * e.getValue().hashCode();
            }

            hash = h;
        }

        return hash;
    }

    /**
     * Creates or returns a set of bindings which contains both sets of arguments.
     * A new object is only created if the result is not identical to one of the arguments;
     * otherwise, an argument is returned.
     *
     * @param first a component binding set
     * @param second a component binding set
     * @param <T> the type of value bound
     * @return the resulting set of bindings
     */
    public static <T> Bindings<T> from(final Bindings<T> first,
                                       final Bindings<T> second) {

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
        bindings.putAll(first.map);
        bindings.putAll(second.map);
        return new Bindings<T>(bindings, boundVariables);
    }

    /**
     * Gets the value associated with a variable in these bindings
     * @param variable the variable, as a key
     * @return the associated value
     */
    public T get(final String variable) {
        return map.get(variable);
    }

    /**
     * The set of variable/value entries of these bindings
     * @return a set of variable/value entries
     */
    public Set<Map.Entry<String, T>> entrySet() {
        return map.entrySet();
    }

    /**
     * Gets the number of bindings
     * @return the number of bindings
     */
    public int size() {
        return map.size();
    }

    /**
     * Finds whether these bindings are compatible with another set of bindings.
     * Two sets of bindings are compatible if, for each variable they have in common, they have identical values.
     *
     * @param other the other set of bindings to test
     * @param vars the query variables from which both sets of bindings are drawn
     * @return whether these bindings are compatible with the provided bindings
     */
    public boolean compatibleWith(final Bindings<T> other,
                                  final Query.QueryVariables vars) {
        // compare only those bindings for variables shared between the two solutions
        int b = boundVariables & other.boundVariables;
        String[] a = vars.asArray();
        for (String v : a) {
            if (0 != (b & 1)) {
                if (!map.get(v).equals(other.map.get(v))) {
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
        for (Map.Entry<String, T> e : map.entrySet()) {
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

    @Override
    public int hashCode() {
        return getHash();
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof Bindings && (((Bindings) other).getHash() == getHash());
    }
}
