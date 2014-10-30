package edu.rpi.twc.sesamestream.impl;

import edu.rpi.twc.sesamestream.SesameStream;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.Var;

/**
 * A tuple of values or variables
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class VarList {

    private final String name;
    private final Value value;
    private final VarList rest;

    public VarList(final String name,
                   final Value value,
                   final VarList rest) {
        this.name = name;
        this.value = value;
        this.rest = rest;

        if (SesameStream.getDoDebug()) {
            checkForDuplicateName(name);
        }
    }

    private void checkForDuplicateName(final String name) {
        if (null == name) {
            return;
        }

        VarList cur = this.getRest();
        while (null != cur) {
            if (name.equals(cur.getName())) {
                throw new IllegalStateException("duplicate name '" + name + "' in VarList: " + this);
            }

            cur = cur.rest;
        }
    }

    public String getName() {
        return name;
    }

    public Value getValue() {
        return value;
    }

    public VarList getRest() {
        return rest;
    }

    public Var asVar() {
        return new Var(name, value);
    }

    /**
     * Creates a new list containing the elements of two other lists.
     * The order of elements in the original lists is not preserved.
     * @param first a list of elements to add.  It will appear in the tail of the resulting list
     * @param second another list of elements to add.
     *               It will appear in the head of the resulting list, in reverse order
     * @return the resulting list
     */
    public static VarList union(final VarList first,
                                final VarList second) {
        VarList cur1 = second;
        VarList cur2 = first;

        while (null != cur2) {
            cur1 = new VarList(cur2.name, cur2.value, cur1);
            cur2 = cur2.rest;
        }

        return cur1;
    }

    /**
     * @return the number of elements in this list
     */
    public int length() {
        VarList cur = this;
        int l = 0;
        while (cur != null) {
            l++;
            cur = cur.rest;
        }

        return l;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VarList(");
        boolean first = true;
        VarList cur = this;
        while (null != cur) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }

            sb.append(cur.name).append(":").append(cur.value);

            cur = cur.rest;
        }
        sb.append(")");
        return sb.toString();
    }
}
