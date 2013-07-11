package edu.rpi.twc.sesamestream;

import org.openrdf.model.Value;
import org.openrdf.query.algebra.Var;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class VarList {

    public static final VarList NIL = new VarList();

    private final String name;
    private final Value value;
    private final VarList rest;

    private VarList() {
        name = null;
        value = null;
        rest = null;
    }

    public VarList(final String name,
                   final Value value,
                   final VarList rest) {
        this.name = name;
        this.value = value;
        this.rest = rest;

        if (SesameStream.DEBUG) {
            checkForDuplicateName(name);
        }
    }

    private void checkForDuplicateName(final String name) {
        if (null == name) {
            return;
        }

        VarList cur = this.getRest();
        while (!cur.isNil()) {
            if (name.equals(cur.getName())) {
                throw new IllegalStateException("duplicate name '" + name + "' in VarList: " + this);
            }

            cur = cur.rest;
        }
    }

    public boolean isNil() {
        return rest == null;
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

    public VarList prepend(final VarList other) {
        VarList cur1 = this;
        VarList cur2 = other;

        while (!cur2.isNil()) {
            cur1 = new VarList(cur2.name, cur2.value, cur1);
            cur2 = cur2.rest;
        }

        return cur1;
    }

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
        while (!cur.isNil()) {
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
