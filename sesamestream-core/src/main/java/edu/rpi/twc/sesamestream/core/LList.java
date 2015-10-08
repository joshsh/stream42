package edu.rpi.twc.sesamestream.core;

import java.util.Collection;

/**
 * A simple, space-efficient linked list data structure.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class LList<T> {
    public static LList NIL = new LList();

    private T value;

    private LList<T> rest;

    private LList() {
        this.value = null;
        this.rest = null;
    }

    public LList(final T value,
                 final LList<T> rest) {
        this.value = value;
        this.rest = rest;
    }

    public LList(final Collection<T> values) {
        if (0 == values.size()) {
            throw new IllegalArgumentException();
        }

        LList<T> restCur = NIL;
        T valueCur = null;

        for (T t : values) {
            if (!restCur.isNil()) {
                restCur = new LList<T>(valueCur, restCur);
            }

            valueCur = t;
        }

        rest = restCur;
        value = valueCur;
    }

    public boolean isNil() {
        return null == rest;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public LList<T> getRest() {
        return rest;
    }

    public void setRest(LList<T> rest) {
        this.rest = rest;
    }

    public LList<T> push(final T t) {
        return new LList<T>(t, this);
    }

    public static <T> LList<T> union(final LList<T> first,
                                     final LList<T> second) {
        LList<T> cur1 = second;
        LList<T> cur2 = first;

        while (!cur2.isNil()) {
            cur1 = new LList<T>(cur2.value, cur1);
            cur2 = cur2.rest;
        }

        return cur1;
    }

    public int length() {
        LList<T> cur = this;
        int l = 0;
        while (!cur.isNil()) {
            l++;
            cur = cur.rest;
        }

        return l;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LList(");
        boolean first = true;
        LList<T> cur = this;
        while (!cur.isNil()) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }

            sb.append(cur.value);

            cur = cur.rest;
        }
        sb.append(")");
        return sb.toString();
    }
}
