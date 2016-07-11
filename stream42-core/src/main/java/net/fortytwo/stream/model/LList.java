package net.fortytwo.stream.model;

/**
 * A simple, space-efficient linked list data structure.
 *
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class LList<T> {
    public static final LList NIL = new LList();

    private T value;

    private LList<T> rest;

    private LList() {
        this.value = null;
        this.rest = null;
    }

    private LList(final T value,
                  final LList<T> rest) {
        this.value = value;
        this.rest = rest;
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
        return new LList<>(t, this);
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
