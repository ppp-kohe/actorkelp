package csl.actor.util;

import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

public final class ObjectsList extends AbstractList<Object> implements Serializable, RandomAccess {
    private Object[] buffer;
    private int position;

    public ObjectsList() {
        this(10);
    }

    public ObjectsList(int capacity) {
        this(new Object[capacity], 0);
    }

    public ObjectsList(Object[] buffer, int length) {
        this.buffer = buffer;
        position = length;
    }

    @Override
    public Object get(int index) {
        return buffer[index];
    }

    @Override
    public int size() {
        return position;
    }

    @Override
    public Object[] toArray() {
        return Arrays.copyOf(buffer, size());
    }

    @Override
    public Object set(int index, Object element) {
        Object v = buffer[index];
        buffer[index] = element;
        return v;
    }

    @Override
    public int indexOf(Object o) {
        Object[] buffer = this.buffer;
        if (o == null) {
            for (int i = 0; i < position; ++i) {
                if (buffer[i] == null) {
                    return i;
                }
            }
        } else {
            for (int i = 0; i < position; ++i) {
                if (o.equals(buffer[i])) {
                    return i;
                }
            }
        }
        return -1;
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) >= 0;
    }

    @Override
    public Spliterator<Object> spliterator() {
        return Spliterators.spliterator(buffer, 0, position, Spliterator.ORDERED);
    }

    @Override
    public void clear() {
        position = 0;
    }

    @Override
    public boolean add(Object o) {
        if (position >= buffer.length) {
            grow(position + 1);
        }
        buffer[position++] = o;
        return true;
    }

    private void grow(int n) {
        buffer = Arrays.copyOf(buffer, Math.max(buffer.length * 2, n));
    }

    @Override
    public boolean addAll(Collection<?> c) {
        Object[] a = c.toArray();
        int as = a.length;
        if (as == 0) {
            return false;
        } else {
            Object[] buffer = this.buffer;
            int size = this.position;
            if (as > buffer.length - size) {
                grow(size + as);
            }
            System.arraycopy(a, 0, buffer, size, as);
            position += as;
            return true;
        }
    }

    @Override
    public void forEach(Consumer<? super Object> action) {
        Objects.requireNonNull(action);
        Object[] buffer = this.buffer;
        for (int i = 0, s = position; i < s; ++i) {
            action.accept(buffer[i]);
        }
    }
}
