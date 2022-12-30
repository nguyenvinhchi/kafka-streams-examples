package org.example.base.stock.util;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class FixedSizePriorityQueue<T> {
    private TreeSet<T> inner;
    private int maxSize;

    public FixedSizePriorityQueue(Comparator<T> comparator, int maxSize) {
        this.maxSize = maxSize;
        this.inner = new TreeSet<>(comparator);
    }

    public FixedSizePriorityQueue<T> add(T ele) {
        inner.add(ele);
        if (inner.size() > maxSize) {
            inner.pollLast(); // last is the smallest item ?
        }
        return this;
    }

    public FixedSizePriorityQueue<T> remove(T ele) {
        if (inner.contains(ele)) {
            inner.remove(ele);
        }
        return this;
    }

    public Iterator<T> iterator() {
        return inner.iterator();
    }

    @Override
    public String toString() {
        return "FixedSizePriorityQueue{" +
                "QueueContents=" + inner + "}";

    }
}
