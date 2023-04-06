package com.cdy.queueBuffer.concurrent;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentQuickList<T> implements Cloneable, Serializable {
    private AtomicInteger usedSize;
    private volatile int totalSize = 8;
    private ArrayNode<T> head;
    private volatile ArrayNode<T> tail;

    private void init(int size) {
        this.usedSize = new AtomicInteger();
        this.totalSize = size;
        this.head = new ArrayNode<>(size, 0);
        this.tail = this.head;
    }

    public ConcurrentQuickList(int size) {
        this.init(size);
    }

    public ConcurrentQuickList() {
        this.init(this.totalSize);
    }

    public int size() {
        return usedSize.get();
    }

    public void add(T t) {
        if (t == null) {
            throw new NullPointerException();
        }
        int curSize = this.usedSize.incrementAndGet(), index = curSize - 1;
        if (curSize < 0) {
            this.usedSize.decrementAndGet();
            throw new OutOfMemoryError();
        }
        if (index >= this.totalSize) {
            synchronized (this) {
                if (index >= this.totalSize) {
                    ArrayNode newNode = new ArrayNode(Math.max(this.totalSize / 2, index - this.totalSize + 1), this.totalSize);
                    this.tail.next = newNode;
                    newNode.pre = this.tail;
                    this.tail = newNode;
                    this.totalSize += newNode.size();
                }
            }
        }
        ArrayNode targetNode = this.tail;
        if (index < targetNode.beginIndex) {
            do {
                targetNode = targetNode.pre;
            } while (index < targetNode.beginIndex);
        } else if (index >= targetNode.beginIndex + targetNode.size()) {
            do {
                targetNode = targetNode.next;
            } while (index >= targetNode.beginIndex + targetNode.size());
        }
        targetNode.add(t, index - targetNode.beginIndex);
    }

    public T strongConsistencyGet(int index) {
        rangeCheck(index);
        ArrayNode<T> node = this.head, preNode = node;
        int tempTotalLength = node.array.length;
        while (index >= tempTotalLength) {
            preNode = node;
            node = node.next;
            if (node == null) {
                do {
                    try {
                        Thread.sleep(0);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    node = preNode.next;
                } while (node == null);
            }
            tempTotalLength += node.array.length;
        }
        int targetIndex = node.array.length - (tempTotalLength - index);
        Object o = node.array[targetIndex];
        if (o == null) {
            do {
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                o = node.array[targetIndex];
            } while (o == null);
        }
        return (T) o;
    }

    public T weakConsistencyGet(int index) {
        rangeCheck(index);
        ArrayNode<T> node = this.head;
        int tempTotalLength = node.array.length;
        while (index >= tempTotalLength) {
            node = node.next;
            if (node == null) {
                return null;
            }
            tempTotalLength += node.array.length;
        }
        return (T) node.array[node.array.length - (tempTotalLength - index)];
    }

    public void strongConsistencyForEach(CallBack<? super T> callBack) {
        ArrayNode<T> node = this.head, preNode = node;
        int endIndex = usedSize.get() - 1, curIndex = 0;
        while (curIndex <= endIndex) {
            if (node == null) {
                do {
                    try {
                        Thread.sleep(0);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    node = preNode.next;
                } while (node == null);
            }
            for (int i = 0; i < node.array.length; i++, curIndex++) {
                if (curIndex > endIndex) {
                    return;
                }
                Object o = node.array[i];
                if (o == null) {
                    do {
                        try {
                            Thread.sleep(0);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        o = node.array[i];
                    } while (o == null);
                }
                callBack.call((T) o);
            }
            preNode = node;
            node = node.next;
        }
    }

    public void weakConsistencyForEach(CallBack<? super T> callBack) {
        ArrayNode<T> node = this.head;
        int endIndex = usedSize.get() - 1, curIndex = 0;
        while (node != null) {
            for (int i = 0; i < node.array.length; i++, curIndex++) {
                if (curIndex > endIndex) {
                    return;
                }
                Object o = node.array[i];
                if (o == null) {
                    continue;
                }
                callBack.call((T) o);
            }
            node = node.next;
        }
    }

    private void rangeCheck(int index) {
        int curSize = this.usedSize.get();
        if (index >= curSize) {
            throw new IndexOutOfBoundsException("Index: " + index+ ", Size: " + curSize);
        }
    }

    @Override
    public String toString() {
        StringBuilder content = new StringBuilder();
        ArrayNode tempNode = this.head;
        int size = this.usedSize.get();
        int index = 0;
        while (tempNode != null) {
            for (int i = 0; i < tempNode.size() && index < size; i++, index++) {
                Object o = tempNode.array[i];
                if (o == null) {
                    content.append("null");
                } else {
                    content.append(o.toString());
                }
                content.append(",");
            }
            tempNode = tempNode.next;
        }
        return "[" + (content.length() > 0 ? content.substring(0, content.length() - 1) : "") + "]";
    }

    private class ArrayNode<T> {

        private int beginIndex;

        private Object[] array;

        private volatile ArrayNode pre;

        private volatile ArrayNode next;

        public ArrayNode(int size, int beginIndex) {
            this.array = new Object[size];
            this.beginIndex = beginIndex;
        }

        public void add(T t, int index) {
            this.array[index] = t;
        }

        public int size() {
            return this.array.length;
        }
    }

    public interface CallBack<T> {

        public void call(T t);

    }
}
