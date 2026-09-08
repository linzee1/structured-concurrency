package io.github.huatalk.parallelinscope.queue;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A clone of JDK's {@code LinkedBlockingQueue} with dynamically adjustable capacity.
 *
 * <p>Uses the classic two-lock queue algorithm (separate {@code putLock} and {@code takeLock} with
 * {@link AtomicInteger} count) for high concurrency.
 *
 * <p>Key addition: {@link #setCapacity(int)} allows runtime capacity adjustment.
 *
 * @param <E> the type of elements held in this queue
 * @author Doug Lea
 */
public class VariableLinkedBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>, Serializable {

    private static final long serialVersionUID = 1L;

    static class Node<E> {
        E item;
        Node<E> next;

        Node(E x) {
            item = x;
        }
    }

    /** Configured queue capacity. */
    private volatile int capacity;

    /** Element count shared by the put and take locks. */
    private final AtomicInteger count = new AtomicInteger();

    /** Sentinel node at the head of the linked queue. */
    transient Node<E> head;

    /** Last node in the linked queue. */
    private transient Node<E> last;

    /** Lock protecting dequeue operations. */
    private final ReentrantLock takeLock = new ReentrantLock();

    /** Condition signaled when the queue becomes non-empty. */
    private final Condition notEmpty = takeLock.newCondition();

    /** Lock protecting enqueue operations. */
    private final ReentrantLock putLock = new ReentrantLock();

    /** Condition signaled when the queue has available capacity. */
    private final Condition notFull = putLock.newCondition();

    /** Creates an effectively unbounded queue. */
    public VariableLinkedBlockingQueue() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Creates an empty queue with the supplied capacity.
     *
     * @param capacity the positive queue capacity
     */
    public VariableLinkedBlockingQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException();
        this.capacity = capacity;
        last = head = new Node<>(null);
    }

    /**
     * Creates an effectively unbounded queue containing the supplied elements.
     *
     * @param c the initial elements
     */
    public VariableLinkedBlockingQueue(Collection<? extends E> c) {
        this(Integer.MAX_VALUE);
        final ReentrantLock lock = this.putLock;
        lock.lock();
        try {
            int n = 0;
            for (E e : c) {
                if (e == null) throw new NullPointerException();
                if (n == capacity) throw new IllegalStateException("Queue full");
                enqueue(new Node<>(e));
                ++n;
            }
            count.set(n);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Dynamically adjusts queue capacity.
     *
     * @param capacity new capacity (must be positive)
     */
    public void setCapacity(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException();
        final int oldCapacity = this.capacity;
        this.capacity = capacity;
        if (capacity > oldCapacity) {
            signalNotFull();
        }
    }

    /**
     * Returns the configured capacity.
     *
     * @return the configured capacity
     */
    public int capacity() {
        return capacity;
    }

    private void signalNotEmpty() {
        final ReentrantLock lock = this.takeLock;
        lock.lock();
        try {
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    private void signalNotFull() {
        final ReentrantLock lock = this.putLock;
        lock.lock();
        try {
            notFull.signal();
        } finally {
            lock.unlock();
        }
    }

    private void enqueue(Node<E> node) {
        last = last.next = node;
    }

    private E dequeue() {
        Node<E> h = head;
        Node<E> first = h.next;
        h.next = h; // help GC
        head = first;
        E x = first.item;
        first.item = null;
        return x;
    }

    void fullyLock() {
        putLock.lock();
        takeLock.lock();
    }

    void fullyUnlock() {
        takeLock.unlock();
        putLock.unlock();
    }

    @Override
    public int size() {
        return count.get();
    }

    @Override
    public int remainingCapacity() {
        return capacity - count.get();
    }

    @Override
    public void put(E e) throws InterruptedException {
        if (e == null) throw new NullPointerException();
        int c;
        Node<E> node = new Node<>(e);
        final ReentrantLock lock = this.putLock;
        final AtomicInteger cnt = this.count;
        lock.lockInterruptibly();
        try {
            while (cnt.get() == capacity) {
                notFull.await();
            }
            enqueue(node);
            c = cnt.getAndIncrement();
            if (c + 1 < capacity) notFull.signal();
        } finally {
            lock.unlock();
        }
        if (c == 0) signalNotEmpty();
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        if (e == null) throw new NullPointerException();
        long nanos = unit.toNanos(timeout);
        int c;
        final ReentrantLock lock = this.putLock;
        final AtomicInteger cnt = this.count;
        lock.lockInterruptibly();
        try {
            while (cnt.get() == capacity) {
                if (nanos <= 0L) return false;
                nanos = notFull.awaitNanos(nanos);
            }
            enqueue(new Node<>(e));
            c = cnt.getAndIncrement();
            if (c + 1 < capacity) notFull.signal();
        } finally {
            lock.unlock();
        }
        if (c == 0) signalNotEmpty();
        return true;
    }

    @Override
    public boolean offer(E e) {
        if (e == null) throw new NullPointerException();
        final AtomicInteger cnt = this.count;
        if (cnt.get() == capacity) return false;
        int c;
        Node<E> node = new Node<>(e);
        final ReentrantLock lock = this.putLock;
        lock.lock();
        try {
            if (cnt.get() == capacity) return false;
            enqueue(node);
            c = cnt.getAndIncrement();
            if (c + 1 < capacity) notFull.signal();
        } finally {
            lock.unlock();
        }
        if (c == 0) signalNotEmpty();
        return true;
    }

    @Override
    public E take() throws InterruptedException {
        E x;
        int c;
        final AtomicInteger cnt = this.count;
        final ReentrantLock lock = this.takeLock;
        lock.lockInterruptibly();
        try {
            while (cnt.get() == 0) {
                notEmpty.await();
            }
            x = dequeue();
            c = cnt.getAndDecrement();
            if (c > 1) notEmpty.signal();
        } finally {
            lock.unlock();
        }
        if (c == capacity) signalNotFull();
        return x;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E x;
        int c;
        long nanos = unit.toNanos(timeout);
        final AtomicInteger cnt = this.count;
        final ReentrantLock lock = this.takeLock;
        lock.lockInterruptibly();
        try {
            while (cnt.get() == 0) {
                if (nanos <= 0L) return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            x = dequeue();
            c = cnt.getAndDecrement();
            if (c > 1) notEmpty.signal();
        } finally {
            lock.unlock();
        }
        if (c == capacity) signalNotFull();
        return x;
    }

    @Override
    public E poll() {
        final AtomicInteger cnt = this.count;
        if (cnt.get() == 0) return null;
        E x;
        int c;
        final ReentrantLock lock = this.takeLock;
        lock.lock();
        try {
            if (cnt.get() == 0) return null;
            x = dequeue();
            c = cnt.getAndDecrement();
            if (c > 1) notEmpty.signal();
        } finally {
            lock.unlock();
        }
        if (c == capacity) signalNotFull();
        return x;
    }

    @Override
    public E peek() {
        if (count.get() == 0) return null;
        final ReentrantLock lock = this.takeLock;
        lock.lock();
        try {
            return (head.next == null) ? null : head.next.item;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) return false;
        fullyLock();
        try {
            for (Node<E> trail = head, p = trail.next; p != null; trail = p, p = p.next) {
                if (o.equals(p.item)) {
                    unlink(p, trail);
                    return true;
                }
            }
            return false;
        } finally {
            fullyUnlock();
        }
    }

    void unlink(Node<E> p, Node<E> trail) {
        p.item = null;
        trail.next = p.next;
        if (last == p) last = trail;
        if (count.getAndDecrement() == capacity) signalNotFull();
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) return false;
        fullyLock();
        try {
            for (Node<E> p = head.next; p != null; p = p.next) {
                if (o.equals(p.item)) return true;
            }
            return false;
        } finally {
            fullyUnlock();
        }
    }

    @Override
    public Object[] toArray() {
        fullyLock();
        try {
            int size = count.get();
            Object[] a = new Object[size];
            int k = 0;
            for (Node<E> p = head.next; p != null; p = p.next) {
                a[k++] = p.item;
            }
            return a;
        } finally {
            fullyUnlock();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T[] toArray(T[] a) {
        fullyLock();
        try {
            int size = count.get();
            if (a.length < size) {
                a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
            }
            int k = 0;
            for (Node<E> p = head.next; p != null; p = p.next) {
                a[k++] = (T) p.item;
            }
            if (a.length > k) a[k] = null;
            return a;
        } finally {
            fullyUnlock();
        }
    }

    @Override
    public void clear() {
        fullyLock();
        try {
            for (Node<E> p, h = head; (p = h.next) != null; h = p) {
                h.next = h;
                p.item = null;
            }
            head = last;
            if (count.getAndSet(0) == capacity) notFull.signal();
        } finally {
            fullyUnlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        if (c == null) throw new NullPointerException();
        if (c == this) throw new IllegalArgumentException();
        if (maxElements <= 0) return 0;
        boolean signalNotFull = false;
        final ReentrantLock lock = this.takeLock;
        lock.lock();
        try {
            int n = Math.min(maxElements, count.get());
            int i = 0;
            try {
                while (i < n) {
                    E x = dequeue();
                    c.add(x);
                    ++i;
                }
                return n;
            } finally {
                if (i > 0) {
                    signalNotFull = (count.getAndAdd(-i) == capacity);
                }
            }
        } finally {
            lock.unlock();
            if (signalNotFull) signalNotFull();
        }
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    private class Itr implements Iterator<E> {
        private Node<E> current;
        private Node<E> lastRet;
        private E currentElement;

        Itr() {
            fullyLock();
            try {
                current = head.next;
                if (current != null) currentElement = current.item;
            } finally {
                fullyUnlock();
            }
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        private Node<E> nextNode(Node<E> p) {
            for (; ; ) {
                Node<E> s = p.next;
                if (s == p) return head.next;
                if (s == null || s.item != null) return s;
                p = s;
            }
        }

        @Override
        public E next() {
            fullyLock();
            try {
                if (current == null) throw new NoSuchElementException();
                lastRet = current;
                E item = currentElement;
                current = nextNode(current);
                currentElement = (current == null) ? null : current.item;
                return item;
            } finally {
                fullyUnlock();
            }
        }

        @Override
        public void remove() {
            if (lastRet == null) throw new IllegalStateException();
            fullyLock();
            try {
                Node<E> node = lastRet;
                lastRet = null;
                for (Node<E> trail = head, p = trail.next; p != null; trail = p, p = p.next) {
                    if (p == node) {
                        unlink(p, trail);
                        break;
                    }
                }
            } finally {
                fullyUnlock();
            }
        }
    }

    @Override
    public Spliterator<E> spliterator() {
        return Spliterators.spliterator(this, Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.CONCURRENT);
    }
}
