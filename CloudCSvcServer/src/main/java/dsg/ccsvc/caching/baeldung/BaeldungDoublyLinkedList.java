package dsg.ccsvc.caching.baeldung;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BaeldungDoublyLinkedList<T> {

    private BaeldungDummyNode<T> dummyNode;
    private BaeldungLinkedListNode<T> head;
    private BaeldungLinkedListNode<T> tail;
    private AtomicInteger size;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
 
    public BaeldungDoublyLinkedList() {
        this.dummyNode = new BaeldungDummyNode<T>(this);
        clear();
    }

    public void clear() {
        this.lock.writeLock().lock();
        try {
            head = dummyNode;
            tail = dummyNode;
            size = new AtomicInteger(0);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public int size() {
        this.lock.readLock().lock();
        try {
            return size.get();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        this.lock.readLock().lock();
        try {
            return head.isEmpty();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public boolean contains(T value) {
        this.lock.readLock().lock();
        try {
            return search(value).hasElement();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public BaeldungLinkedListNode<T> search(T value) {
        this.lock.readLock().lock();
        try {
            return head.search(value);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    public BaeldungLinkedListNode<T> add(T value) {
        this.lock.writeLock().lock();
        try {
            head = new BaeldungNode<T>(value, head, this);
            if (tail.isEmpty()) {
                tail = head;
            }
            size.incrementAndGet();
            return head;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public boolean addAll(Collection<T> values) {
        this.lock.writeLock().lock();
        try {
            for (T value : values) {
                if (add(value).isEmpty()) {
                    return false;
                }
            }
            return true;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public BaeldungLinkedListNode<T> remove(T value) {
        this.lock.writeLock().lock();
        try {
            BaeldungLinkedListNode<T> linkedListNode = head.search(value);
            if (!linkedListNode.isEmpty()) {
                if (linkedListNode == tail) {
                    tail = tail.getPrev();
                }
                if (linkedListNode == head) {
                    head = head.getNext();
                }
                linkedListNode.detach();
                size.decrementAndGet();
            }
            return linkedListNode;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public BaeldungLinkedListNode<T> removeTail() {
        this.lock.writeLock().lock();
        try {
            BaeldungLinkedListNode<T> oldTail = tail;
            if (oldTail == head) {
                tail = head = dummyNode;
            } else {
                tail = tail.getPrev();
                oldTail.detach();
            }
            if (!oldTail.isEmpty()) {
                size.decrementAndGet();
            }
            return oldTail;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public BaeldungLinkedListNode<T> moveToFront(BaeldungLinkedListNode<T> node) {
        return node.isEmpty() ? dummyNode : updateAndMoveToFront(node, node.getElement());
    }

    public BaeldungLinkedListNode<T> updateAndMoveToFront(BaeldungLinkedListNode<T> node, T newValue) {
        this.lock.writeLock().lock();
        try {
            if (node.isEmpty() || (this != (node.getListReference()))) {
                return dummyNode;
            }
            detach(node);
            add(newValue);
            return head;
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void detach(BaeldungLinkedListNode<T> node) {
        if (node != tail) {
            node.detach();
            if (node == head) {
                head = head.getNext();
            }
            size.decrementAndGet();
        } else {
            removeTail();
        }
    }
}
