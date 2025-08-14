package dsg.ccsvc.caching.baeldung;

/**
 * Created by arash on 09.07.21.
 */
public class BaeldungNode<T> implements BaeldungLinkedListNode<T> {
    private T value;
    private BaeldungDoublyLinkedList<T> list;
    private BaeldungLinkedListNode next;
    private BaeldungLinkedListNode prev;

    public BaeldungNode(T value, BaeldungLinkedListNode<T> next, BaeldungDoublyLinkedList<T> list) {
        this.value = value;
        this.next = next;
        this.setPrev(next.getPrev());
        this.prev.setNext(this);
        this.next.setPrev(this);
        this.list = list;
    }

    @Override
    public boolean hasElement() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    public T getElement() {
        return value;
    }

    public void detach() {
        this.prev.setNext(this.getNext());
        this.next.setPrev(this.getPrev());
    }

    @Override
    public BaeldungDoublyLinkedList<T> getListReference() {
        return this.list;
    }

    @Override
    public BaeldungLinkedListNode<T> setPrev(BaeldungLinkedListNode<T> prev) {
        this.prev = prev;
        return this;
    }

    @Override
    public BaeldungLinkedListNode<T> setNext(BaeldungLinkedListNode<T> next) {
        this.next = next;
        return this;
    }

    @Override
    public BaeldungLinkedListNode<T> getPrev() {
        return this.prev;
    }

    @Override
    public BaeldungLinkedListNode<T> getNext() {
        return this.next;
    }

    @Override
    public BaeldungLinkedListNode<T> search(T value) {
        return this.getElement() == value ? this : this.getNext().search(value);
    }
}
