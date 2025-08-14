package dsg.ccsvc.caching.baeldung;

/**
 * Created by arash on 09.07.21.
 */
public class BaeldungDummyNode<T> implements BaeldungLinkedListNode<T> {
    private BaeldungDoublyLinkedList<T> list;

    public BaeldungDummyNode(BaeldungDoublyLinkedList<T> list) {
        this.list = list;
    }

    @Override
    public boolean hasElement() {
        return false;
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public T getElement() throws NullPointerException {
        throw new NullPointerException();
    }

    @Override
    public void detach() {
        return;
    }

    @Override
    public BaeldungDoublyLinkedList<T> getListReference() {
        return list;
    }

    @Override
    public BaeldungLinkedListNode<T> setPrev(BaeldungLinkedListNode<T> next) {
        return next;
    }

    @Override
    public BaeldungLinkedListNode<T> setNext(BaeldungLinkedListNode<T> prev) {
        return prev;
    }

    @Override
    public BaeldungLinkedListNode<T> getPrev() {
        return this;
    }

    @Override
    public BaeldungLinkedListNode<T> getNext() {
        return this;
    }

    @Override
    public BaeldungLinkedListNode<T> search(T value) {
        return this;
    }
}
