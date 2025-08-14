package dsg.ccsvc.caching.baeldung;

public interface BaeldungLinkedListNode<V> {
    boolean hasElement();

    boolean isEmpty();

    V getElement() throws NullPointerException;

    void detach();

    BaeldungDoublyLinkedList<V> getListReference();

    BaeldungLinkedListNode<V> setPrev(BaeldungLinkedListNode<V> prev);

    BaeldungLinkedListNode<V> setNext(BaeldungLinkedListNode<V> next);

    BaeldungLinkedListNode<V> getPrev();

    BaeldungLinkedListNode<V> getNext();

    BaeldungLinkedListNode<V> search(V value);
}
