package dsg.ccsvc.caching.baeldung;

/**
 * Created by arash on 09.07.21.
 */

public class BaeldungCacheElement<K,V> {
    private K key;
    private V value;

    public BaeldungCacheElement(K key, V value) {
        this.value = value;
        this.key = key;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }
}
