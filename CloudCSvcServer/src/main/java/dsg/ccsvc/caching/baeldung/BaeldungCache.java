package dsg.ccsvc.caching.baeldung;

import java.util.Optional;

public interface BaeldungCache<K, V> {
    boolean put(K key, V value);

    Optional<V> get(K key);

    // NOTE: Added GJ94
    boolean remove(K key);

    int size();

    boolean isEmpty();

    void clear();
}
