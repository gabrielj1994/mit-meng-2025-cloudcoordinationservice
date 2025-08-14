package dsg.ccsvc.caching;

import java.util.Optional;

public interface CoordinationSvcCacheLayer<K, V> {
    boolean put(K key, V value);

    Optional<V> get(K key);

    // NOTE: Added GJ94
    boolean remove(K key);

    int size();

    boolean isEmpty();

    void clear();
}
