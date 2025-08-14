package dsg.ccsvc.caching;

import dsg.ccsvc.caching.baeldung.BaeldungLRUCache;

import java.util.Optional;

public class CSvcSimpleLRUCache<K, V> implements CoordinationSvcCacheLayer<K, V> {

    private BaeldungLRUCache<K, V> lruCache;

    public CSvcSimpleLRUCache(int size) {
        this.lruCache = new BaeldungLRUCache<>(size);
    }

    public boolean put(K key, V value) {
        return lruCache.put(key, value);
    }

    public Optional<V> get(K key) {
        return lruCache.get(key);
    }

    // NOTE: Added GJ94
    public boolean remove(K key) {
        return lruCache.remove(key);
    }

    public int size() {
        return lruCache.size();
    }

    public boolean isEmpty() {
        return lruCache.isEmpty();
    }

    public void clear() {
        lruCache.clear();
    }
}
