package dsg.ccsvc.util;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

// TODO: Clean this up
// NOTE: Maintains a one to one mapping
public class DualMapWrapper<K, V> {

    private ConcurrentHashMap<V, K> reverseMap;
    private ConcurrentHashMap<K, V> map;

    public DualMapWrapper() {
        map = new ConcurrentHashMap<>();
        reverseMap = new ConcurrentHashMap<>();
    }

    public ConcurrentHashMap<V, K> getReverseMap() {
        return reverseMap;
    }

    public ConcurrentHashMap<K, V> getMap() {
        return map;
    }

    public void dualPut(K key, V value) {
        // NOTE: Remove outdated mappings
        if (map.containsKey(key)) {
            V oldValue = map.get(key);
            reverseMap.remove(oldValue);
        }

        if (reverseMap.containsKey(value)) {
            K oldKey = reverseMap.get(value);
            map.remove(oldKey);
        }

        map.put(key, value);
        reverseMap.put(value, key);
    }

    public void dualRemove(K key, V value) {
        // NOTE: This is a logical requirement but it should not be possible
        //  to have a broken mapping under expected usage patterns
        V oldValue = map.remove(key);
        if (!Objects.isNull(oldValue) && !oldValue.equals(value)) {
            reverseMap.remove(oldValue);
        }

        K oldKey = reverseMap.remove(value);
        if (!Objects.isNull(oldKey) && !oldKey.equals(key)) {
            map.remove(oldKey);
        }
    }
}
