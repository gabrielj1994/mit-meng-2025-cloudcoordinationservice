package org.example.storage;

import org.example.model.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

// TODO: Consider just replicating the cache
public class SimpleStore implements DataStore {

    HashMap<String, Record> recordMap;

    public SimpleStore() {
        recordMap = new HashMap<>();
    }

    @Override
    public boolean updateRecord(Record record) {
        recordMap.put(record.getKey(), record);
        return true;
    }

    @Override
    public boolean replaceRecords(List<Record> records) {
        // TODO: Consider lock store
        recordMap.clear();
        for (Record record : records) {
            recordMap.put(record.getKey(), record);
        }
        return true;
    }

    @Override
    public boolean removeRecord(String recordKey) {
        recordMap.remove(recordKey);
        return true;
    }

    @Override
    public Optional<Record> getRecord(String recordKey) {
        return Optional.ofNullable(recordMap.get(recordKey));
    }

    @Override
    public List<Record> getAllRecords() {
        return new ArrayList<>(recordMap.values());
    }
}
