package org.example.storage;

import org.example.model.Record;

import java.util.List;
import java.util.Optional;

public interface DataStore {

    boolean updateRecord(Record record);

    boolean replaceRecords(List<Record> records);

    boolean removeRecord(String recordKey);

    Optional<Record> getRecord(String recordKey);

    List<Record> getAllRecords();

}
