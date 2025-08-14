package dsg.ccsvc.log;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class LogRecordFSHeader implements Serializable {

    private static final long serialVersionUID = 1L;

    // TODO: bigger size than needed at the moment. Future proofing
    public static final int HEADER_SIZE = 128;
    private int transactionId;
    private int logEntryByteSize;
    // TODO: Consider what other metadata can be exposed in header
    public LogRecordFSHeader(int transactionId, int logRecordSize) {
        this.transactionId = transactionId;
        this.logEntryByteSize = logRecordSize;
    }
}
