package dsg.ccsvc.log;

public class LogRecordMetadata {

    private final long offset;
    private final int length;

    public LogRecordMetadata(long recordOffset, int recordLength) {
        this.offset = recordOffset;
        this.length = recordLength;
    }

    public long getOffset() {
        // TODO: Consider return shallow copy
        return offset;
    }

    public int getLength() {
        return length;
    }
}
