package dsg.ccsvc.log;

import dsg.ccsvc.watch.WatchedEvent;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static dsg.ccsvc.watch.WatchedEvent.WatchType.*;

public class LogRecord implements Serializable {

    public enum LogRecordType {
        NOOP,
        CONNECT,
        CREATE,
        CREATE_EPHEMERAL,
        DELETE,
        SET;

        public static LogRecordType fromInteger(int value) {

            for (LogRecordType type : LogRecordType.values()) {
                if (value == type.ordinal()) {
                    return type;
                }
            }
            return null;
        }
    }

    public static List<WatchedEvent.WatchType> evaluateTriggeredWatchTypes(LogRecord.LogRecordType recordType) {
        switch (recordType) {
            case DELETE:
                return Arrays.asList(EXIST, CHILDREN, DATA);
            case SET:
                return Collections.singletonList(DATA);
            case CREATE_EPHEMERAL:
            case CREATE:
                return Arrays.asList(EXIST, CHILDREN);
            default:
                return Collections.emptyList();
        }
    }

    private static final long serialVersionUID = 1L;
    private String path;
    private byte[] data;
    private final LogRecordType type;
    private LogRecordStats stats;

    // TODO: consider naming this something else
    private final long managerId;

    public LogRecord() {
        // Empty Header Record
        this("", new byte[0], LogRecordType.NOOP, -1);
    }

    // TODO: port all usages over to full constructor
    public LogRecord(String path, byte[] data, LogRecordType type) {
        this(path, data, type, null, -1);
    }

    public LogRecord(String path, byte[] data, LogRecordType type, long managerId) {
        this(path, data, type, null, managerId);
    }

    public LogRecord(String path, byte[] data, LogRecordType type, LogRecordStats stats, long managerId) {
        this.path = path;
        this.data = data;
        this.type = type;
        this.stats = stats;
        this.managerId = managerId;
    }

    public byte[] getData() {
        // TODO: Consider return shallow copy
        return data;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public LogRecordType getType() {
        return type;
    }

    public long getManagerId() {
        return managerId;
    }

    public void setStats(LogRecordStats stats) {
        this.stats = stats;
    }

    public LogRecordStats getStats() {
        return stats;
    }

    public boolean containsStats() {
        return !Objects.isNull(stats);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("===\n");
        stringBuilder.append("path:\n");
        stringBuilder.append(path).append("\n");
        stringBuilder.append("manager_id:\n");
        stringBuilder.append(managerId).append("\n");
        stringBuilder.append("type:\n");
        stringBuilder.append(type).append("\n");
        stringBuilder.append("data:\n");
        stringBuilder.append(new String(data, StandardCharsets.UTF_8)).append("\n");
        if (!Objects.isNull(stats)) {
            stringBuilder.append("stats:\n");
            stringBuilder.append(stats);
        }
        stringBuilder.append("===\n");
        return stringBuilder.toString();
    }
}
