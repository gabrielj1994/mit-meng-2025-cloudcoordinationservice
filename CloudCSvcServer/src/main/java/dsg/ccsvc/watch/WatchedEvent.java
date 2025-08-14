package dsg.ccsvc.watch;

import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.datalayer.DataLayerMgrBase;

import java.util.Objects;

public class WatchedEvent {

    public enum WatchType {
        DATA("data"),
        CHILDREN("children"),
        EXIST("exist");

        private String cmdStr;

        WatchType(String cmdStr) {
            this.cmdStr = cmdStr;
        }
        public static WatchType fromInteger(int value) {
            for (WatchType type : WatchType.values()) {
                if (value == type.ordinal()) {
                    return type;
                }
            }

            return null;
        }

        public String getCmdStr() {
            return cmdStr;
        }
    }
    private String path;

    private WatchType watchType;

    private long managerId;

    private int transactionId;

    public void setPath(String path) {
        this.path = path;
    }

    public WatchType getWatchType() {
        return watchType;
    }

    public void setWatchType(WatchType watchType) {
        this.watchType = watchType;
    }

    public long getManagerId() {
        return managerId;
    }

    public void setManagerId(long managerId) {
        this.managerId = managerId;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    private WatchConsumerWrapper<WatchInput> watcherCallable;

    // TODO: Refactor watchedEvent and watchconsumerwrapper
    public WatchedEvent(String path, WatchConsumerWrapper<WatchInput> watcherCallable) {
        if (Objects.isNull(watcherCallable)) {
            throw new IllegalArgumentException("ERROR - WatchedEvent - Null required parameter 'watcherCallable'");
        }
        this.path = path;
        this.watcherCallable = watcherCallable;
        this.watchType = watcherCallable.getWatchType();
    }

    public String getPath() {
        return path;
    }

    public void triggerWatch(DataLayerMgrBase manager, WatchInput watchInput) {
        watcherCallable.accept(manager, watchInput);
    }

    public boolean getWatchPersistentFlag() {
        return watcherCallable.getPersistentFlag();
    }

    public long getTriggerTransactionThreshold() {
        return watcherCallable.getTxIdThreshold();
    }
}
