package dsg.ccsvc.watch;

import java.util.function.BiConsumer;

import dsg.ccsvc.watch.WatchedEvent.WatchType;
import dsg.ccsvc.datalayer.DataLayerMgrBase;

public class WatchConsumerWrapper<T> implements BiConsumer<DataLayerMgrBase, T> {

    private BiConsumer<DataLayerMgrBase, T> wrappedConsumer;
    private boolean persistentFlag;
    private WatchType watchType;
    private long txIdThreshold;
    private boolean evaluateThresholdFlag;

    public WatchConsumerWrapper(BiConsumer<DataLayerMgrBase, T> consumer, boolean persistentFlag, WatchType type,
                                long transactionThreshold, boolean evaluateThresholdFlag) {
        this.wrappedConsumer = consumer;
        this.persistentFlag = persistentFlag;
        this.watchType = type;
        this.txIdThreshold = transactionThreshold;
        this.evaluateThresholdFlag = evaluateThresholdFlag;
    }

    public WatchConsumerWrapper(BiConsumer<DataLayerMgrBase, T> consumer, boolean persistentFlag, WatchType type) {
        this(consumer, persistentFlag, type, 0, true);
    }

    public void accept(DataLayerMgrBase manager, T t) {
        wrappedConsumer.accept(manager, t);
    }

    public boolean getPersistentFlag() {
        return persistentFlag;
    }

    public WatchType getWatchType() {
        return watchType;
    }

    public long getTxIdThreshold() {
        return txIdThreshold;
    }

    public void setTxIdThreshold(long transactionThreshold) {
        this.txIdThreshold = transactionThreshold;
    }

    public boolean getEvaluateThresholdFlag() {
        return evaluateThresholdFlag;
    }
}
