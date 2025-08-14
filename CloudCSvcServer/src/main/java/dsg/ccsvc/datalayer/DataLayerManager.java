package dsg.ccsvc.datalayer;

import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.caching.CoordinationSvcCacheLayer;
import dsg.ccsvc.command.*;
import dsg.ccsvc.command.CommitOptions;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent;
import dsg.ccsvc.InvalidServiceStateException;

import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.Future;

public interface DataLayerManager extends AutoCloseable {

    void startManager() throws InvalidServiceStateException, IOException;

    long getManagerId();

    boolean claimSoftLease() throws InvalidServiceStateException;
    boolean claimSoftLease(boolean waitFlag) throws InvalidServiceStateException;

    // TODO: Clean up API
    boolean claimLease() throws IOException;

    Future<Boolean> submitSoftLeaseClaim();

    Future<ManagerLeaseHandlerBase> claimSoftLeaseHandler(boolean highPriorityFlag);

    long getLogValidationToken();

    void releaseLease() throws IOException;

    boolean releaseSoftLease();

    boolean extendSoftLease() throws InvalidServiceStateException;

    boolean extendSoftLease(Connection conn) throws InvalidServiceStateException;

    boolean validateEntry();

    boolean addWatch(String path, WatchConsumerWrapper<WatchInput> watchCallable) throws InvalidServiceStateException;

    boolean removeWatch(String path, WatchedEvent.WatchType type) throws InvalidServiceStateException;

    LogRecord retrieveFromLog(String path, boolean getStatsFlag, WatchConsumerWrapper<WatchInput> watchCallable) throws InvalidServiceStateException;

    void enableCacheLayer(CoordinationSvcCacheLayer<String, LogRecord> cache);

    LogRecord retrieveFromCache(String path, boolean statsFlag, boolean readThroughFlag) throws InvalidServiceStateException;

    CommandOutput retrieveChildren(String path, boolean statsFlag,
                                          WatchConsumerWrapper<WatchInput> watchCallable,
                                          boolean blockingFlag, boolean syncLogFlag) throws InvalidServiceStateException;

    boolean commitToLog(long logValidationValue, LogRecord logRecord,
                        CommitOptions options) throws InvalidServiceStateException;

    // TODO: Decide on proper design for execution flows
    // TODO: Refactor return types to Output / Response class (can check for output or error-flags/errors)
    CommandOutput create(CreateCommand cmd);

    CommandOutput delete(DeleteCommand cmd);

    CommandOutput get(GetCommand cmd);

    // TODO: add missing function abstractions
    // Instant logInstant = manager.getLogInstant();
    // prepareManager(manager);
    // boolean commitFlag = manager.commitToLog(logInstant, logRecord, false);
    //public Instant getLogInstant() {
    // private boolean validateLog(Instant logInstant) {
    // public
}
