package dsg.ccsvc.datalayer;

import dsg.ccsvc.*;
import dsg.ccsvc.caching.CoordinationSvcCacheLayer;
import dsg.ccsvc.command.*;
import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.log.LogRecordMetadata;
import dsg.ccsvc.log.LogRecordStats;
import dsg.ccsvc.util.DebugConstants;
import dsg.ccsvc.util.DualMapWrapper;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;
import java.nio.file.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

@Slf4j
public abstract class DataLayerMgrBase implements DataLayerManager {

    static final String DELETED_RECORD_PATH = "DELETED_RECORD";
    long managerId;
    int tbdLeaseMultiplier = 1;
    ConcurrentHashMap<String, LogRecordMetadata> pathMetadataMap;
    final Object outstandingChangesMonitor = new Object();
    final Object localStructsMonitor = new Object();
    CoordinationSvcCacheLayer<String, LogRecord> recordDataCache;
    ConcurrentHashMap<String, LogRecordStats> pathStatsMap;
    ConcurrentHashMap<String, Set<String>> pathChildrenMap;
    // TODO: Revisit if sessions are needed
    ConcurrentHashMap<Integer, String> sessionsMap;
    ConcurrentHashMap<String, Integer> watchManager;

    // TODO: Consider if ephem tracking structs are needed
    ConcurrentHashMap<String, Long> ephemeralOwnersMap;
    ConcurrentHashMap<Long, Integer> ephemeralOwnerCount;
    ConcurrentHashMap<Long, Set<String>> ephemeralOwnerToPaths;
//    ConcurrentHashMap<Long, HashSet<String>> ephemeralOwnerToPaths;
    // TODO: Consider if these data structs from ZK design are valuable
//    HashMap<Long, String> localOutstandingTxnToPath;
//    HashMap<String, Long> localPathToOutstandingTxn;
    DualMapWrapper<Integer, String> localOutstandingTxnToPath;
    ConcurrentHashMap<String, LogRecord> localOutstandingPathToRecord;

    ConcurrentHashMap<WatchedEvent.WatchType, ConcurrentHashMap<String, HashSet<WatchedEvent>>> pathWatchMap;

    String cSvcNodeRoot;

    boolean isRunning = false;
    boolean isInitialized = false;

    void initializeMetadataStores() {
        /**
         * read log and populate relevant store
         *
         */
        pathMetadataMap = new ConcurrentHashMap<>();
        pathChildrenMap = new ConcurrentHashMap<>();
        watchManager = new ConcurrentHashMap<>();
        sessionsMap = new ConcurrentHashMap<>();
        pathStatsMap = new ConcurrentHashMap<>();
        ephemeralOwnersMap = new ConcurrentHashMap<>();
        ephemeralOwnerCount = new ConcurrentHashMap<>();
        ephemeralOwnerToPaths = new ConcurrentHashMap<>();
        localOutstandingPathToRecord = new ConcurrentHashMap<>();
        localOutstandingTxnToPath = new DualMapWrapper<>();
        pathWatchMap = new ConcurrentHashMap<>();
        pathWatchMap.put(WatchedEvent.WatchType.EXIST, new ConcurrentHashMap<>());
        pathWatchMap.put(WatchedEvent.WatchType.DATA, new ConcurrentHashMap<>());
        pathWatchMap.put(WatchedEvent.WatchType.CHILDREN, new ConcurrentHashMap<>());
//        path_metadata_map.put(znode_root, new LogRecordMetadata(0, 0));
        pathChildrenMap.put(this.cSvcNodeRoot, new HashSet<>());

        pathStatsMap.put(this.cSvcNodeRoot, new LogRecordStats(0, 0,
                0, 0, 0, 0, -1, -1, 0, 0, 0));
    }

    // TODO: is this needed?
//    private void validateMetadataStores() {
//
//    }

    // TODO: consider if this is the proper design?
    //  Does any other ephemeral info need to be exposed
    public boolean containsEphemeralNode(String path) {
        return ephemeralOwnersMap.containsKey(path);
    }

    public Set<String> getChildrenNodes(String parentPath) {
        return pathChildrenMap.get(parentPath);
    }

    public boolean validatePath(String path) {
        boolean isValid = pathChildrenMap.containsKey(path);
        synchronized (outstandingChangesMonitor) {
            if (localOutstandingPathToRecord.containsKey(path)) {
                isValid = !(localOutstandingPathToRecord.get(path).getType()
                            .equals(LogRecord.LogRecordType.DELETE));
            }
        }
        return isValid;
    }

    public boolean validateNewPath(String newRecordPath) {
        String parentPath = Paths.get(newRecordPath).getParent().toString();
        boolean isNodeValid;
        synchronized (outstandingChangesMonitor) {
            // NOTE: Check parent path
            isNodeValid = pathChildrenMap.containsKey(parentPath)
                    || localOutstandingPathToRecord.containsKey(parentPath);
            // NOTE: Check node path
            isNodeValid = isNodeValid && !pathChildrenMap.containsKey(newRecordPath)
                    && !localOutstandingPathToRecord.containsKey(newRecordPath);
        }
        return isNodeValid;
    }

    void initializeMetadata(String zNodeRoot) {
        this.cSvcNodeRoot = zNodeRoot;
        initializeMetadataStores();
    }

    public void addWatch(String path, WatchedEvent watcher) {

    }

    // TODO: consider moving this call into the manager, and have it pass in the necessary information
    public void updateMetadataStore(LogRecord logRecord, long timestamp, int transactionId) {
//    public void updateMetadataStore(LogRecord logRecord) {
        // NOTE: No validation on this method. Assumes operation is successful.
//        this.validateMetadataStores();
        if (DebugConstants.DEBUG_FLAG) {
            log.debug(String.format("updateMetadataStore - Baseline [type=%s, path=%s]",
                                                logRecord.getType(), logRecord.getPath()));
        }
        if (logRecord.getType().equals(LogRecord.LogRecordType.NOOP)) {
            return;
        }
        Path filepath = Paths.get(logRecord.getPath());
        String filename = Objects.isNull(filepath.getFileName()) ? "" : filepath.getFileName().toString();
        String parentPathStr = Objects.isNull(filepath.getParent()) ? "" : filepath.getParent().toString();
//        long timestamp = this.getRecordTimestamp(filepath.toString());
//        int transactionId = this.getRecordTransactionId(filepath.toString());
        LogRecordStats stats;
        Long ephemeralOwner = 0L;
        synchronized (localStructsMonitor) {
            switch (logRecord.getType()) {
                case SET:
                    // TODO: Consider validations; Currently null pointer exception on missing
                    stats = this.pathStatsMap.get(filepath.toString());
                    stats.incrementVersion();
                    stats.setModifiedTransactionId(transactionId);
                    stats.setModifiedTimestamp(timestamp);
                    stats.setDataLength(logRecord.getData().length);
                    break;
//            case NOOP:
//                break;
                case CREATE_EPHEMERAL:
                    if (DebugConstants.DEBUG_FLAG) {
                        log.debug(String.format("updateMetadataStore - CREATE EPHEMERAL - [path=%s]", filepath));
                    }
                    ephemeralOwner = logRecord.getManagerId();
                    // NOTE: manage ephemeral struct
                    this.ephemeralOwnersMap.put(filepath.toString(), ephemeralOwner);
                    if(!this.ephemeralOwnerToPaths.containsKey(ephemeralOwner)) {
                        this.ephemeralOwnerToPaths.put(ephemeralOwner, new HashSet<>());
                    }
                    this.ephemeralOwnerToPaths.get(ephemeralOwner).add(filepath.toString());
                    this.ephemeralOwnerCount.merge(ephemeralOwner, 1, Integer::sum);
//                    if (this.ephemeralOwnerCount.containsKey(ephemeralOwner)) {
//                        this.ephemeralOwnerCount.merge(ephemeralOwner, 1, Integer::sum);
//                    } else {
//                        this.ephemeralOwnerCount.put(ephemeralOwner, 1);
//                    }
//                this.
                    // TODO: Handle ephemeral
//                if (type.equals(LogRecord.LogRecordType.CREATE_EPHEMERAL)) {
//                    manager.ephemeralOwnersMap.put(path.toString(), managerId);
//                }
                    // NOTE: No break
//                    log.warn(String.format("\t\tREMOVE ME - CREATE_EPHEMERAL " +
//                                    " [ephemeralOwnersMap=%s,\n\t ephemeralOwnerCount=%s]",
//                            ephemeralOwnersMap, ephemeralOwnerCount));
                case CREATE:
                    if (DebugConstants.DEBUG_FLAG) {
                        log.debug(String.format("updateMetadataStore - CREATE - [path=%s]", filepath));
                    }
                    if (this.pathChildrenMap.containsKey(filepath.toString())) {
                        // TODO: throw exception; extra create
                        System.out.println(String.format("REMOVE ME - DEBUG - Double create [path=%s]",
                                filepath.toString()));
                        throw new RuntimeException(String.format("Invalid log state - Double create [path=%s]",
                                filepath.toString()));
                    }
//                Path filepath = Paths.get(logRecord.getPath());
//                String filename = filepath.getFileName().toString();
//                String parentPathStr = filepath.getParent().toString();
//                long timestamp = this.getRecordTimestamp(filepath.toString());
//                int transactionId = this.getRecordTransactionId(filepath.toString());
                    stats = new LogRecordStats(transactionId, transactionId, transactionId,
                            timestamp, timestamp, 0, 0, 0, 0, logRecord.getData().length, ephemeralOwner);
                    // NOTE: Manage stats
                    this.pathStatsMap.put(filepath.toString(), stats);
                    // Update parents
                    stats = this.pathStatsMap.get(parentPathStr);
                    stats.incrementChildrenVersion();
                    stats.setChildrenListMutationTransactionId(transactionId);
                    stats.incrementChildCount();

                    // NOTE: Add empty hashset
                    this.pathChildrenMap.put(filepath.toString(), new HashSet<>());
                    // NOTE: Add filename to parentpath child set. Should be guaranteed to be in the map.
                    this.pathChildrenMap.get(parentPathStr).add(filename);
                    // TODO: REMOVE ME REMOVEME
//                    log.warn(String.format("\t\tREMOVE ME - retrieveChildren " +
//                                    "- children added [path=%s,\n\t children=%s]",
//                            filepath, this.pathChildrenMap.get(parentPathStr)));
                    break;
//                case CREATE_ROOT:
//                    if (DebugConstants.DEBUG_FLAG) {
//                        System.out.println(String.format("REMOVE ME - updateMetadataStore " +
//                                "- CREATE_ROOT - [path=%s]", filepath));
//                    }
//                    if (this.pathChildrenMap.containsKey(filepath.toString())) {
//                        // TODO: throw exception; extra create
//                        System.out.println(String.format("REMOVE ME - DEBUG - Double create [path=%s]",
//                                filepath.toString()));
//                        throw new RuntimeException(String.format("Invalid log state - Double create [path=%s]",
//                                filepath.toString()));
//                    }
////                Path filepath = Paths.get(logRecord.getPath());
////                String filename = filepath.getFileName().toString();
////                String parentPathStr = filepath.getParent().toString();
////                long timestamp = this.getRecordTimestamp(filepath.toString());
////                int transactionId = this.getRecordTransactionId(filepath.toString());
//                    stats = new LogRecordStats(transactionId, transactionId, transactionId,
//                            timestamp, timestamp, 0, 0, 0, 0, logRecord.getData().length, ephemeralOwner);
//                    // NOTE: Manage stats
//                    this.pathStatsMap.put(filepath.toString(), stats);
//
//                    // NOTE: Add empty hashset
//                    this.pathChildrenMap.put(filepath.toString(), new HashSet<>());
//                    break;
//            case CREATE_EPHEMERAL:
//                // TODO: consider merging the creates
//                stats = new LogRecordStats(transactionId, transactionId, transactionId,
//                        timestamp, timestamp, 0, 0, logRecord.getManagerId());
//                // NOTE: Manage stats
//                this.pathStatsMap.put(filepath.toString(), stats);
//                // Update parents
//                stats = this.pathStatsMap.get(parentPathStr);
//                stats.incrementChildrenVersion();
//                stats.setChildrenListMutationTransactionId(transactionId);
//
//                // NOTE: Add empty hashset
//                this.pathChildrenMap.put(filepath.toString(), new HashSet<>());
//                // NOTE: Add filename to parentpath child set. Should be guaranteed to be in the map.
//                this.pathChildrenMap.get(parentPathStr).add(filename);
//
//                // NOTE: manage ephemeral struct
//                this.ephemeralOwnersMap.put(filepath.toString(), logRecord.getManagerId());
//                break;
                case DELETE:
//                Path filepath = Paths.get(logRecord.getPath());
//                String filename = filepath.getFileName().toString();
//                String parentPathStr = filepath.getParent().toString();
//                long timestamp = this.getRecordTimestamp(filepath.toString());
//                int transactionId = this.getRecordTransactionId(filepath.toString());
                    if (Objects.isNull(this.pathChildrenMap.remove(filepath.toString()))) {
                        // TODO: throw exception; extra delete
                        System.out.println(String.format("REMOVE ME - DEBUG - Missing node [path=%s]", filepath));
                        throw new RuntimeException("Invalid log state - Delete on missing path");
                    }
                    this.pathChildrenMap.get(parentPathStr).remove(filename);

                    // NOTE: Check ephemerals
                    ephemeralOwner = this.ephemeralOwnersMap.remove(filepath.toString());
                    if (!Objects.isNull(ephemeralOwner)) {
                        // NOTE: Should never be null, since there shouldn't be a delete before create
                        this.ephemeralOwnerToPaths.get(ephemeralOwner).remove(filepath.toString());
                        Integer newValue = this.ephemeralOwnerCount.merge(ephemeralOwner, -1, Integer::sum);
                        if (Objects.isNull(newValue) || newValue < 0) {
                            // TODO: throw new exception; extra delete
                            System.out.println(String.format("REMOVE ME - DEBUG " +
                                            "- updateMetadataStore - Null Ephemeral Owner [path=%s, ephemeralOwner=%s]",
                                            filepath, ephemeralOwner.toString()));
                        } else if (newValue == 0) {
                            // TODO: remove owner
                            this.ephemeralOwnerCount.remove(ephemeralOwner);
                            this.ephemeralOwnerToPaths.remove(ephemeralOwner);
                        }
//                    } else if (newValue < 0 ) {
//                        // TODO: throw new exception; extra delete; shouldn't be reachable once delete is in
//                        System.out.println(String.format("REMOVE ME - DEBUG " +
//                                "- updateMetadataStore - Below 0 Ephemeral Owner [path=%s, ephemeralOwner=%s]",
//                                filepath, ephemeralOwner.toString()));
//                    }
                    }

                    // NOTE: Manage stats
                    this.pathStatsMap.remove(filepath.toString());
                    // Update parents
                    stats = this.pathStatsMap.get(parentPathStr);
                    stats.incrementChildrenVersion();
                    stats.setChildrenListMutationTransactionId(transactionId);
                    stats.decrementChildCount();
                    break;
                case CONNECT:
                    break;
                // TODO: add disconnect? clear ephemeral nodes. How to clean up without standard disconnect, ttl?
                default:
            }
        }
    }

    //TODO: consider making abstract class and remove initialize methods from public scope
    abstract boolean initializeUniqueId() throws InvalidServiceStateException;

    public abstract ManagerLeaseHandlerBase startLeaseHandler();

    public void startManager() throws InvalidServiceStateException, IOException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    @Override
    public void close() throws InterruptedException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    @Override
    public boolean claimSoftLease() throws InvalidServiceStateException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    @Override
    public boolean claimSoftLease(boolean waitFlag) throws InvalidServiceStateException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    public Future<ManagerLeaseHandlerBase> claimSoftLeaseHandler() {
        return claimSoftLeaseHandler(false);
    }

    @Override
    public Future<ManagerLeaseHandlerBase> claimSoftLeaseHandler(boolean highPriorityFlag) {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    public boolean releaseSoftLease() {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    public boolean extendSoftLease() throws InvalidServiceStateException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    public boolean extendSoftLease(Connection conn) throws InvalidServiceStateException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    public void enableCacheLayer(CoordinationSvcCacheLayer<String, LogRecord> cache) {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    public LogRecord retrieveFromCache(String path, boolean statsFlag,
                                       boolean readThroughFlag) throws InvalidServiceStateException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    public LogRecord retrieveFromLog(String path, boolean getStatsFlag,
                                 WatchConsumerWrapper<WatchInput> watchCallable) throws InvalidServiceStateException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }


    public CommandOutput retrieveChildren(String path, boolean statsFlag,
                                      WatchConsumerWrapper<WatchInput> watchCallable,
                                      boolean blockingFlag, boolean syncLogFlag) throws InvalidServiceStateException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    public boolean addWatch(String path,
                            WatchConsumerWrapper<WatchInput> watchCallable) throws InvalidServiceStateException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    public boolean removeWatch(String path, WatchedEvent.WatchType type) throws InvalidServiceStateException {
        // NOTE: Not implemented
        throw new NotImplementedException();
    }

    public String getRootNode() {
        return cSvcNodeRoot;
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    public boolean isRunning() {
        return isRunning;
    }

    // TODO: REMOVE ME REMOVEME for testing purposes only
    //  CONSIDER MAKING PROTECTED/PRIVATE and used in test?
    public void setLeaseMultiplier(int leaseMultiplier) {
        throw new NotImplementedException();
    }

        // TODO: consider if getrecordtimestamp & getrecordtransactionId are needed
//    protected abstract long getRecordTimestamp(String key);
//
//    protected abstract int getRecordTransactionId(String key);

    // ==
    // TODO: Clean up outdated APIs
    // ==
    public CommandOutput create(CreateCommand cmd) {
        throw new NotImplementedException();
    }

    public CommandOutput delete(DeleteCommand cmd) {
        throw new NotImplementedException();
    }

    public CommandOutput get(GetCommand cmd) {
        throw new NotImplementedException();
    }
}
