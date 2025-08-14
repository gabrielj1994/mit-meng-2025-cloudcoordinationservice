package dsg.ccsvc.datalayer;

import dsg.ccsvc.*;
import dsg.ccsvc.caching.CoordinationSvcCacheLayer;
import dsg.ccsvc.command.*;
import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.log.LogRecordFSHeader;
import dsg.ccsvc.log.LogRecordStats;
import dsg.ccsvc.util.CSvcUtils;
import dsg.ccsvc.util.DebugConstants;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.time.StopWatch;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static dsg.ccsvc.log.LogRecord.LogRecordType.*;
import static dsg.ccsvc.log.LogRecordFSHeader.HEADER_SIZE;
import static dsg.ccsvc.util.CSvcUtils.tbdCustomFileExists;

@Slf4j
public class DataLayerMgrSharedFS extends DataLayerMgrBase implements AutoCloseable {

//    long managerId;
    long currentReadOffset;
    // TODO: Consider if there is a better design
    int leaseClaimTicket = -1;
    ArrayList<Path> leaseQueueCache;
    int maintenanceRoleTicket = -1;
    ArrayList<Path> maintenanceRoleQueueCache;
//    int tbdFSLeaseThreshold = 100;
    int tbdFSLeaseThreshold = 50;
    // TODO: Consider moving one of the multipliers to base
//    int tbdLeaseMultiplier = 1;
    int tbdFSHeartbeatMultiplier = 50;
    int recoveryAttemptCounter = 0;
    int readRecoveryAttemptCounter = 0;
    long readValidationTimestamp = -1;
    int watchTxIdWatermark = 0;
    int writeTxIdWatermark = 0;
    long lastWriteOffset = 0;
    int backgroundExecutorPeriodicity = 127;
    AtomicInteger readFailureCounter;
    AtomicBoolean recoveryRequiredFlag;
    AtomicInteger currentReadSegmentIndex;
    AtomicInteger currentWriteSegmentIndex;
    String workDirPathStr;
    String logSegmentPrefixStr;
    Path logDirPath;
    Path logPath;
    Path logSizePath;
    Path logOffsetEntryFilePath;
    Path logTempPath;
    Path logTempRolloverPath;
    Path logWritePath;
    Path logWriteHolderPath;
    Path logReadPath;
    Path logRecoveryWritePath;
    Path logRecoveryReadPath;
//    Path recoveryLeasePath;
    Path recoveryLeasePath;
    Path leasePath;
    Path leaseDirPath;
    // TODO: REMOVE ME
//    Path leaseDirPathTest;
    Path leaseClaimDirPath;
    Path leasePriorityClaimDirPath;
    Path maintenanceRoleDirPath;
    // TODO: REMOVE UNUSED PATHS
    Path watchDirPath;
    Path timestampDirPath;
    Path timestampFilePath;
    // TODO: REMOVE UNUSED PATHS
    Path timestampEntryFilePath;
    Path heartbeatDirPath;
    Path heartbeatFilePath;
    Path timeoutDirPath;
    // TODO: consider other options for executing watches
    ExecutorService watchExecutor;
    // TODO: rename this properly
    ExecutorService backgroundLeaseClaimer;
    ExecutorService backgroundLeaseExtender;
    ScheduledExecutorService backgroundLogReader;
    ScheduledExecutorService backgroundTasksHandler;
    final Object watchStructMonitor = new Object();

    // TODO: Implement proper UUID generation (or equivalent as needed)
    AtomicInteger logCounter = new AtomicInteger(0);
    final Object tbdLogReadMonitor = new Object();
    ArrayList<FSReadWaitWrapper> tbdLogReadWaitQueue;
    Integer sessionCounter = 0;
    Properties properties;
    boolean cacheFlag = false;
//    ConcurrentHashMap<String, Long> tbdEphemeralPendingDeletes = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Future<?>> tbdEphemeralPendingDeletes = new ConcurrentHashMap<>();
//    CoordinationSvcCacheLayer<String, LogRecord> recordDataCache;
    ConcurrentHashMap<String, FSInmemoryInfo> tbdPathToInfo;
    ConcurrentHashMap<Path, MappedByteBuffer> tbdPathToBuffer;
    static final int TIMESTAMP_BYTE_OFFSET = 0;
    static final int CONTENT_BYTE_OFFSET = Long.BYTES;
    static final long NONEPHEMERAL_OWNER_VALUE = 0;
    static final long RECOVERY_COUNT_THRESHOLD = 10;
    static final long READ_FAILURE_THRESHOLD = 10;
    static final long READ_LOG_TS_DELTA_THRESHOLD = 5000;
    static final long WRITE_LOG_TS_DELTA_THRESHOLD = 2500;
    static final long RECOVERY_TS_DELTA_THRESHOLD = 2500;
    static final String USERNAME = "csvcadmin";
    static final String PASSWORD = "!123Csvc";
    static final String LOG_FILENAME = "csvc.log";
    static final String LOG_SIZE_FILENAME = LOG_FILENAME.concat(".size");
    static final String LOG_OFFSET_FILENAME = LOG_FILENAME.concat(".offset");
    static final String LOG_TEMP_FILENAME = LOG_FILENAME.concat(".temp");
    static final String LOG_WRITE_FILENAME = LOG_FILENAME.concat(".write");
    static final String LOG_READ_FILENAME = LOG_FILENAME.concat(".read");
    static final String LOG_RECOVERY_SUFFIX = ".recovery";
    static final String LOG_ROLLOVER_SUFFIX = ".rollover";
//    static final String LOG_RECOVERY_FILENAME = LOG_FILENAME.concat(".recovery");
    static final String LOG_SEGMENT_PREFIX = LOG_FILENAME.concat(".segment_");
//    static final String TXID_FILENAME = ".txid";
    static final String LEASE_FILENAME = "csvc.lease";
    static final String LOG_DIR_NAME = "log_dir";
    static final String LEASE_DIR_NAME = "lease_dir";
    static final String LEASE_CLAIM_DIR_NAME = "claims_dir";
    static final String LEASE_PRIORITY_CLAIM_DIR_NAME = "priority_claims_dir";
    static final String WATCH_DIR_NAME = "watch_dir";
    static final String TIMESTAMP_DIR_NAME = "timestamp_dir";
    static final String ENTRY_TIMESTAMP_FILENAME = "entry_ts.dat";
    static final String HEARTBEAT_DIR_NAME = "heartbeat_dir";
    static final String TIMEOUT_DIR_NAME = "timeout_dir";
    static final String MAINTENANCE_DIR_NAME = "maintenance_dir";
    static final String WATCH_CHILDREN_SUBDIR = "/children";
    static final String WATCH_DATA_SUBDIR = "/data";
    static final String WATCH_EXISTS_SUBDIR = "/exists";
    // NOTE: 1024 * 1024 * 32 | (32 MBytes)
    public static final int LOG_SIZE_THRESHOLD = 1024*1024*32;

    public DataLayerMgrSharedFS(String workDirPath, String cSvcNodeRoot) {
        this.cSvcNodeRoot = cSvcNodeRoot;
        this.workDirPathStr = workDirPath;
        this.logDirPath = Paths.get(this.workDirPathStr, LOG_DIR_NAME);
        this.logPath = this.logDirPath.resolve(LOG_FILENAME);
        this.logSizePath = this.logDirPath.resolve(LOG_SIZE_FILENAME);
        this.logOffsetEntryFilePath = this.logDirPath.resolve(LOG_OFFSET_FILENAME);
        this.logTempPath = this.logDirPath.resolve(LOG_TEMP_FILENAME);
        this.logTempRolloverPath = Paths.get(logTempPath.toString().concat(LOG_ROLLOVER_SUFFIX));
        this.logWritePath = this.logDirPath.resolve(LOG_WRITE_FILENAME);
        this.logWriteHolderPath = this.logDirPath.resolve(LOG_WRITE_FILENAME.concat(".holder"));
        this.logReadPath = this.logDirPath.resolve(LOG_READ_FILENAME);
//        this.logRecoveryReadPath = this.logDirPath.resolve(LOG_RECOVERY_FILENAME);
        this.logRecoveryReadPath = Paths.get(this.logReadPath.toString().concat(LOG_RECOVERY_SUFFIX));
        this.logRecoveryWritePath = Paths.get(this.logWritePath.toString().concat(LOG_RECOVERY_SUFFIX));
//        this.recoveryLeasePath = this.logDirPath.resolve(LOG_RECOVERY_LEASE_FILENAME);
        this.maintenanceRoleDirPath = Paths.get(this.workDirPathStr, MAINTENANCE_DIR_NAME);
        this.leaseDirPath = Paths.get(this.workDirPathStr, LEASE_DIR_NAME);
//        this.leaseDirPathTest = Paths.get(this.workDirPathStr, LEASE_DIR_NAME.concat("_TEST"));
        this.leasePath = this.leaseDirPath.resolve(LEASE_FILENAME);
        this.recoveryLeasePath = this.leaseDirPath.resolve(LEASE_FILENAME.concat(LOG_RECOVERY_SUFFIX));
//        this.leasePath = this.leaseDirPathTest.resolve(LEASE_FILENAME);
        this.leaseClaimDirPath = this.leaseDirPath.resolve(LEASE_CLAIM_DIR_NAME);
        this.leasePriorityClaimDirPath = this.leaseClaimDirPath.resolve(LEASE_PRIORITY_CLAIM_DIR_NAME);
        this.watchDirPath = Paths.get(this.workDirPathStr, WATCH_DIR_NAME);
        this.timestampDirPath = Paths.get(this.workDirPathStr, TIMESTAMP_DIR_NAME);
        this.timestampEntryFilePath = this.timestampDirPath.resolve(ENTRY_TIMESTAMP_FILENAME);
        this.heartbeatDirPath = Paths.get(this.workDirPathStr, HEARTBEAT_DIR_NAME);
        this.timeoutDirPath = Paths.get(this.workDirPathStr, TIMEOUT_DIR_NAME);
        this.logSegmentPrefixStr = this.logDirPath.resolve(LOG_SEGMENT_PREFIX).toString();
        this.currentReadOffset = 0;
        this.readFailureCounter = new AtomicInteger(0);
        this.currentReadSegmentIndex = new AtomicInteger(0);
        this.currentWriteSegmentIndex = new AtomicInteger(0);
        this.recoveryRequiredFlag = new AtomicBoolean(false);
    }

    // TODO: testing. remove this
    public Properties getProperties() {
        return properties;
    }

    public long getManagerId() {
        return managerId;
    }

    public void enableCacheLayer(CoordinationSvcCacheLayer<String, LogRecord> cache) {
        recordDataCache = cache;
        cacheFlag = true;
    }

    @Override
    public void setLeaseMultiplier(int leaseMultiplier) {
        if (leaseMultiplier > 0) {
            this.tbdLeaseMultiplier = leaseMultiplier;
            this.tbdFSHeartbeatMultiplier = Math.max(tbdFSHeartbeatMultiplier, tbdLeaseMultiplier * 3);
        }
    }

    private void clearCacheLayer() {
        if (cacheFlag) {
            cacheFlag = false;
            recordDataCache.clear();
        }
    }

    private void clearMappedBuffers() {
//        tbdPathToBuffer.forEach((path, buffer) -> {
//            log.info(String.format("\t\tREMOVEME - clearMappedBuffers [path=%s]", path));
//            try {
//                Method cleanerMethod = buffer.getClass().getMethod("cleaner");
//                cleanerMethod.setAccessible(true);
//                Object cleaner = cleanerMethod.invoke(buffer);
//                if (cleaner != null) {
//                    Method cleanMethod = cleaner.getClass().getMethod("clean");
//                    cleanMethod.invoke(cleaner);
//                }
//            } catch (Exception e) {
//                log.error(String.format("Failed to clean MappedByteBuffer [path=%s]", path), e);
//            }
//        });
//        tbdPathToBuffer.forEach((path, buffer) -> CSvcUtils.unmapMappedBuffer(buffer));
        // TODO: REMOVEME REMOVE ME CLEAN THIS UP
//        for (Path filePath : tbdPathToBuffer.keySet()) {
//            MappedByteBuffer mappedBuffer = tbdPathToBuffer.get(filePath);
//            tbdPathToBuffer.remove(filePath);
//            CSvcUtils.unmapMappedBuffer(mappedBuffer);
//        }
        tbdPathToBuffer.clear();
    }

    private void clearReadWaits() {
        for (FSReadWaitWrapper waitingRead : tbdLogReadWaitQueue) {
            synchronized (waitingRead.getMonitorObject()) {
                waitingRead.getMonitorObject().notifyAll();
            }
        }
        tbdLogReadWaitQueue.clear();
    }

    private void initiateTermination() {
        if (isRunning) {
            new Thread(() -> {
                while (true) {
                    try {
                        close();
                        break;
                    } catch (InterruptedException e) {
                        // No-Op
                        Thread.currentThread().interrupt();
                    }
                }
            }).start();
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (isInitialized) {
            log.info(String.format("Closing Manager [manager_id=%d]", managerId));
//            isRunning = false;
            disableRunningFlag();
            clearCacheLayer();
            clearReadWaits();
            clearMappedBuffers();
            backgroundLogReader.shutdown();
            watchExecutor.shutdown();
            backgroundLeaseClaimer.shutdown();
            backgroundLeaseExtender.shutdown();
            backgroundTasksHandler.shutdown();
            try {
                if (!backgroundLogReader.awaitTermination(20, TimeUnit.MILLISECONDS)) {
                    backgroundLogReader.shutdownNow();
                }
                if (!watchExecutor.awaitTermination(20, TimeUnit.MILLISECONDS)) {
                    watchExecutor.shutdownNow();
                }
                if (!backgroundLeaseClaimer.awaitTermination(20, TimeUnit.MILLISECONDS)) {
                    backgroundLeaseClaimer.shutdownNow();
                }
                if (!backgroundLeaseExtender.awaitTermination(20, TimeUnit.MILLISECONDS)) {
                    backgroundLeaseExtender.shutdownNow();
                }
                if (!backgroundTasksHandler.awaitTermination(20, TimeUnit.MILLISECONDS)) {
                    backgroundTasksHandler.shutdownNow();
                }
            } catch (InterruptedException e) {
                backgroundLogReader.shutdownNow();
                watchExecutor.shutdownNow();
                backgroundLeaseClaimer.shutdownNow();
                backgroundLeaseExtender.shutdownNow();
                backgroundTasksHandler.shutdownNow();
                throw e;
            } finally {
                isInitialized = false;
                // NOTE: Suggest GC take place
//                System.gc();
            }
        }
    }

    private void disableRunningFlag() {
        this.isRunning = false;
    }

    public boolean isRunning() {
        return this.isRunning;
    }

    private void tbdDeleteRetryWrapper(Path directoryPath) {
        CSvcUtils.retryOperation(() -> {
                    try {
                        deleteDirectories(directoryPath);
                    } catch (FileSystemException e) {
                        System.gc();
                    }
                },
                (Exception e) -> {
                    log.warn(String.format("tbdDeleteRetryWrapper " +
                            "- Failed to delete directories [path=%s]\n", directoryPath));
                    log.debug("tbdDeleteRetryWrapper - Exception:", e);
                }, 10);
//        int retryCount = 0;
//        int retryMax = 10;
//        boolean deleteFlag = false;
//        while (!deleteFlag) {
//            try {
//                retryCount++;
//                deleteDirectories(directoryPath);
//                deleteFlag = true;
//            } catch (NoSuchFileException e) {
//                break;
//            } catch (IOException e) {
//                // No-Op
//                if (retryCount >= retryMax) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
    }

    private void deleteDirectories(Path directoryPath) throws IOException {
        // Walk the file tree and delete entries in reverse order
        Files.walkFileTree(directoryPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file); // Delete each file
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir); // Delete the directory itself
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public void tbdFSCleanup() {
        log.error("REMOVE ME - tbdFSCleanup");
        tbdDeleteRetryWrapper(Paths.get(workDirPathStr));
//        tbdDeleteRetryWrapper(logDirPath);
//        tbdDeleteRetryWrapper(leaseDirPath);
//        tbdDeleteRetryWrapper(maintenanceRoleDirPath);
//        tbdDeleteRetryWrapper(heartbeatDirPath);
//        tbdDeleteRetryWrapper(timeoutDirPath);
//        tbdDeleteRetryWrapper(watchDirPath);
//        tbdDeleteRetryWrapper(Paths.get(watchDirPath.toString().concat(WATCH_CHILDREN_SUBDIR)));
//        tbdDeleteRetryWrapper(Paths.get(watchDirPath.toString().concat(WATCH_DATA_SUBDIR)));
//        tbdDeleteRetryWrapper(Paths.get(watchDirPath.toString().concat(WATCH_EXISTS_SUBDIR)));
    }

    @Override
    void initializeMetadata(String cSvcNodeRoot) {
        super.initializeMetadata(cSvcNodeRoot);
        // === FS METADATA
        tbdPathToBuffer = new ConcurrentHashMap<>();
        tbdPathToInfo = new ConcurrentHashMap<>();
        tbdPathToInfo.put(cSvcNodeRoot, FSInmemoryInfo.builder().transactionId(1)
                                                                .logSegment(0).headerStartOffset(0)
                                                                .build());
        tbdLogReadWaitQueue = new ArrayList<>();
    }

    private void updateMetadataStore(LogRecord logRecord, long timestamp, int transactionId,
                                     int currentSegmentIndex, long currentReadOffset) {
        if (DebugConstants.DEBUG_FLAG) {
            log.warn(String.format("DEBUG - updateMetadataStore " +
                    "- [tx_id=%d,\nlog_record=\n%s]", transactionId, logRecord));
        }
        updateMetadataStore(logRecord, timestamp, transactionId);
        if (logRecord.getType().equals(DELETE)) {
            tbdPathToInfo.remove(logRecord.getPath());
        } else {
            tbdPathToInfo.put(logRecord.getPath(), FSInmemoryInfo.builder().transactionId(transactionId)
                    .logSegment(currentSegmentIndex)
                    .headerStartOffset(currentReadOffset).build());
        }
    }

    @Override
    public void updateMetadataStore(LogRecord logRecord, long timestamp, int transactionId) {
        super.updateMetadataStore(logRecord, timestamp, transactionId);
        // TODO: Consider moving cache managing to base abstract class
        if (cacheFlag) {
            switch (Objects.requireNonNull(logRecord.getType())) {
                case SET:
                case CREATE_EPHEMERAL:
                case CREATE:
                    recordDataCache.put(logRecord.getPath(), logRecord);
                    break;
                case DELETE:
                    recordDataCache.remove(logRecord.getPath());
                    break;
                case CONNECT:
                    break;
                default:
            }
        }
    }

//    public ManagerLeaseHandlerBase submitLeaseClaim() throws InterruptedException, ExecutionException {
//        boolean leaseClaim = false;
//        while (!leaseClaim) {
////            try {
//                leaseClaim = submitSoftLeaseClaim().get();
////            } catch (RuntimeException e) {
////                throw e;
////            }
//        }
//
//    }

    @Override
    public ManagerLeaseHandlerBase startLeaseHandler() {
        return new FSLeaseHandler(this);
    }

    private void initialize() throws IOException, InvalidServiceStateException {
        if (isInitialized) {
            log.warn(String.format("initialize - Manager already initialized [manager_id=%d]", managerId));
            return;
        }
        int periodicitySalt = (int)(Math.random()*17);
        initializeDirectories();
        initializeMetadata(cSvcNodeRoot);
        initializeUniqueId();
        // TODO: Consider best placement for maintenance role ticket add
        tbdFSAddMaintenanceRoleTicket();
        backgroundLogReader = Executors.newSingleThreadScheduledExecutor();
        watchExecutor = Executors.newSingleThreadExecutor();
        backgroundLeaseExtender = Executors.newSingleThreadExecutor();
//        backgroundLeaseClaimer = Executors.newSingleThreadExecutor();
        backgroundLeaseClaimer = new ThreadPoolExecutor(1, 1, 0L,
                        TimeUnit.MILLISECONDS, new PriorityBlockingQueue<>());
        backgroundTasksHandler = Executors.newSingleThreadScheduledExecutor();
        // TODO: Maintenance Runnable
        tbdInitializeLogFileVersion();
        TbdFSLogReader logReaderFSRunnable = new TbdFSLogReader(this);
        FSMaintenanceRunnable maintenanceRunnable = new FSMaintenanceRunnable(this);

        backgroundLogReader.scheduleAtFixedRate(
                logReaderFSRunnable, 50, 30+periodicitySalt, TimeUnit.MILLISECONDS);
        try {
            if (tbdFSEvaluateMaintenanceRole()) {
                log.info(String.format("initialize - Gained Maintenance Role [manager_id=%d]", managerId));
                tbdFSManageEphemerals();
            }
        } catch (InvalidServiceStateException e) {
            // TODO: Consider error out?
            System.out.println(String.format("FSInitialize - Unexpected Exception"));
            e.printStackTrace(System.out);
            throw e;
        }

//        if (DebugConstants.LOGREADER_FLAG) {
        backgroundTasksHandler.scheduleAtFixedRate(
                maintenanceRunnable, 50, backgroundExecutorPeriodicity, TimeUnit.MILLISECONDS);
//        }

        isInitialized = true;
//        System.out.println("Post Initialize Active Threads:");
//        for (Thread t : Thread.getAllStackTraces().keySet()) {
//            System.out.println("Thread Name: " + t.getName() + ", State: " + t.getState());
//        }
    }

    private void createDirectories(Path dirPath) throws IOException {
        try {
            Files.createDirectories(dirPath);
        } catch (FileAlreadyExistsException e) {
            // No-op
        } catch (IOException e) {
            log.error(String.format("Failed to create directory [logpath=%s]", dirPath));
            throw e;
        }
    }

    private void initializeDirectories() throws IOException {
        createDirectories(logDirPath);
        createDirectories(leaseDirPath);
//        createDirectories(leaseDirPathTest);
        createDirectories(leaseClaimDirPath);
        createDirectories(leasePriorityClaimDirPath);
        createDirectories(timestampDirPath);
        createDirectories(maintenanceRoleDirPath);
        createDirectories(heartbeatDirPath);
        createDirectories(timeoutDirPath);
        createDirectories(watchDirPath);
        createDirectories(watchDirPath.resolve(WATCH_CHILDREN_SUBDIR));
        createDirectories(watchDirPath.resolve(WATCH_DATA_SUBDIR));
        createDirectories(watchDirPath.resolve(WATCH_EXISTS_SUBDIR));
    }

    private void tbdInitializeLogFileVersion() throws InvalidServiceStateException {
        // NOTE: Read existing log file, starting from oldest segment.
        //   Must check if log or log_temp file exists (can be mid write)
        //   If log does not exist, create log
        CSvcUtils.retryCheckedOperation(() -> {
                    boolean existingLogFlag = tbdCheckLogExists();
                    if (existingLogFlag) {
                        tbdConsumeEntries(false);
                    } else {
                        tbdCreateLog();
                    }
                }, (Exception e) -> log.error("CRITICAL ERROR - InitializeLog " +
                        "- Failed to initialize or consume log", e),
                10);
    }

    private int tbdEvaluateTxId_ARCHIVED() throws InvalidServiceStateException {
        int txid = 0;
        try (RandomAccessFile ignored = new RandomAccessFile(logTempPath.toFile(), "r");
             FileChannel fileChannel = ignored.getChannel()) {
            long offset = currentReadOffset;
            if (offset + HEADER_SIZE >= fileChannel.size()) {
                txid = logCounter.get() + 1;
                log.debug(String.format("tbdEvaluateTxId - Detecting rolled over log, using log counter [txid=%d]", txid));
            } else {
                ByteBuffer hdrBuffer;
                hdrBuffer = ByteBuffer.allocate(HEADER_SIZE);
                fileChannel.position(offset);
                LogRecordFSHeader header;
                while (fileChannel.position() + HEADER_SIZE >= fileChannel.size()) {
                    if (fileChannel.read(hdrBuffer) <= 0) {
                        // TODO: Error out
                        throw new InvalidServiceStateException("CRITICAL ERROR - tbdEvaluateTxId " +
                                "- Failed to read record header");
                    }

                    header = SerializationUtils.deserialize(hdrBuffer.array());
                    txid = header.getTransactionId() + 1;
                    fileChannel.position(fileChannel.position() + header.getLogEntryByteSize());
                }
            }
        } catch (FileNotFoundException e) {
            // TODO: Fix error handling
            // TODO: FNFE means logtemppath wasn't set up ahead of time
            throw new InvalidServiceStateException(e);
        } catch (IOException e) {
            throw new InvalidServiceStateException(e);
        } catch (Exception e) {
            log.error("tbdEvaluateTxId - Unexpected Exception", e);
            throw new InvalidServiceStateException(e);
        }

        if (txid == 0) {
            throw new InvalidServiceStateException("tbdEvaluateTxId - Error evaluating valid transaction id");
        }

        return txid;
    }

    private int tbdEvaluateTxId() throws InvalidServiceStateException {
        // NOTE: Check for rollover since last evaluation
        // int writeTxIdWatermark = 0;
        // long lastWriteOffset = 0;
        // NOTE: Use logTempPath
        int txid = 0;
        try (RandomAccessFile ignored = new RandomAccessFile(logTempPath.toFile(), "r");
             FileChannel fileChannel = ignored.getChannel()) {
            long offset = currentReadOffset;
            if (HEADER_SIZE >= fileChannel.size() || offset == fileChannel.size()) {
                txid = logCounter.get() + 1;
                if (HEADER_SIZE >= fileChannel.size()) {
                    log.info(String.format("tbdEvaluateTxId - Detecting rolled over log, using log counter [txid=%d]",
                            txid));
                } else {
                    log.debug(String.format("tbdEvaluateTxId - In memory state up to date with log," +
                                    " using log counter [txid=%d]", txid));
                }
            } else if (offset + HEADER_SIZE < fileChannel.size()) {
                log.debug(String.format("tbdEvaluateTxId - Log Reader behind current log, looking ahead"));
                ByteBuffer hdrBuffer;
                hdrBuffer = ByteBuffer.allocate(HEADER_SIZE);
                fileChannel.position(offset);
                LogRecordFSHeader header;
                while (fileChannel.position() + HEADER_SIZE >= fileChannel.size()) {
                    if (fileChannel.read(hdrBuffer) <= 0) {
                        // TODO: Error out
                        throw new InvalidServiceStateException("CRITICAL ERROR - tbdEvaluateTxId " +
                                "- Failed to read record header");
                    }

                    header = SerializationUtils.deserialize(hdrBuffer.array());
                    txid = header.getTransactionId() + 1;
                    fileChannel.position(fileChannel.position() + header.getLogEntryByteSize());
                }
            } else {
                throw new InvalidServiceStateException(String.format("CRITICAL ERROR - tbdEvaluateTxId " +
                                "- Invalid log temp file size [current_read_offset=%d, log_size=%d]",
                        offset, fileChannel.size()));
            }
        } catch (FileNotFoundException e) {
            // TODO: Fix error handling
            // TODO: FNFE means logtemppath wasn't set up ahead of time
            throw new InvalidServiceStateException(e);
        } catch (IOException e) {
            throw new InvalidServiceStateException(e);
        } catch (Exception e) {
            log.error("tbdEvaluateTxId - Unexpected Exception", e);
            throw new InvalidServiceStateException(e);
        }

        if (txid == 0) {
            throw new InvalidServiceStateException("tbdEvaluateTxId - Error evaluating valid transaction id");
        }

        return txid;
    }

    private boolean tbdReadLogRecovery() throws InvalidServiceStateException {
        boolean successFlag = true;
        log.info(String.format("tbdReadLogRecovery - Initiating Read Log Recovery [manager_id=%d]", managerId));
        // NOTE: Ensure read path does not exist
        //       OR read log invalid
        if (!Files.exists(logReadPath) || !tbdFSValidateReadLog()) {
            successFlag = CSvcUtils.retryCheckedOperation(
                    () -> {
                        Files.copy(logPath, logRecoveryReadPath, StandardCopyOption.REPLACE_EXISTING);
                        return true;},
                    (Exception e) -> {
                        log.error("CRITICAL ERROR - tbdReadLogRecovery - Failed to copy log", e);
                        return false;},
                    10, false);
            if (!successFlag) {
                return false;
            }
//            int retryCount = 0;
//            int retryMax = 10;
//            while (retryCount < retryMax) {
//                retryCount++;
//                try {
//                    Files.copy(logPath, logRecoveryPath, StandardCopyOption.REPLACE_EXISTING);
//                    break;
//                } catch (IOException e) {
//                    // NOTE: No-Op
//                    if (retryCount == retryMax) {
////                        if (DebugConstants.DEBUG_FLAG) {
//                        System.out.println(String.format("ERROR - tbdReadLogRecovery - Failed all retries"));
//                        e.printStackTrace(System.out);
////                        }
//                        return false;
//                    }
//                }
//            }
            //==
            successFlag = CSvcUtils.retryCheckedOperation(() -> {
                        Files.move(logRecoveryReadPath, logReadPath,
                                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                        return true;},
                    (Exception e) -> {
                        log.error("CRITICAL ERROR - tbdReadLogRecovery - Failed to replace read log", e);
                        return false;},
                    10, false);
//            retryCount = 0;
//            while (retryCount < retryMax) {
//                retryCount++;
//                try {
//                    Files.move(logRecoveryPath, logReadPath, StandardCopyOption.ATOMIC_MOVE);
//                    break;
//                } catch (IOException e) {
//                    // NOTE: No-Op
//                    if (retryCount == retryMax) {
//                        System.out.println(String.format("ERROR - tbdReadLogRecovery - Failed all retries"));
//                        e.printStackTrace(System.out);
//                        return false;
//                    }
//                }
//            }
        }
        return successFlag;
    }

    private synchronized void resetRecoveryMetadata() {
        recoveryRequiredFlag.set(false);
        recoveryAttemptCounter = 0;
    }

    private boolean evaluateRecoveryState() {
        // TODO: REMOVEME
        log.warn("evaluateRecoveryState - Evaluating recovery state");
        try {
            // TODO: return this to one line
            long currentTime = System.currentTimeMillis();
            // TODO: This can result in issues if entry file path is missing
            long entryTimestamp = tbdFSRetrieveTimestamp(timestampEntryFilePath, false);
            long timeDelta = currentTime - entryTimestamp;
            log.warn(String.format("evaluateRecoveryState - Evaluating recovery state" +
                    " [current_time=%d, entry_timestamp=%d, time_delta=%d]",
                    currentTime, entryTimestamp, timeDelta));
            return timeDelta > WRITE_LOG_TS_DELTA_THRESHOLD;
        } catch (IOException e) {
            // NOTE: Failed to retrieve entry timestamp, attempt recovery
            return true;
        }
    }

    private boolean tbdTempRecoveryHelperStaleBackupCheck(Path recoveryPath) throws InvalidServiceStateException {
        // NOTE: consume entries from read log, only headers and saving txid.
        //  do the same for temp log, and pick the one that has the highest txid
        int readTxId = 0;
        int tempTxId = 0;
        ByteBuffer hdrBuffer = ByteBuffer.allocate(HEADER_SIZE);
        long headerPosition;
        LogRecordFSHeader header = null;
        // NOTE: Read log
        try (RandomAccessFile ignored = new RandomAccessFile(logReadPath.toFile(), "r");
             FileChannel fileChannel = ignored.getChannel()) {
            while (fileChannel.position() + HEADER_SIZE < fileChannel.size()) {
                headerPosition = fileChannel.position();
                hdrBuffer.clear();
                if (fileChannel.read(hdrBuffer) <= 0) {
                    // NOTE: break
                    break;
//                    if (DebugConstants.DEBUG_FLAG) {
//                        log.warn("DEBUG - tbdConsumeEntries - Failed to read record header");
//                    }
//                    throw new InvalidServiceStateException("tbdConsumeEntries " +
//                            "- Failed to read record header");
                }

                header = SerializationUtils.deserialize(hdrBuffer.array());
                fileChannel.position(headerPosition + HEADER_SIZE + header.getLogEntryByteSize());
            }
            if (!Objects.isNull(header)) {
                readTxId = header.getTransactionId();
            }
        } catch (Exception e) {
            throw new InvalidServiceStateException(e);
        }

        try (RandomAccessFile ignored = new RandomAccessFile(recoveryPath.toFile(), "r");
             FileChannel fileChannel = ignored.getChannel()) {
            while (fileChannel.position() + HEADER_SIZE < fileChannel.size()) {
                headerPosition = fileChannel.position();
                hdrBuffer.clear();
                if (fileChannel.read(hdrBuffer) <= 0) {
                    // NOTE: break
                    break;
//                    if (DebugConstants.DEBUG_FLAG) {
//                        log.warn("DEBUG - tbdConsumeEntries - Failed to read record header");
//                    }
//                    throw new InvalidServiceStateException("tbdConsumeEntries " +
//                            "- Failed to read record header");
                }

                header = SerializationUtils.deserialize(hdrBuffer.array());
                fileChannel.position(headerPosition + HEADER_SIZE + header.getLogEntryByteSize());
            }
            if (!Objects.isNull(header)) {
                tempTxId = header.getTransactionId();
            }
        } catch (Exception e) {
            throw new InvalidServiceStateException(e);
        }

        return readTxId > tempTxId;
    }

    private boolean tbdLogRecovery() throws InvalidServiceStateException {
        LogRecordFSHeader header;
//        TbdFSEntry entry;
        ByteBuffer hdrBuffer;
//        ByteBuffer buffer;
        log.info(String.format("tbdLogRecovery - Initiating Log Recovery [manager_id=%d]", managerId));
        // TODO: Consider removing the sleep
        try {
            Thread.sleep(25);
        } catch (InterruptedException e) {
            throw new InvalidServiceStateException(e);
        }
        if (!tbdCustomFileExists(logPath)) {
            // TODO: Check recoveryLeasePath. Check last modified.
            //  OR just let maintenance role handle it
//            if (Files.exists(recoveryLeasePath)) {
//
//            }
            boolean logRolledFlag = false;
            // NOTE: Check for log roll-over failure
            if (Files.exists(logTempRolloverPath)) {
                if (!Files.exists(logTempPath)) {
                    logRolledFlag = true;
                } else {
                    // NOTE: Clean up remaining roll-over backup
                    CSvcUtils.retryOperation(() -> Files.deleteIfExists(logTempRolloverPath),
                            (Exception e) -> log.warn(String.format("tbdLogRecovery " +
                                        "- Failed to clean up roll-over backup [manager_id=%d, backup_path=%s]",
                                    managerId, logTempRolloverPath)), 5);
                }
            }

            log.info(String.format("tbdLogRecovery - No Log Path [manager_id=%d]", managerId));
            if ((Files.exists(logTempPath) || (Files.exists(logTempRolloverPath)) && Files.exists(logReadPath))) {
                // NOTE: Validate temp file using read file
                int readTxID;
                try (RandomAccessFile ignored = new RandomAccessFile(logReadPath.toFile(), "r");
                     FileChannel fileChannel = ignored.getChannel()) {
                    hdrBuffer = ByteBuffer.allocate(HEADER_SIZE);
                    if (fileChannel.read(hdrBuffer) <= 0) {
                        if (DebugConstants.DEBUG_FLAG) {
                            log.error("DEBUG - tbdLogRecovery - Failed to read record header");
                        }
                        throw new InvalidServiceStateException("CRITICAL ERROR - tbdLogRecovery " +
                                "- Failed to read record header");
                    }
                    header = SerializationUtils.deserialize(hdrBuffer.array());
                    readTxID = header.getTransactionId();
                } catch (IOException e) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.error(String.format("DEBUG - tbdLogRecovery - Error"), e);
                    }
                    throw new InvalidServiceStateException(e);
                }

                // NOTE: Validate entry against back-up or most recent log segment
                Path validationPath = logRolledFlag ? Paths.get(logSegmentPrefixStr.concat(
                        String.valueOf(tbdFSUpdateWriteSegmentIndex()-1))) : logTempPath;
                try (RandomAccessFile ignored = new RandomAccessFile(validationPath.toFile(), "r");
                     FileChannel fileChannel = ignored.getChannel()) {
                    hdrBuffer = ByteBuffer.allocate(HEADER_SIZE);
                    if (fileChannel.read(hdrBuffer) <= 0) {
                        if (DebugConstants.DEBUG_FLAG) {
                            log.error("DEBUG - tbdLogRecovery - Failed to read record header");
                        }
                        throw new InvalidServiceStateException("CRITICAL ERROR - tbdLogRecovery " +
                                "- Failed to read record header");
                    }
                    header = SerializationUtils.deserialize(hdrBuffer.array());
                    if (header.getTransactionId() < readTxID) {
                        log.error(String.format("tbdLogRecovery -" +
                                    " Failed to recover log [temp_txid=%d, read_txid=%d]",
                                    header.getTransactionId(), readTxID));
                        return false;
                    }
                } catch (IOException e) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.error(String.format("DEBUG - tbdLogRecovery - Error [manager_id=%d]", managerId));
                    }
                    throw new InvalidServiceStateException(e);
                }

                // NOTE: Validate log entries
                try {
//                    if (!logRolledFlag) {
//                        long readSize = Files.size(logReadPath);
//                        long tempSize = Files.size(logTempPath);
//                        if (readSize > tempSize) {
//                            log.warn(String.format("tbdLogRecovery - Invalid Log Back-Up - Back-Up missing entries" +
//                                    " [read_size=%d, temp_size=%d]", readSize, tempSize));
//                            Files.copy(logReadPath, logTempPath, StandardCopyOption.REPLACE_EXISTING);
//                        }
                    if (tbdTempRecoveryHelperStaleBackupCheck(validationPath)) {
                        log.warn(String.format("tbdLogRecovery - Stale Back Up Log Detected " +
                                "- Falling back on read log"));
                        Files.copy(logReadPath, logTempPath, StandardCopyOption.REPLACE_EXISTING);
                    }
//                    }
                } catch (IOException e) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.error(String.format("DEBUG - tbdLogRecovery - Error [manager_id=%d]", managerId));
                    }
                    throw new InvalidServiceStateException(e);
                }

                // NOTE: Temp validated. Snapshot recovery
                try {
                    Files.copy(logTempPath, logRecoveryWritePath, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
//                    if (DebugConstants.DEBUG_FLAG) {
                    log.error(String.format("DEBUG - tbdLogRecovery " +
                            "- Failed to recover from backup [manager_id=%d]", managerId));
//                    }
                    throw new InvalidServiceStateException(e);
                }

                // NOTE: Consolidate Temp & Read
                try {
                    Files.move(logRecoveryWritePath, logReadPath, StandardCopyOption.ATOMIC_MOVE,
                                StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    // NOTE: Partial recovery
                    log.warn("tbdLogRecovery - Partial Log Recovery - Read File Recovery Failed", e);
                    log.warn("tbdLogRecovery - Resuming Execution");
                }

                // NOTE: Replace log path
                try {
                    Files.move(logTempPath, logPath, StandardCopyOption.ATOMIC_MOVE);
                } catch (IOException e) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.warn(String.format("DEBUG - tbdLogRecovery " +
                                "- Failed to recover from backup [manager_id=%d]", managerId));
                    }
                    throw new InvalidServiceStateException(e);
                }
                log.info("tbdLogRecovery - Successful Log Recovery");
                return true;
            } else {
                log.warn(String.format("tbdLogRecovery - Failed Recovery," +
                                    " Missing Backup or Read File [manager_id=%d]", managerId));
                // TODO: REMOVE ME REMOVEME
                //   Iterate over log directory to check contents
//                logDirPath
                try (DirectoryStream<Path> files = Files.newDirectoryStream(logDirPath)) {
                    for (Path ticketFile : files) {
                        log.warn(String.format("REMOVEME - tbdLogRecovery - Log Dir File [path=%s]", ticketFile));
                    }
                } catch (IOException e) {
//                    throw new RuntimeException(e);
                }
                return false;
            }
        } else if (Files.exists(logTempPath) && tbdCustomFileExists(logPath)) {
            log.info(String.format("tbdLogRecovery - Log Path & Temp [manager_id=%d]", managerId));
            // NOTE: Potential file system error, clean up temp file
            try {
                Files.deleteIfExists(logTempPath);
            } catch (IOException e) {
                if (DebugConstants.DEBUG_FLAG) {
                    log.warn(String.format("DEBUG - tbdLogRecovery - Failed to clean up backup log [manager_id=%d]",
                            managerId));
                }
                throw new InvalidServiceStateException(e);
            }
            return true;
        } else {
            log.info("tbdLogRecovery - No-Op Log Recovery");
            return true;
        }
    }

    // TODO: Consider where method will be used (CMDs)
    // TODO: refactor API to properly return commandoutput
//    public boolean tbdCommitToLogFS(long logValidationValue, LogRecord logRecord,
//                               int expectedVersion, boolean validateVersionFlag,
//                               boolean sequentialFlag, boolean dryRun) throws InvalidServiceStateException {
    public boolean commitToLog(long logValidationValue, LogRecord logRecord,
                                    int expectedVersion, boolean validateVersionFlag,
                                    boolean sequentialFlag, boolean dryRun) throws InvalidServiceStateException {
//        System.out.println("commitToLog Active Threads:");
//        for (Thread t : Thread.getAllStackTraces().keySet()) {
//            System.out.println("Thread Name: " + t.getName() + ", State: " + t.getState());
//        }
        if (!isRunning) {
            throw new InvalidServiceStateException("Manager Not Running");
        }
        if (DebugConstants.DEBUG_FLAG) {
            log.debug(String.format("commitToLog [path=%s, sequential_flag=%b, record=%s]",
                    logRecord.getPath(), sequentialFlag, logRecord));
        }
        // bitmask style. 0: DATA_TRIGGER 1: CHILDREN_TRIGGER 2: EXIST_TRIGGER
        int txId;
        int recordVersion = -1;
        String path = logRecord.getPath();
        String pathOriginal = logRecord.getPath();
//        boolean logSizedFlag = false;
        boolean logRolledFlag = false;
        final long logSize;

        StopWatch watch = new StopWatch();
        if (DebugConstants.METRICS_FLAG) {
            watch.start();
        }
//        int segmentWaitThreshold = currentWriteSegmentIndex.get();
        int segmentWaitThreshold = tbdFSUpdateWriteSegmentIndex();

//        boolean logExistsFlag = CSvcUtils.retryCheckedOperation(() -> {
//            if (Files.exists(logPath)) {
//                return true;
//            } else {
//                // TODO: REMOVE ME;
//                //  Used to test deterioration
////                Thread.sleep(5);
//                throw new InvalidServiceStateException(String.format("CRITICAL ERROR - tbdCommitToLogFS " +
//                        "- Missing Log File, Recovery Flag Enabled" +
//                        " [path=%s]", logPath));
//            }
//        }, (Exception e) -> {
//            Throwable rootCause = CSvcUtils.unwrapRootCause(e);
//            if (!(rootCause instanceof InvalidServiceStateException)) {
//                log.error(String.format("CRITICAL ERROR - tbdCommitToLogFS " +
//                        "- Unexpected exception checking log [manager_id=%d]", managerId), e);
//            }
//            return false;
//        }, 5, false);

        boolean logExistsFlag = CSvcUtils.retryCheckedOperation(() -> {
            if (tbdCustomFileExists(logPath)) {
                return true;
            } else {
                // NOTE: Sleep 1.5ms to potentially resolve shared FS issues
                LockSupport.parkNanos(1500000);
                throw new InvalidServiceStateException(String.format("CRITICAL ERROR - tbdCommitToLogFS " +
                        "- Missing Log File" +
                        " [path=%s]", logPath));
            }
        }, (Exception e) -> {
            Throwable rootCause = CSvcUtils.unwrapRootCause(e);
            if (!(rootCause instanceof InvalidServiceStateException)) {
                log.error(String.format("CRITICAL ERROR - tbdCommitToLogFS " +
                        "- Unexpected exception checking log [manager_id=%d]", managerId), e);
            }
            return false;
        }, 3, false);

        if (!logExistsFlag) {
            // TODO: Error out. Entered commit but logpath missing
            //  (meaning another write occurred or crashed)
            // TODO: Attempt log recovery
            if (evaluateRecoveryState()) {
                recoveryRequiredFlag.set(true);
                throw new InvalidServiceStateException(String.format("CRITICAL ERROR - tbdCommitToLogFS " +
                        "- Missing Log File, Recovery Flag Enabled" +
                        " [path=%s]", logPath));
            } else {
                throw new InvalidServiceStateException(String.format("CRITICAL ERROR - tbdCommitToLogFS " +
                        "- Missing Log File, Lease Collision" +
                        " [path=%s]", logPath));
            }
        }

        if (Files.exists(logTempPath)) {
            // TODO: Error out. Something went wrong with a prior write
            // TODO: Attempt log recovery?
            if (evaluateRecoveryState()) {
                recoveryRequiredFlag.set(true);
                throw new InvalidServiceStateException(String.format("CRITICAL ERROR - tbdCommitToLogFS " +
                        "- Potential Concurrent Write, Recovery Flag Enabled [log_temp_path=%s]", logTempPath));
            } else {
                throw new InvalidServiceStateException(String.format("CRITICAL ERROR - tbdCommitToLogFS " +
                        "- Potential Concurrent Write [log_temp_path=%s]", logTempPath));
            }
        }

        logSize = CSvcUtils.retryCheckedOperation(
                () -> Files.size(logPath),
                (Exception e) -> log.error(String.format("tbdCommitToLogFS " +
                                "- Failed to retrieve log size [managerId=%d]", managerId), e),
                4, new InvalidServiceStateException(
                        String.format("tbdCommitToLogFS - Failed to retrieve log size [managerId=%d]", managerId)));
//        if (logSize < 0) {
//            throw new InvalidServiceStateException(String.format("tbdCommitToLogFS - Failed to retrieve log size [managerId=%d]",
//                    managerId));
//        }
//        while (!logSizedFlag) {
//            try {
////                snapshotSegmentIndex = currentWriteSegmentIndex.get();
//                logSize = Files.size(logPath);
//                logSizedFlag = true;
//            } catch (IOException e) {
//                // TODO: Log error
//                log.warn("REMOVE - TODO LOG - tbdRetrieveFromLogFS - sizing log");
//                e.printStackTrace(System.out);
//            }
//        }

        // NOTE: Reset recovery flag
        resetRecoveryMetadata();

        // NOTE: Update log size
        CSvcUtils.retryCheckedOperation(() -> tbdFSWriteTimestampWithContent(
                    logSizePath, ByteBuffer.allocate(Long.BYTES).putLong(logSize).array(), true),
                (Exception e) -> log.debug(String.format("WARN - commitToLog -" +
                        " Failed to update log size metadata [manager_id=%d]", managerId), e), 3);

        if (DebugConstants.METRICS_FLAG) {
            watch.stop();
            log.info(String.format("REMOVE ME - DEBUG - commitToLog - Initial Validations" +
                            " [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
            watch.reset();
            watch.start();
        }

        // NOTE: Wait for read to catch up
        final Object readWaitMonitor = new Object();
        synchronized (readWaitMonitor) {
            while ((currentReadSegmentIndex.get() < segmentWaitThreshold)
                    || (currentReadOffset < logSize && currentReadSegmentIndex.get() == segmentWaitThreshold)) {
                try {
                    log.debug(String.format("commitToLog - Write Wait [size_threshold=%d, segment_threshold=%d]",
                                                        logSize, segmentWaitThreshold));
                    tbdHelperInsertWaitWrapper(FSReadWaitWrapper.builder().tbdLogSizeThreshold(logSize)
                            .tbdLogSegmentThreshold(segmentWaitThreshold)
                            .monitorObject(readWaitMonitor).build());
                    readWaitMonitor.wait();
                } catch (InterruptedException e) {
                    // TODO: Consider which exception to throw
                    Thread.currentThread().interrupt();
                    throw new InvalidServiceStateException(e);
                }
            }
        }

        if (DebugConstants.METRICS_FLAG) {
            watch.stop();
            log.info(String.format("REMOVE ME - DEBUG - commitToLog - Read Wait" +
                    " [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
            watch.reset();
            watch.start();
        }
        // TODO: Sequential flag. Update path
        if (sequentialFlag) {
            // NOTE: Check parent stats table for children version
            // TODO: Consider if inmemory stats store can be used. Any edge cases? Likely requires sync
            String parentPath = Paths.get(path).getParent().toString();
            if (!pathStatsMap.containsKey(parentPath)) {
                // TODO: Log no parent error
                return false;
            }
            // NOTE: Update path
            path = path + String.format(Locale.ENGLISH, "%010d",
                                            pathStatsMap.get(parentPath).getChildSequentialVersion());
        }
        // TODO: Validations.
        // TODO: check stats(?) for existence of parent

        // TODO: Add to file
        // TODO: move log to temp file, copy, update the copy
        //// TODO: OUTDATED; prepare watch files (but flag them as temp, so they aren't consumed)
        try {
            boolean commitValid;
            // TODO: Validations.
            switch (logRecord.getType()) {
                case CREATE_EPHEMERAL:
                case CREATE:
                    commitValid = tbdFSValidateCreateCommit(path);
                    break;
                case DELETE:
                    // TODO: Clean up logic to be closer to db version
                    commitValid = tbdFSValidateDeleteCommit(path, validateVersionFlag, expectedVersion);
                    break;
                case SET:
                    commitValid = tbdFSValidateSetCommit(path, validateVersionFlag, expectedVersion);
                    break;
                default:
                    // TODO: Consider Error Out
                    commitValid = false;
            }

            if (!commitValid) {
                if (DebugConstants.DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - commitToLog - Invalid commit [type=%s, path=%s]",
                            logRecord.getType(), logRecord.getPath()));
                }
                return false;
            }

            logRecord.setPath(path);
            Files.move(logPath, logTempPath, StandardCopyOption.ATOMIC_MOVE);
            if (Files.exists(logWritePath)) {
                // TODO: Warn of having to clean up write remnants. Something went wrong with a prior write
                log.warn("TODO - UPDATE WARN - Warn of having to clean up write remnants." +
                        " Something went wrong with a prior write");
//                Files.delete(logWritePath);
            }
//            StandardOpenOption[] writeOptions;
            if (Files.size(logTempPath) >= LOG_SIZE_THRESHOLD) {
                // TODO: Roll over log into segment
//                Path nextSegmentPath = Paths.get(logSegmentPrefixStr.concat(
//                        String.valueOf(currentWriteSegmentIndex.getAndIncrement())));
                Path nextSegmentPath = Paths.get(logSegmentPrefixStr.concat(
                        String.valueOf(tbdFSUpdateWriteSegmentIndex())));
                // TODO: Validate path doesn't exist
                while (Files.exists(nextSegmentPath)) {
                    // TODO: Error out?
                    log.warn(String.format("Non-Terminal Error - commitToLog - Log roll-over collision " +
                                            "- Log segment already exists [segment_path=%s]", nextSegmentPath));
//                    nextSegmentPath = Paths.get(logSegmentPrefixStr.concat(
//                            String.valueOf(currentWriteSegmentIndex.getAndIncrement())));
                    nextSegmentPath = Paths.get(logSegmentPrefixStr.concat(
                            String.valueOf(tbdFSUpdateWriteSegmentIndex())));
                }
                Files.write(logTempRolloverPath, new byte[0],
                                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                                StandardOpenOption.SYNC);
                Files.move(logTempPath, nextSegmentPath, StandardCopyOption.ATOMIC_MOVE);
                Files.move(logTempRolloverPath, logTempPath, StandardCopyOption.ATOMIC_MOVE);
                logRolledFlag = true;
            }
            Files.copy(logTempPath, logWritePath, StandardCopyOption.REPLACE_EXISTING);

            if (DebugConstants.METRICS_FLAG) {
                watch.stop();
                log.info(String.format("REMOVE ME - DEBUG - commitToLog - File Preparations" +
                        " [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
                watch.reset();
                watch.start();
            }

            txId = tbdEvaluateTxId();

            FSEntry fsEntry = FSEntry.builder().record(logRecord)
                                    .entryTimestamp(tbdEvaluateNextEntryTimestamp(true)).build();
            byte[] fsEntryBytes = SerializationUtils.serialize(fsEntry);
            LogRecordFSHeader header = new LogRecordFSHeader(txId, fsEntryBytes.length);
            byte[] fsHeaderBytes = SerializationUtils.serialize(header);
            byte[] combinedArrays = new byte[HEADER_SIZE + fsEntryBytes.length];
            System.arraycopy(fsHeaderBytes, 0, combinedArrays, 0, fsHeaderBytes.length);
            System.arraycopy(fsEntryBytes, 0, combinedArrays, HEADER_SIZE, fsEntryBytes.length);

            if (DebugConstants.METRICS_FLAG) {
                watch.stop();
                System.out.println(String.format("DEBUG - METRICS - commitToLog - Pre-Write" +
                        " [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
                watch.reset();
                watch.start();
            }

//            Files.write(logWritePath, combinedArrays, StandardOpenOption.APPEND, StandardOpenOption.SYNC);
            try (FileChannel channel = FileChannel.open(logWritePath,
                    StandardOpenOption.APPEND, StandardOpenOption.SYNC)) {
                channel.write(ByteBuffer.wrap(combinedArrays));
                channel.force(true);
            }
//            Files.copy(logWritePath, logPath);
            // TODO: PROPERLY SET THIS UP, USE ANOTHER PATH
            Files.copy(logWritePath, logWriteHolderPath, StandardCopyOption.REPLACE_EXISTING);
            Files.move(logWriteHolderPath, logPath, StandardCopyOption.ATOMIC_MOVE);
            log.info(String.format("commitToLog - Successful log commit [txid=%d, path=%s, type=%s]", txId,
                    logRecord.getType(), logRecord.getPath()));

            if (DebugConstants.METRICS_FLAG) {
                watch.stop();
                System.out.println(String.format("DEBUG - METRICS - commitToLog - Write & Copy" +
                        " [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
                watch.reset();
                watch.start();
            }
        } catch (Exception e) {
            // NOTE: (FileAlreadyExistsException | FileNotFoundException)
            //   Another write occurring in parallel, or log in invalid state

            // NOTE: Revert changes
            logRecord.setPath(pathOriginal);
            if (Files.exists(logTempPath)) {
                try {
                    Files.move(logTempPath, logPath, StandardCopyOption.ATOMIC_MOVE);
                    log.warn(String.format("commitToLog - Failed commit - Discarding changes"));
                } catch (IOException ex) {
                    // No-Op: Failed to recover
                    log.error(String.format("CRITICAL ERROR - commitToLog - Unable to recover from failed write " +
                                            "- log in invalid state [path=%s]", logRecord.getPath()));
                }
            }
            // TODO: REMOVE ME ; duplicate log after exception is caught outside of call
            log.error(String.format(" REMOVE ME REMOVEME - CRITICAL ERROR - commitToLog - Failed commit"), e);
            throw new InvalidServiceStateException(e);
        }

        synchronized (outstandingChangesMonitor) {
            addToOutstandingRecords(txId, path, logRecord);
        }

        if (logRolledFlag) {
            tbdFSUpdateWriteSegmentIndex();
        }

        // NOTE: Update log size
        CSvcUtils.retryCheckedOperation(() -> tbdFSWriteTimestampWithContent(
                        logSizePath, ByteBuffer.allocate(Long.BYTES).putLong(Files.size(logPath)).array(), true),
                (Exception e) -> log.debug(String.format("WARN - commitToLog - POST COMMIT -" +
                        " Failed to update log size metadata [manager_id=%d]", managerId), e), 3);


        // NOTE: Attempt to update read file & clean up temp file
        CSvcUtils.retryCheckedOperation(() -> {
                    Files.move(logWritePath, logReadPath, StandardCopyOption.REPLACE_EXISTING,
                            StandardCopyOption.ATOMIC_MOVE);
                    log.debug(String.format("commitToLog - Successful read logupdate [txid=%d, path=%s, type=%s]", txId,
                            logRecord.getType(), logRecord.getPath()));
                },
                (Exception e) -> log.warn(String.format("commitToLog - POST COMMIT -" +
                        " Failed to update read log [record_path=%s]", logRecord.getPath()), e),
                3);

        CSvcUtils.retryCheckedOperation(() -> {
                    Files.deleteIfExists(logTempPath);
                    log.debug(String.format("commitToLog - Successful temp log clean up [txid=%d, path=%s, type=%s]",
                            txId, logRecord.getType(), logRecord.getPath()));
                },
                (Exception e) -> log.warn(String.format("commitToLog - POST COMMIT -" +
                        " Failed to clean up temp log post commit [record_path=%s]", logRecord.getPath()), e),
                3);

        if (DebugConstants.METRICS_FLAG) {
            watch.stop();
            log.info(String.format("REMOVE ME - DEBUG - commitToLog - Record Write" +
                    " [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
            watch.reset();
            watch.start();
        }
        return true;
    }

    private boolean tbdFSValidateSetCommit(String path, boolean validateVersionFlag, int expectedVersion) {
        boolean isValid = pathStatsMap.containsKey(path);
        if (isValid && validateVersionFlag) {
            isValid = pathStatsMap.get(path).getVersion() == expectedVersion;
        }
        if (DebugConstants.DEBUG_FLAG && !isValid) {
            log.warn(String.format("DEBUG - tbdFSValidateSetCommit - Invalid commit" +
                        " [path=%s, expected_version=%d, path_stats_map=%s]", path, expectedVersion, pathStatsMap));
        }
        return isValid;
    }

    private boolean tbdFSValidateDeleteCommit(String path, boolean validateVersionFlag, int expectedVersion) {
        LogRecordStats stats = pathStatsMap.get(path);
        boolean isValid = !Objects.isNull(stats) && stats.getChildCount() == 0;
        if (isValid && validateVersionFlag) {
            isValid = pathStatsMap.get(path).getVersion() == expectedVersion;
        }
        if (DebugConstants.DEBUG_FLAG && !isValid) {
                log.warn(String.format("DEBUG - tbdFSValidateDeleteCommit - Invalid commit" +
                        " [path=%s, expected_version=%d, path_stats_map=%s]", path, expectedVersion, pathStatsMap));
        }
        return isValid;
    }

    private boolean tbdFSValidateCreateCommit(String path) {
        boolean isValid = !pathStatsMap.containsKey(path);
        if (isValid) {
            // NOTE: Check parent exists and not ephemeral
            path = Paths.get(path).getParent().toString();
            isValid = pathStatsMap.containsKey(path)
                    && pathStatsMap.get(path).getEphemeralOwnerId() == NONEPHEMERAL_OWNER_VALUE;
        }
        if (DebugConstants.DEBUG_FLAG && !isValid) {
                log.warn(String.format("REMOVE ME - DEBUG -" +
                    " tbdFSValidateCreateCommit - Invalid commit [path=%s, path_stats_map=%s]", path, pathStatsMap));
        }
        return isValid;
    }

    public void tbdHelperInsertWaitWrapper(FSReadWaitWrapper FSReadWaitWrapper) {
        int index = 0;
        while (index < tbdLogReadWaitQueue.size()) {
            FSReadWaitWrapper currentWaitWrapper = tbdLogReadWaitQueue.get(index);
            if ((currentWaitWrapper.getTbdLogSizeThreshold()
                    >= FSReadWaitWrapper.getTbdLogSizeThreshold()
                    && currentWaitWrapper.getTbdLogSegmentThreshold()
                        == FSReadWaitWrapper.getTbdLogSegmentThreshold())
                    || currentWaitWrapper.getTbdLogSegmentThreshold()
                    > FSReadWaitWrapper.getTbdLogSegmentThreshold()) {
                // TODO: stop iteration when another waiting
                break;
            }
            index++;
        }
        // TODO: Confirm that there are no out of range errors (index should never be > size)
        if (DebugConstants.DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - tbdHelperInsertWaitWrapper " +
                    "- Adding Wait Element [TbdFSReadWaitWrapper=%s]", FSReadWaitWrapper));
        }
        tbdLogReadWaitQueue.add(index, FSReadWaitWrapper);
    }

    public LogRecord retrieveFromLog(String path, boolean getStatsFlag,
                                     WatchConsumerWrapper<WatchInput> watchCallable)
                                        throws InvalidServiceStateException {
        if (!isRunning) {
            throw new InvalidServiceStateException("Manager Not Running");
        }
        String output;
        LogRecord resultRecord = new LogRecord(DELETED_RECORD_PATH, new byte[0], LogRecord.LogRecordType.DELETE);

        if (localOutstandingTxnToPath.getReverseMap().getOrDefault(path, -1) > logCounter.get()) {
            resultRecord = localOutstandingPathToRecord.get(path);
            if (!Objects.isNull(resultRecord)
                    && (!getStatsFlag || resultRecord.containsStats())) {
                if (Objects.isNull(watchCallable)) {
                    return resultRecord;
                } else {
                    // NOTE: Disable statsflag due to pre-populated record
                    getStatsFlag = false;
                }
            }
        }

//        boolean logSizedFlag = false;
        long logSize;
        try {
            logSize = CSvcUtils.retryCheckedOperation(
                    () -> Files.size(logPath),
                    (Exception e) -> log.warn(String.format("retrieveFromLog -" +
                                    " Failed to retrieve log size [manager_id=%d]",
                            managerId), e),
                    3, new InvalidServiceStateException(String.format(
                            "retrieveFromLog - Failed to retrieve log size [manager_id=%d]", managerId)));
        } catch (InvalidServiceStateException e) {
            logSize = CSvcUtils.retryCheckedOperation(
                    this::retrieveLogFileSizeFromMetadata,
                    (Exception ex) -> log.error(String.format("retrieveFromLog -" +
                                    " Failed to retrieve log size from metadata [manager_id=%d]",
                            managerId), ex),
                    3, new InvalidServiceStateException(String.format(
                            "retrieveFromLog - Failed to retrieve log size [manager_id=%d]", managerId)));
        }

        int segmentWaitThreshold = tbdFSUpdateWriteSegmentIndex();

        // NOTE: Wait for reader to catch up
        final Object readWaitMonitor = new Object();
        synchronized (readWaitMonitor) {
            while ((currentReadSegmentIndex.get() < segmentWaitThreshold)
                    || (currentReadOffset < logSize && currentReadSegmentIndex.get() == segmentWaitThreshold)) {
                try {
                    log.debug(String.format("retrieveFromLog - Read Wait [size_threshold=%d, segment_threshold=%d]",
                                                        logSize, segmentWaitThreshold));
                    tbdHelperInsertWaitWrapper(FSReadWaitWrapper.builder().tbdLogSizeThreshold(logSize)
                            .tbdLogSegmentThreshold(segmentWaitThreshold)
                            .monitorObject(readWaitMonitor).build());
                    readWaitMonitor.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new InvalidServiceStateException(e);
                }
            }
        }

        // TODO: Block log consumption and related state mutations
        synchronized (tbdLogReadMonitor) {
            // TODO: fetch info on segment and offset
            FSInmemoryInfo FSInmemoryInfo = tbdPathToInfo.get(path);
            if (Objects.isNull(FSInmemoryInfo)) {
                // TODO: Error out
                return resultRecord;
            }
            // TODO: Helper function to fetch the data
            // TODO: if stats flag need to fetch from inmemory struct
            //  (MUST WAIT FOR LOGREADER TO GET TO LOG ENTRY AT CALL)
            try {
                resultRecord = tbdFSRecordRetrieveHelper(path, FSInmemoryInfo.getLogSegment(),
                                        FSInmemoryInfo.getHeaderStartOffset(), FSInmemoryInfo.getTransactionId());
                if (getStatsFlag) {
                    resultRecord.setStats(pathStatsMap.get(path));
                }

                if (!Objects.isNull(watchCallable) && !resultRecord.getType().equals(DELETE)) {
                    if (watchCallable.getEvaluateThresholdFlag()) {
                        watchCallable.setTxIdThreshold(FSInmemoryInfo.getTransactionId());
                    }
                    addWatcherTxHelper(path, watchCallable);
                }

                return resultRecord;
            } catch (IOException e) {
                // TODO: Update log handling
                log.error(String.format("retrieveFromLog - All retries failed [path=%s]", path));
                throw new InvalidServiceStateException(e);
            }
        }
    }

    public LogRecord tbdFSRecordRetrieveHelper(String path, int logSegment, long readOffset,
                                        int expectedTransactionId) throws IOException, InvalidServiceStateException {
//        byte[] fileBytes;
//        TbdLogRecordFSHeader header;
//        TbdFSEntry tbdFSEntry;
        return CSvcUtils.retryCheckedOperation(() -> {
            byte[] fileBytes;
            if (logSegment < tbdFSUpdateWriteSegmentIndex()) {
                fileBytes = Files.readAllBytes(Paths.get(logSegmentPrefixStr.concat(
                        String.valueOf(logSegment))));
            } else {
                fileBytes = Files.readAllBytes(this.logReadPath);
            }

            // NOTE: Validations
            if (readOffset + HEADER_SIZE >= fileBytes.length) {
                // NOTE: Restart, check again if log segment was rolled over
                throw new InvalidServiceStateException("tbdFSRecordRetrieveHelper " +
                        "- readOffset + HEADER_SIZE >= fileBytes.length");
            }
            LogRecordFSHeader header = SerializationUtils.deserialize(Arrays.copyOfRange(fileBytes,
                    (int)readOffset, (int)readOffset+HEADER_SIZE));

            if (header.getTransactionId() != expectedTransactionId) {
                // NOTE: Restart, check again if log segment was rolled over
                throw new InvalidServiceStateException(String.format("tbdFSRecordRetrieveHelper " +
                        "- Not expected transaction ID [expected_txid=%d, record_txid=%d]",
                        expectedTransactionId, header.getTransactionId()));
            }

            FSEntry tbdFSEntry = SerializationUtils.deserialize(Arrays.copyOfRange(fileBytes,
                    (int)readOffset+HEADER_SIZE, ((int)readOffset+HEADER_SIZE)+header.getLogEntryByteSize()));
            if (!tbdFSEntry.getRecord().getPath().equals(path)) {
                // NOTE: Restart
                throw new InvalidServiceStateException(String.format("tbdFSRecordRetrieveHelper " +
                        "- Missing expected path [expected_path=%s, record_path=%s]",
                        path, tbdFSEntry.getRecord().getPath()));
            }
            return tbdFSEntry.getRecord();
        }, (Exception e) -> {
            throw new InvalidServiceStateException("tbdFSRecordRetrieveHelper - Failed all retries", e);
        }, 10);



//        int retryCount = 0;
//        int retryMax = 10;
//        while (retryCount < retryMax) {
//            retryCount++;
//            if (logSegment < tbdFSUpdateWriteSegmentIndex()) {
//                try {
//                    // TODO: Test performance with other read options
//                    fileBytes = Files.readAllBytes(Paths.get(logSegmentPrefixStr.concat(
//                            String.valueOf(logSegment))));
//                } catch (IOException e) {
//                    // No-Op
//                    // TODO: Log errors
//                    if (retryCount >= retryMax) {
//                        throw e;
//                    }
//                    continue;
//                }
//            } else {
//                try {
//                    fileBytes = Files.readAllBytes(this.logReadPath);
//                } catch (IOException e) {
//                    // No-Op
//                    continue;
//                }
//            }
//
//
//            header = SerializationUtils.deserialize(Arrays.copyOfRange(fileBytes, (int)readOffset,
//                    (int)readOffset+HEADER_SIZE));
//
//            if (header.getTransactionId() != expectedTransactionId) {
//                // TODO: Restart, check again if log segment was rolled over
//                // TODO: Consider recursion with special flag
//                // TODO: Update exception
//                log.warn(String.format("tbdFSRecordRetrieveHelper - Not expected transaction ID" +
//                        " [expected_txid=%d, record_txid=%d]", expectedTransactionId, header.getTransactionId()));
//                fileBytes = null;
//                continue;
//            }
//
//            tbdFSEntry = SerializationUtils.deserialize(Arrays.copyOfRange(fileBytes, (int)readOffset+HEADER_SIZE,
//                    ((int)readOffset+HEADER_SIZE)+header.getLogEntryByteSize()));
//            if (!tbdFSEntry.getRecord().getPath().equals(path)) {
//                // TODO: Restart, check again if log segment was rolled over
//                // TODO: Consider recursion with special flag
//                // TODO: Update exception
//                log.warn(String.format("tbdFSRecordRetrieveHelper - Missing expected path" +
//                                        " [expected_path=%s, record_path=%s]", path, tbdFSEntry.getRecord().getPath()));
//                fileBytes = null;
//                continue;
//            }
//            return tbdFSEntry.getRecord();
//        }
//        // TODO: Update error
//        throw new InvalidServiceStateException("tbdFSRecordRetrieveHelper - Failed all retries");
    }

    private int tbdEvaluateRecordVersion(LogRecord record) {
        int version = 0;
        switch (record.getType()) {
            case DELETE:
                version = pathStatsMap.get(record.getPath()).getVersion();
                break;
            case SET:
                version = pathStatsMap.get(record.getPath()).getVersion() + 1;
                break;
            default:
        }
        return version;
    }

    private void tbdTriggerWatchHelper(LogRecordFSHeader header, FSEntry entry)
                                                    throws InvalidServiceStateException {
        // VALIDATION: Watch trigger order
        if (watchTxIdWatermark >= header.getTransactionId()) {
            if (DebugConstants.DEBUG_FLAG) {
                log.debug(String.format("DEBUG - Watch Skipped" +
                                " [tx_id_watermark=%d, path=%s, tx_id=%d]", watchTxIdWatermark,
                                entry.getRecord().getPath(), header.getTransactionId()));
            }
            return;
        }
        HashSet<WatchedEvent> watches;
        HashSet<WatchedEvent> removedWatches = new HashSet<>();
        String watchPath;
        for (WatchedEvent.WatchType watchType
                : LogRecord.evaluateTriggeredWatchTypes(entry.getRecord().getType())) {
            watchPath = watchType.equals(WatchedEvent.WatchType.CHILDREN)
                            ? Paths.get(entry.getRecord().getPath()).getParent().toString()
                            : entry.getRecord().getPath();
            WatchInput watchInput = WatchInput.builder()
                    .recordVersion(tbdEvaluateRecordVersion(entry.getRecord()))
                    .path(watchPath)
                    .triggerRecordType(entry.getRecord().getType())
                    .triggerTransactionId(header.getTransactionId()).build();
            if (DebugConstants.DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - tbdTriggerWatchHelper - Checking Watches" +
                        " [path=%s, watch_type=%s]", watchPath, watchType));
                System.out.println(String.format("DEBUG - tbdTriggerWatchHelper - Registered Watches" +
                        " [watches=%s]", pathWatchMap.toString()));
            }
            watches = pathWatchMap.get(watchType).getOrDefault(watchPath, new HashSet<>());
            for(WatchedEvent registeredWatch : watches) {
                if (registeredWatch.getTriggerTransactionThreshold() > header.getTransactionId()) {
                    // NOTE: Skip watch if below tx id threshold
                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println(String.format("DEBUG - Watch Skipped" +
                                        " [path=%s, txThreshold=%d]", registeredWatch.getPath(),
                                registeredWatch.getTriggerTransactionThreshold()));
                    }
                    continue;
                }
                if (DebugConstants.DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - tbdTriggerWatchHelper - Triggered Watch" +
                            " [path=%s, watch_type=%s]", entry.getRecord().getPath(), watchType));
                }
//                            watchInputMap.put(registeredWatch, watchInput);
                if (watchExecutor.isShutdown() || watchExecutor.isTerminated() ) {
                    System.out.println(String.format(
                            "CRITICAL ERROR - LogReader - Manager Watch Executor Not Running " +
                                    "- TERMINATING [manager_id=%d]", managerId));
                    throw new InvalidServiceStateException(String.format(
                            "CRITICAL ERROR - LogReader - Manager Watch Executor Not Running " +
                                    "- TERMINATING [manager_id=%d]", managerId));
                } else {
                    watchExecutor.submit(() -> registeredWatch.triggerWatch(this, watchInput));
                    // NOTE: Should always increase
                    watchTxIdWatermark = Math.max(watchTxIdWatermark, header.getTransactionId());
                }
                if (!registeredWatch.getWatchPersistentFlag()) {
                    removedWatches.add(registeredWatch);
                }
            }
            watches.removeAll(removedWatches);
        }
    }

    private void tbdConsumeSegmentEntries(boolean triggerWatchesFlag) throws InvalidServiceStateException {
        Path nextSegmentPath = Paths.get(logSegmentPrefixStr.concat(String.valueOf(currentReadSegmentIndex.get())));
        byte[] fileBytes;
        LogRecordFSHeader header;
        FSEntry entry;
        while (Files.exists(nextSegmentPath)) {
            log.debug(String.format("tbdConsumeSegmentEntries - Reading from log segment [path=%s, expected_txid=%d]",
                    nextSegmentPath, logCounter.get()+1));
            try {
                fileBytes = Files.readAllBytes(nextSegmentPath);
            } catch (IOException e) {
                log.error(String.format("CRITICAL ERROR - tbdConsumeSegmentEntries - Unexpected Exception -" +
                        " Reading from log segment [path=%s, manager_id=%d]", nextSegmentPath, managerId), e);
                throw new InvalidServiceStateException(e);
            }

            while (currentReadOffset + HEADER_SIZE < fileBytes.length) {
                header = SerializationUtils.deserialize(Arrays.copyOfRange(fileBytes, (int)currentReadOffset,
                        (int)currentReadOffset+HEADER_SIZE));
                if (header.getTransactionId() != logCounter.get()+1) {
                    log.error(String.format("tbdConsumeSegmentEntries - Missing expected transaction ID" +
                                    " [expected_txid=%d, header_txid=%d]",
                            logCounter.get()+1, header.getTransactionId()));
                    throw new InvalidServiceStateException(String.format("tbdConsumeSegmentEntries -" +
                                    " Missing expected transaction ID [expected_txid=%d, header_txid=%d]",
                            logCounter.get()+1, header.getTransactionId()));
                }

                entry = SerializationUtils.deserialize(
                        Arrays.copyOfRange(fileBytes, (int)currentReadOffset+HEADER_SIZE,
                                (int)currentReadOffset+HEADER_SIZE+header.getLogEntryByteSize()));

                if (triggerWatchesFlag) {
                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println(String.format("DEBUG - tbdConsumeSegmentEntries " +
                                "- Evaluating Triggered Watches"));
                    }
                    tbdTriggerWatchHelper(header, entry);
                }
                updateMetadataStore(entry.getRecord(), entry.getEntryTimestamp(),
                        header.getTransactionId(), currentReadSegmentIndex.get(), currentReadOffset);
                synchronized (outstandingChangesMonitor) {
                    logCounter.incrementAndGet();
                    removeFromOutstandingRecords(header.getTransactionId(), entry.getRecord().getPath());
                }
                currentReadOffset += HEADER_SIZE+header.getLogEntryByteSize();
            }
            currentReadOffset = 0;
            int nextReadSegmentIndex = currentReadSegmentIndex.incrementAndGet();
            nextSegmentPath = Paths.get(logSegmentPrefixStr.concat(String.valueOf(nextReadSegmentIndex)));
            currentWriteSegmentIndex.updateAndGet(currentValue -> Math.max(currentValue, nextReadSegmentIndex));
        }
    }

    private void tbdIncrementReadFailureCounter() {
        readFailureCounter.incrementAndGet();
    }

    private void tbdConsumeEntries(boolean triggerWatchesFlag) throws InvalidServiceStateException {
        CSvcUtils.retryCheckedOperation(() -> {
                    tbdConsumeSegmentEntries(triggerWatchesFlag);
                    try (RandomAccessFile ignored = new RandomAccessFile(logReadPath.toFile(), "r");
                         FileChannel fileChannel = ignored.getChannel()) {
                        if (currentReadOffset == 0) {
                            // NOTE: Starting read OR rollover
                            log.debug(String.format("tbdConsumeSegmentEntries -" +
                                    " Reading from top of log [expected_txid=%d]", logCounter.get()+1));
                        }

                        ByteBuffer hdrBuffer = ByteBuffer.allocate(HEADER_SIZE);
                        if (currentReadOffset <= fileChannel.size()) {
                            // NOTE: Continue at previous offset
                            fileChannel.position(currentReadOffset);
                        } else {
                            // NOTE: Restart
                            if (DebugConstants.DEBUG_FLAG) {
                                log.warn(String.format("DEBUG - tbdConsumeEntries - Invalid read log size" +
                                    " [current_read_offset=%d, log_size=%d]", currentReadOffset, fileChannel.size()));
                            }
                            throw new InvalidServiceStateException(String.format("tbdConsumeEntries " +
                                    "- Invalid read log size [current_read_offset=%d, log_size=%d]",
                                    currentReadOffset, fileChannel.size()));
                        }

                        long headerPosition;
                        ByteBuffer buffer;
                        while (fileChannel.position() + HEADER_SIZE < fileChannel.size()) {
                            headerPosition = fileChannel.position();
                            if (fileChannel.read(hdrBuffer) <= 0) {
                                // NOTE: Restart
                                if (DebugConstants.DEBUG_FLAG) {
                                    log.warn("DEBUG - tbdConsumeEntries - Failed to read record header");
                                }
                                throw new InvalidServiceStateException("tbdConsumeEntries " +
                                        "- Failed to read record header");
                            }

                            LogRecordFSHeader header = SerializationUtils.deserialize(hdrBuffer.array());
                            if (logCounter.get()+1 != header.getTransactionId()) {
                                // NOTE: Restart
//                                if (DebugConstants.DEBUG_FLAG) {
                                    log.warn(String.format("tbdConsumeEntries - Missing expected transaction ID" +
                                                    " [expected_txid=%d, header_txid=%d, log_offset=%d]",
                                            logCounter.get()+1, header.getTransactionId(), headerPosition));
//                                }
                                throw new InvalidServiceStateException(String.format("tbdConsumeEntries -" +
                                                " Missing expected transaction ID [expected_txid=%d, header_txid=%d]",
                                        logCounter.get()+1, header.getTransactionId()));
                            }
                            hdrBuffer.clear();
                            buffer = ByteBuffer.allocate(header.getLogEntryByteSize());

                            if (fileChannel.read(buffer) <= 0) {
                                // NOTE: Restart
                                if (DebugConstants.DEBUG_FLAG) {
                                    log.warn("tbdConsumeEntries - Failed to read from log read path");
                                }
                                throw new InvalidServiceStateException("tbdConsumeEntries -" +
                                        " Failed to read from log read path");
                            }
                            FSEntry entry = SerializationUtils.deserialize(buffer.array());

                            if (triggerWatchesFlag) {
                                tbdTriggerWatchHelper(header, entry);
                            }

                            // TODO: Consider better design to achieve the required state management
                            updateMetadataStore(entry.getRecord(),
                                    entry.getEntryTimestamp(),
                                    header.getTransactionId(), currentReadSegmentIndex.get(), headerPosition);
                            currentReadOffset = fileChannel.position();
                            synchronized (outstandingChangesMonitor) {
                                logCounter.incrementAndGet();
                                removeFromOutstandingRecords(header.getTransactionId(), entry.getRecord().getPath());
                            }
                            buffer.clear();
                        }
                    }
                    readFailureCounter.set(0);
                },
                (Exception e) -> {
                    log.error(String.format("CRITICAL ERROR - tbdConsumeEntries -" +
                        " Failed to consume entries from read log -" +
                        " Incrementing read failure counter [manager_id=%d, failure_counter=%d]",
                            managerId, readFailureCounter.get()), e);
//                    if (e instanceof FileNotFoundException || e instanceof NoSuchFileException) {
                    tbdIncrementReadFailureCounter();
//                    }
                    throw new InvalidServiceStateException(e);
                }, 5);
    }

    private boolean tbdFSValidateReadLog() throws InvalidServiceStateException {
        boolean logValidFlag;

        // NOTE: Compare read log & log sizes
        long logSize;
        try {
            logSize = CSvcUtils.retryCheckedOperation(
                    () -> Files.size(logPath),
                    (Exception e) -> log.warn(String.format("tbdFSValidateReadLog " +
                            "- Failed to retrieve log size [manager_id=%d]", managerId), e),
                    3, new InvalidServiceStateException(String.format(
                            "tbdFSValidateReadLog - Failed to retrieve log size [manager_id=%d]", managerId)));
        } catch (InvalidServiceStateException e) {
            logSize = CSvcUtils.retryCheckedOperation(
                    this::retrieveLogFileSizeFromMetadata,
                    (Exception ex) -> {
                        log.error(String.format("tbdFSValidateReadLog -" +
                                        " Failed to retrieve log size from metadata [manager_id=%d]",
                                managerId), ex);
                        throw new InvalidServiceStateException(ex);
                    }, 3);
        }

        long readLogSize = CSvcUtils.retryCheckedOperation(
                () -> Files.size(logReadPath),
                (Exception e) -> {
                    log.warn(String.format("tbdFSValidateReadLog " +
                            "- Failed to retrieve log size [manager_id=%d]", managerId), e);
                    throw new InvalidServiceStateException(e);
                }, 5);
        logValidFlag = readLogSize == logSize;
        if (!logValidFlag) {
            return false;
        }

        // TODO: Check tx_ids

        return true;
    }

    private void tbdFSManageReadLog() throws InvalidServiceStateException {
        // TODO: Properly document
        //  Checks last modified time for read log and validates against write log
        //  If modified time delta is beyond threshold, then update read log
        //  Goal is to avoid an error in sparse write scenario to result in read log being stale for extended periods
        //  of time
        long readLogTS = CSvcUtils.retryCheckedOperation(
                () -> Files.getLastModifiedTime(logReadPath).toMillis(),
                (Exception e) -> {
                    log.warn(String.format("tbdFSValidateReadLog " +
                            "- Failed to retrieve read log modified time [manager_id=%d]", managerId), e);
                    throw new InvalidServiceStateException(e);
                }, 5);
        readLogTS = Math.max(readValidationTimestamp, readLogTS);
        long currentTimeMS = System.currentTimeMillis();
        if (currentTimeMS - readLogTS > READ_LOG_TS_DELTA_THRESHOLD) {
            if (!tbdFSValidateReadLog()) {
                // NOTE: Update read log
                CSvcUtils.retryCheckedOperation(
                        () -> Files.copy(logPath, logRecoveryWritePath),
                        (Exception e) -> {
                            log.warn(String.format("tbdFSValidateReadLog " +
                                    "- Error updating read log - Failed to copy log [manager_id=%d]", managerId), e);
                            throw new InvalidServiceStateException(e);
                        }, 5);
                CSvcUtils.retryCheckedOperation(
                        () -> Files.move(logRecoveryWritePath, logReadPath, StandardCopyOption.ATOMIC_MOVE,
                                StandardCopyOption.REPLACE_EXISTING),
                        (Exception e) -> {
                            log.warn(String.format("tbdFSValidateReadLog " +
                                    "- Error updating read log - Failed to replace read log" +
                                    " [manager_id=%d]", managerId), e);
                            throw new InvalidServiceStateException(e);
                        }, 5);
            }
        }
        readValidationTimestamp = currentTimeMS;
    }

    private void tbdFSManageEphemerals() throws ClosedByInterruptException {
        // NOTE: Validate current timestamp against heartbeat file modified timestamp
        long currentTimeMS = System.currentTimeMillis();
//        try {
//            currentTimeMS = CSvcUtils.retryCheckedOperation(this::tbdRetrieveTimeStamp,
//                    (Exception e) -> {
//                        log.warn(String.format("tbdFSManageEphemerals " +
//                                        "- Unable to retrieve current time [manager_id=%d]", managerId));
//                        return -1L;
//                    }, 10, -1L);
//            if (currentTimeMS < 0) {
//                return;
//            }
//        } catch (InvalidServiceStateException e) {
//            if (e.getCause() instanceof ClosedByInterruptException) {
//                Thread.currentThread().interrupt();
//                throw new ClosedByInterruptException();
//            }
//            return;
//        }
//        int retryCount = 0;
//        int retryMax = 5;
//        while (retryCount < retryMax) {
//            retryCount++;
//            try {
//                currentTimeMS = tbdRetrieveTimeStamp();
//                break;
//            } catch (InvalidServiceStateException e) {
//                if (retryCount == retryMax) {
//                    System.out.println(String.format("tbdFSManageEphemerals - Unable to retrieve current time"));
//                    e.printStackTrace(System.out);
//                    return;
//                }
//            }
//        }

        ArrayList<Path> timedOutPaths = new ArrayList<>();
        long fileModifiedTS;
        long heartbeatDelta;
        String fileName;
        long ownerId;
        Set<Long> untrackedOwners = new HashSet<>(ephemeralOwnerCount.keySet());
        try (DirectoryStream<Path> files = Files.newDirectoryStream(heartbeatDirPath)) {
            for (Path heartbeatFile : files) {
                fileName = heartbeatFile.getFileName().toString();
                try {
//                    fileModifiedTS = Files.getLastModifiedTime(heartbeatFile).toMillis();
                    try {
                        ownerId = Long.parseLong(fileName);
                    } catch (NumberFormatException e) {
                        log.debug(String.format("tbdFSManageEphemerals -" +
                                " Invalid file found in Heartbeat Directory [path=%s]", heartbeatFile));
                        continue;
                    }
                    fileModifiedTS = tbdFSRetrieveTimestamp(heartbeatFile, true);
                    heartbeatDelta = currentTimeMS - fileModifiedTS;
                    untrackedOwners.remove(ownerId);
                    // NOTE: Check if they own ephemeral nodes. Else ignore
                    if (heartbeatDelta > (long) tbdFSLeaseThreshold * tbdFSHeartbeatMultiplier
                            && ephemeralOwnerCount.getOrDefault(ownerId, 0) > 0) {
                        timedOutPaths.add(heartbeatFile);
                    }
                } catch (NumberFormatException e) {
                    log.warn(String.format("tbdFSManageEphemerals - Invalid heartbeat file [filename=%s]",
                            fileName), e);
                } catch (FileNotFoundException e) {
                    log.warn(String.format("tbdFSManageEphemerals - Missing file or unexpected file size [filename=%s]",
                            fileName), e);
                } catch (IOException e) {
                    log.warn(String.format("tbdFSManageEphemerals - Unexpected Exception [filename=%s]",
                            fileName), e);
                }
            }
        } catch (IOException e) {
            log.error(String.format("tbdFSManageEphemerals - Error Accessing Heartbeat Directory [path=%s]",
                    heartbeatDirPath), e);
//            e.printStackTrace(System.out);
            return;
        }

        // NOTE: Move timed-out heartbeat files to timeout directory
        for (Path movePath : timedOutPaths) {
            try {
                CSvcUtils.retryCheckedOperation(() -> {
                            Files.move(movePath, timeoutDirPath.resolve(movePath.getFileName()),
                                    StandardCopyOption.ATOMIC_MOVE);
                        },
                        (Exception e) -> log.warn(String.format("tbdFSManageEphemerals " +
                                            "- Failed to move timed out manager file [filename=%s]", movePath)),
                        5);
            } catch (InvalidServiceStateException ignored) {
                // NOTE: No-Op
            }
//            retryCount = 0;
//            while (retryCount < retryMax) {
//                retryCount++;
//                try {
//                    Files.move(movePath, timeoutDirPath.resolve(movePath.getFileName()),
//                                StandardCopyOption.ATOMIC_MOVE);
//                    break;
//                } catch (FileNotFoundException e) {
//                    if (DebugConstants.DEBUG_FLAG) {
//                        System.out.println(String.format("tbdFSManageEphemerals " +
//                                        "- Missing timed out manager file [filename=%s]",
//                                movePath));
//                        e.printStackTrace(System.out);
//                    }
//                    break;
//                } catch (IOException e) {
//                    if (retryCount == retryMax || DebugConstants.DEBUG_FLAG) {
//                        System.out.println(String.format("tbdFSManageEphemerals " +
//                                        "- Failed to move timed out manager file [filename=%s]",
//                                movePath));
//                        e.printStackTrace(System.out);
//                    }
//                }
//            }
        }

        // NOTE: Once last path deleted, delete timeoutdir
        HashSet<Long> pendingDeleteOwners = new HashSet<>();
        HashSet<Path> pendingDeletePaths = new HashSet<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(timeoutDirPath)) {
            for (Path timedoutFile : files) {
                try {
                    ownerId = Long.parseLong(timedoutFile.getFileName().toString());
                } catch (NumberFormatException e) {
                    log.debug(String.format("tbdFSManageEphemerals -" +
                            " Invalid file found in Time Out Directory [path=%s]", timedoutFile));
                    continue;
                }
                untrackedOwners.remove(ownerId);
                if (ephemeralOwnerCount.getOrDefault(ownerId, 0) <= 0) {
                    // NOTE: Clean up manager file with 0 ephemerals
                    pendingDeletePaths.add(timedoutFile);
                } else {
                    pendingDeleteOwners.add(ownerId);
                    log.info(String.format("tbdFSManageEphemerals " +
                            "- Timed out manager [manager_id=%d]", ownerId));
                    if (DebugConstants.DEBUG_FLAG) {
                        // TODO: REMOVE ME
                        log.info(String.format("DEBUG - REMOVEME -" +
                                " Timed out manager paths [manager_id=%d, count=%d,\n\tpaths=%s]",
                                ownerId, ephemeralOwnerCount.getOrDefault(ownerId, 0),
                                ephemeralOwnerToPaths.getOrDefault(ownerId, Collections.emptySet())));
                    }
                }
            }
        } catch (IOException e) {
            log.error(String.format("tbdFSManageEphemerals - Error Accessing Timeout Directory [path=%s]",
                    timeoutDirPath), e);
//            e.printStackTrace(System.out);
            return;
        }
        pendingDeleteOwners.addAll(untrackedOwners);
        tbdFSDeleteEphemeralHelperFn(pendingDeleteOwners);
        for (Path removedOwnerFile : pendingDeletePaths) {
            try {
                if (DebugConstants.DEBUG_FLAG) {
                    log.debug(String.format("tbdFSManageEphemerals - Removing timed out manager [manager_id=%s]",
                            removedOwnerFile.getFileName().toString()));
                }
                Files.delete(removedOwnerFile);
            } catch (IOException e) {
                // NOTE: No-Op. File will be marked for deletion next execution
                if (DebugConstants.DEBUG_FLAG) {
                    log.warn(String.format("tbdFSManageEphemerals - Failed to delete path [path=%s]",
                            removedOwnerFile), e);
//                    e.printStackTrace(System.out);
                }
            }
        }
    }

    private synchronized long tbdEvaluateNextEntryTimestamp(boolean updateTSFlag) throws IOException,
            InvalidServiceStateException {
//        timestampEntryFilePath;
        long entryWatermark = 0L;
        long timestamp = System.currentTimeMillis();
        if (Files.exists(timestampEntryFilePath)) {
            entryWatermark = tbdFSRetrieveTimestamp(timestampEntryFilePath, true);
        } else if (updateTSFlag) {
            tbdFSUpdateTimestamp(timestampEntryFilePath, timestamp, true);
        }
        timestamp = timestamp <= entryWatermark ? entryWatermark + 1 : timestamp;
        if (updateTSFlag) {
            tbdFSUpdateTimestamp(timestampEntryFilePath, timestamp, false);
        }
        return timestamp;
    }

//    private synchronized long tbdRetrieveCurrentTime() throws InvalidServiceStateException, ClosedByInterruptException {
    private synchronized long tbdRetrieveCurrentTime() {
        return System.currentTimeMillis();
        //==== OUTDATED: due to FS issues
//        CSvcUtils.retryCheckedOperation(() -> {
//                Files.write(timestampFilePath, new byte[1],
//                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
//            }, (Exception e) -> {
//                if (DebugConstants.DEBUG_FLAG) {
//                    log.error(String.format("tbdRetrieveTimeStamp - Failed retries [managerId=%d]", managerId), e);
//                }
//                if (e instanceof ClosedByInterruptException) {
//                    // Note: Thread interrupted, error out
//                    throw e;
//                }
//            }, 10);
        //====

//        long timestampMS = -1;
//        boolean retryFlag = true;
//        int retryCount = 0;
//        int retryMax = 10;
//        IOException exception = null;
//        while (retryFlag && retryCount < retryMax) {
//            try {
//                retryCount++;
//                Files.write(timestampFilePath, new byte[1],
//                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
//                retryFlag = false;
//            } catch (ClosedByInterruptException e) {
//                // Note: Thread interrupted, error out
//                throw e;
//            } catch (IOException e) {
//                // No-Op
//                exception = e;
//            }
//        }
//
//        if (retryFlag) {
//            if (DebugConstants.DEBUG_FLAG) {
//                System.out.println("DEBUG - tbdRetrieveTimeStamp - Failed retries");
//            }
//            throw new InvalidServiceStateException(exception);
//        }

//        return CSvcUtils.retryCheckedOperation(() -> Files.getLastModifiedTime(timestampFilePath).toMillis(),
//                (Exception e) -> {
//                    if (DebugConstants.DEBUG_FLAG) {
//                        log.error(String.format("tbdRetrieveTimeStamp - Failed retries [managerId=%d]", managerId), e);
//                    }
//                    if (e instanceof ClosedByInterruptException) {
//                        // Note: Thread interrupted, error out
//                        throw e;
//                    }
//                }, 10);
//
//        retryCount = 0;
//        retryFlag = true;
//        while (retryFlag && retryCount < retryMax) {
//            try {
//                retryCount++;
//                timestampMS = Files.getLastModifiedTime(timestampFilePath).toMillis();
//                retryFlag = false;
//            } catch (ClosedByInterruptException e) {
//                // Note: Thread interrupted, error out
//                throw e;
//            } catch (IOException e) {
//                // No-Op
//                exception = e;
//            }
//        }
//
//        if (retryFlag) {
//            if (DebugConstants.DEBUG_FLAG) {
//                System.out.println("DEBUG - tbdRetrieveTimeStamp - Failed retries");
//            }
//            throw new InvalidServiceStateException(exception);
//        }
//
//        return timestampMS;
    }

    private void tbdCreateLog() throws InvalidServiceStateException {
        try (RandomAccessFile ignored = new RandomAccessFile(logPath.toFile(), "rw");
             FileChannel fileChannel = ignored.getChannel()) {
            LogRecord record = new LogRecord(cSvcNodeRoot, new byte[0], NOOP);

            FSEntry fsEntry = FSEntry.builder().record(record)
                                    .entryTimestamp(tbdEvaluateNextEntryTimestamp(true)).build();
            byte[] fsEntryBytes = SerializationUtils.serialize(fsEntry);
            LogRecordFSHeader header = new LogRecordFSHeader(1,
                                                                    fsEntryBytes.length);
            byte[] fsHeaderBytes = SerializationUtils.serialize(header);

            byte[] combinedArrays = new byte[HEADER_SIZE + fsEntryBytes.length];
            System.arraycopy(fsHeaderBytes, 0, combinedArrays, 0, fsHeaderBytes.length);
            System.arraycopy(fsEntryBytes, 0, combinedArrays, HEADER_SIZE, fsEntryBytes.length);

            int writeCount = fileChannel.write(ByteBuffer.wrap(combinedArrays));

            if (writeCount != combinedArrays.length) {
                // TODO: Error out
                throw new InvalidServiceStateException("CRITICAL ERROR - tbdCreateLog - Invalid write length");
            }
            // TODO: Write log counter?
            Files.copy(logPath, logReadPath, StandardCopyOption.REPLACE_EXISTING);

            // NOTE: Create log size metadata
            CSvcUtils.retryCheckedOperation(() -> tbdFSWriteTimestampWithContent(
                            logSizePath, ByteBuffer.allocate(Long.BYTES).putLong(Files.size(logPath)).array(), true),
                    (Exception e) -> log.warn(String.format("tbdCreateLog -" +
                            " Failed to update log size metadata [manager_id=%d]", managerId), e), 3);
        } catch (IOException e) {
            // TODO: Error out or handle
            System.out.println("CRITICAL ERROR - tbdCreateLog - Unexpected Exception");
            throw new InvalidServiceStateException(e);
        }
    }

    private boolean tbdCheckLogExists() {
        return Files.exists(this.logPath) || Files.exists(this.logTempPath) || Files.exists(this.logReadPath);
    }

    public void startManager() throws InvalidServiceStateException, IOException {
        initialize();
        isRunning = true;
    }

    public long getLogValidationToken() {
        // TODO: Evaluate how to properly couple log validation
        return -1L;
//        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
//        try (Connection conn = ConnectionPool.getConnection()) {
////            Statement tx_stmt = conn.createStatement();
////            PreparedStatement stmt = conn.prepareStatement("INSERT INTO log_record_table (row, path, record_type, data) VALUES (?, ?, ?, ?)");
//            return getLogValidationToken(conn);
//        } catch (SQLException e) {
//            System.out.println(String.format("Error - getLogValidationToken - SQLException"));
//            System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
//                    e.getSQLState()));
//            e.printStackTrace(System.out);
//            throw new RuntimeException(e);
//        }
    }

    private void tbdFSDeleteEphemeralHelperFn(Set<Long> removedOwners) {
        Set<DeleteCommand> pendingDeletes = new HashSet<>();
        long currentTimeMS = System.currentTimeMillis();
        //tbdEphemeralPendingDeletes
        for (Long ownerId : removedOwners) {
            for (String path : ephemeralOwnerToPaths.getOrDefault(ownerId, Collections.emptySet())) {
                // NOTE: Check outstanding changes
                LogRecord outstandingRecord = localOutstandingPathToRecord.get(path);
                if (!Objects.isNull(outstandingRecord) && outstandingRecord.getType().equals(DELETE)) {
                    // NOTE: Skip outstanding delete
                    if (DebugConstants.DEBUG_FLAG) {
                        log.debug(String.format("tbdFSDeleteEphemeralHelperFn -" +
                                " Skipping outstanding delete [path=%s]", path));
                    }
                    continue;
                }
//                if (tbdEphemeralPendingDeletes.getOrDefault(path, Long.MIN_VALUE) < currentTimeMS) {
                Future<?> pendingDelete = tbdEphemeralPendingDeletes.getOrDefault(path, null);
                if (Objects.isNull(pendingDelete) || pendingDelete.isDone()) {
                    log.debug(String.format("tbdFSDeleteEphemeralHelperFn -" +
                            " Adding to pending deletes [path=%s]", path));
                    pendingDeletes.add(DeleteCommand.generateDeleteCommand(
                            path, false, -1, false, false, true));
//                    tbdEphemeralPendingDeletes.put(path, currentTimeMS+((long)backgroundExecutorPeriodicity*4));
//                    tbdEphemeralPendingDeletes.put(path, currentTimeMS+((long)backgroundExecutorPeriodicity*150));
                }
            }
        }

        Future<?> deleteTask;
        for (DeleteCommand cmd : pendingDeletes) {
            deleteTask = watchExecutor.submit(() -> {
                log.info(String.format("tbdFSDeleteEphemeralHelperFn - Executing Ephemeral Delete [path=%s]",
                        cmd.getPath()));
                try {
                    if (cmd.executeManager(this).getErrorCode() == 0) {
                        log.info(String.format("tbdFSDeleteEphemeralHelperFn - Ephemeral Delete Succeeded [path=%s]",
                                cmd.getPath()));
                    }
                } catch (InvalidServiceStateException e) {
                    log.error(String.format(
                            "CRITICAL ERROR - Ephemeral Delete Failed"));
                    e.printStackTrace(System.out);
                } finally {
                    tbdEphemeralPendingDeletes.remove(cmd.getPath());
                }
            });
            tbdEphemeralPendingDeletes.put(cmd.getPath(), deleteTask);
        }
    }

    // TODO: Clean up API calls
    public boolean commitToLog(long logValidationValue, LogRecord logRecord,
                                CommitOptions options) throws InvalidServiceStateException {
        return commitToLog(logValidationValue, logRecord, options.getExpectedVersion(),
                            options.getValidateVersionFlag(), options.getSequentialFlag(), options.isDryRun());
    }

    //    public boolean commitToLog(long logValidationValue, LogRecord logRecord, int expectedVersion,
//                               boolean validateVersionFlag, boolean ephemeralFlag, boolean dryRun) {
    public boolean commitToLog(long logValidationValue, LogRecord logRecord, int expectedVersion,
                               boolean validateVersionFlag, boolean dryRun) throws InvalidServiceStateException {
        return commitToLog(logValidationValue, logRecord, expectedVersion,
                validateVersionFlag, false, dryRun);
    }

    //    public boolean commitToLog(long logValidationValue, LogRecord logRecord, boolean ephemeralFlag, boolean dryRun) {
    public boolean commitToLog(long logValidationValue, LogRecord logRecord, boolean dryRun)
                    throws InvalidServiceStateException {
//        return this.commitToLog(logValidationValue, logRecord, -1,
//                false, ephemeralFlag, dryRun);
        return this.commitToLog(logValidationValue, logRecord, -1,
                false, dryRun);
        //====
//        // TODO: remove dryflag
    }

    @Override
    public boolean addWatch(String path,
                            WatchConsumerWrapper<WatchInput> watchCallable) throws InvalidServiceStateException {
        if (!isRunning) {
            throw new InvalidServiceStateException("Manager Not Running");
        }
        addWatcherTxHelper(path, watchCallable);
        return true;
    }

    @Override
    public boolean removeWatch(String path, WatchedEvent.WatchType type) throws InvalidServiceStateException {
        if (!isRunning) {
            throw new InvalidServiceStateException("Manager Not Running");
        }
        removeWatcherTxHelper(path, type);
        return true;
    }

    private void addWatcherTxHelper(String path, WatchConsumerWrapper<WatchInput> watchCallable) {
//        Consumer<CommandOutput> watchCallable, boolean txStartedFlag) throws SQLException {
        // NOTE: Confirmed in ZK that if node does not exit, watch is not registered
        // TODO: properly implement watches after testing
        if (DebugConstants.DEBUG_FLAG) {
            log.debug(String.format(
                    "addWatcherTxHelper - adding watch [path=%s, type=%s]",
                    path, watchCallable.getWatchType()));
        }

        WatchedEvent watch = new WatchedEvent(path, watchCallable);
        watch.setManagerId(managerId);

        synchronized (watchStructMonitor) {
            // TODO: Properly design begin tx / commit
            if (Objects.isNull(pathWatchMap.get(watch.getWatchType()).get(path))) {
                pathWatchMap.get(watch.getWatchType()).put(path, new HashSet<>());
            }
            pathWatchMap.get(watch.getWatchType()).get(path).add(watch);
        }

        if (DebugConstants.DEBUG_FLAG) {
            log.debug(String.format(
                    "addWatcherTxHelper - added watch [path=%s, type=%s,\n\tpathWatchMap=%s]",
                    path, watchCallable.getWatchType(), pathWatchMap.get(watch.getWatchType()).toString()));
        }
    }

    private void removeWatcherTxHelper(String path, WatchedEvent.WatchType type) {
//        Consumer<CommandOutput> watchCallable, boolean txStartedFlag) throws SQLException {
        // NOTE: Confirmed in ZK that if node does not exit, watch is not registered
        if (DebugConstants.DEBUG_FLAG) {
            log.debug(String.format(
                    "removeWatcherTxHelper - removing watch [path=%s, type=%s]",
                    path, type));
        }

        synchronized (watchStructMonitor) {
            if (!Objects.isNull(pathWatchMap.get(type).get(path))) {
                pathWatchMap.get(type).remove(path);
            }
        }
    }

    private void addToOutstandingRecords(int transactionId, String path, LogRecord record) {
        if (transactionId <= logCounter.get()) {
            return;
        }
        if (localOutstandingTxnToPath.getReverseMap().getOrDefault(path, -1) < transactionId) {
            if (DebugConstants.DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - addToOutstandingRecords - [path=%s, txId=%d]",
                        path, transactionId));
            }
            // NOTE: Changes new or from older transaction
            localOutstandingTxnToPath.dualPut(transactionId, path);
            localOutstandingPathToRecord.put(path, record);
        }
    }

    private void removeFromOutstandingRecords(int transactionId, String path) {
        if (localOutstandingTxnToPath.getReverseMap().getOrDefault(path, -1) <= transactionId) {
            if (DebugConstants.DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - removeFromOutstandingRecords - [path=%s, txId=%d]",
                        path, transactionId));
            }
            localOutstandingTxnToPath.dualRemove(transactionId, path);
            localOutstandingPathToRecord.remove(path);
        }
    }

    private long retrieveLogFileSizeFromMetadata() throws IOException {
        byte[] byteArray = tbdFSRetrieveContent(logSizePath, false);
        return ByteBuffer.wrap(byteArray).getLong();
    }

    public CommandOutput retrieveChildren(String path, boolean statsFlag,
                                          WatchConsumerWrapper<WatchInput> watchCallable, boolean blockingFlag,
                                          boolean syncLogFlag) throws InvalidServiceStateException {
        if (!isRunning) {
            throw new InvalidServiceStateException("Manager Not Running");
        }

        CommandOutput result = new CommandOutput();
        String output = "";
        Set<String> children = null;
        int txId = logCounter.get();
        if (blockingFlag) {
            long logSize;
            try {
                logSize = CSvcUtils.retryCheckedOperation(
                        () -> Files.size(logPath),
                        (Exception e) -> log.warn(String.format("retrieveChildren " +
                                "- Failed to retrieve log size [manager_id=%d]", managerId), e),
                        3, new InvalidServiceStateException(String.format(
                                "retrieveChildren - Failed to retrieve log size [manager_id=%d]", managerId)));
            } catch (InvalidServiceStateException e) {
                logSize = CSvcUtils.retryCheckedOperation(
                        this::retrieveLogFileSizeFromMetadata,
                        (Exception ex) -> log.error(String.format("retrieveChildren -" +
                                        " Failed to retrieve log size from metadata [manager_id=%d]",
                                managerId), ex),
                        3, new InvalidServiceStateException(String.format(
                                "retrieveChildren - Failed to retrieve log size [manager_id=%d]", managerId)));
            }
//            long logSize = CSvcUtils.retryCheckedOperation(
//                    () -> Files.size(logPath),
//                    (Exception e) -> log.warn(String.format("retrieveChildren " +
//                                    "- Failed to retrieve log size [manager_id=%d]", managerId), e),
//                    5, new InvalidServiceStateException(String.format(
//                            "retrieveChildren - Failed to retrieve log size [manager_id=%d]", managerId)));
            int segmentWaitThreshold = tbdFSUpdateWriteSegmentIndex();

            final Object readWaitMonitor = new Object();
            synchronized (readWaitMonitor) {
                while ((currentReadSegmentIndex.get() < segmentWaitThreshold)
                        || (currentReadOffset < logSize && currentReadSegmentIndex.get() == segmentWaitThreshold)) {
                    try {
                        log.debug(String.format("tbdRetrieveFromLogFS - Write Wait" +
                                        " [size_threshold=%d, segment_threshold=%d]",
                                logSize, segmentWaitThreshold));
                        tbdHelperInsertWaitWrapper(FSReadWaitWrapper.builder().tbdLogSizeThreshold(logSize)
                                .tbdLogSegmentThreshold(segmentWaitThreshold)
                                .monitorObject(readWaitMonitor).build());
                        readWaitMonitor.wait();
                    } catch (InterruptedException e) {
                        if (DebugConstants.DEBUG_FLAG) {
                            log.error("retrieveChildren - Interrupted while waiting on read");
                        }
                        Thread.currentThread().interrupt();
                        throw new InvalidServiceStateException(e);
                    }
                }
            }
        }

        synchronized (tbdLogReadMonitor) {
            children = Objects.isNull(pathChildrenMap.get(path))
                        ? null
                        : Collections.unmodifiableSet(pathChildrenMap.get(path));
            if (!Objects.isNull(watchCallable)) {
                if (watchCallable.getEvaluateThresholdFlag()) {
                    watchCallable.setTxIdThreshold(txId);
                }
                addWatcherTxHelper(path, watchCallable);
            }
        }


        if (Objects.isNull(children)) {
            log.warn(String.format(
                    "retrieveChildren " +
                            "- null output - missing node [path=%s]", path));
            output = String.format("List Children Operation Failed - Missing Node.");
            result.populateError(1, output);
            return result;
        }

        result.setChildrenSet(children);
        result.setErrorCode(0);
        result.setExistFlag(true);
        return result;
    }

    public LogRecord retrieveFromCache(String path, boolean statsFlag, boolean readThroughFlag)
                                                                        throws InvalidServiceStateException {
        if (!isRunning) {
            throw new InvalidServiceStateException("Manager Not Running");
        }
        if (cacheFlag) {
            Optional<LogRecord> outstandingValue = Optional.ofNullable(localOutstandingPathToRecord.get(path));
            if (outstandingValue.isPresent()) {
                return outstandingValue.get();
            }
            Optional<LogRecord> cachedValue = recordDataCache.get(path);
            if (cachedValue.isPresent()) {
                return cachedValue.get();
            }
        }
        // TODO: Consider if returning empty delete logrecord is good enough "missing record" value
        return readThroughFlag
                ? retrieveFromLog(path, statsFlag, null)
                : new LogRecord(path, new byte[0], LogRecord.LogRecordType.DELETE);
    }

    public LogRecord retrieveFromLog(String path, boolean getStatsFlag) throws InvalidServiceStateException {
        return retrieveFromLog(path, getStatsFlag, null);
    }

    public LogRecord retrieveFromLog(String key) throws InvalidServiceStateException {
        return retrieveFromLog(key, false);
    }

    boolean initializeUniqueId() throws InvalidServiceStateException {
        CSvcUtils.retryCheckedOperation(() -> {
            managerId = new Random().nextLong();
            managerId = managerId >= 0 ? managerId : -1 * managerId;
            timestampFilePath = timestampDirPath.resolve(String.valueOf(managerId));
            Files.createFile(timestampFilePath);
            heartbeatFilePath = heartbeatDirPath.resolve(String.valueOf(managerId));
            Files.createFile(heartbeatFilePath);
            tbdFSUpdateTimestamp(heartbeatFilePath, true);
        }, (Exception e) -> {
            log.warn("initializeUniqueId " +
                    "- Failed to initialize heartbeat manager file", e);
            throw new InvalidServiceStateException(e);
        }, 20);
        return true;
//        boolean collisionFlag = true;
//        int retryCount = 0;
//        int retryMax = 20;
//        while (collisionFlag) {
//            retryCount++;
//            managerId = new Random().nextLong();
//            managerId = managerId >= 0 ? managerId : -1 * managerId;
//            try {
//                timestampFilePath = timestampDirPath.resolve(String.valueOf(managerId));
//                Files.createFile(timestampFilePath);
//                collisionFlag = false;
//            } catch (IOException e) {
//                // No-Op
//                if (retryCount >= retryMax) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }

//        retryCount = 0;
//        while (retryCount < retryMax) {
//            retryCount++;
//            try {
//                heartbeatFilePath = heartbeatDirPath.resolve(String.valueOf(managerId));
//                Files.createFile(heartbeatFilePath);
//                break;
//            } catch (FileAlreadyExistsException e) {
//                log.warn(String.format("initializeUniqueId " +
//                        "- Collision on heartbeat manager file [managerId=%d]", managerId));
//                break;
//            } catch (IOException e) {
//                // No-Op
//                if (retryCount >= retryMax) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
//        return true;
    }

    private void tbdFSClearLeaseClaimFile(int ticketValue, boolean highPriorityFlag) {
        Path claimPath = highPriorityFlag ? leasePriorityClaimDirPath : leaseClaimDirPath;
        try {
            CSvcUtils.retryCheckedOperation(() ->
                            Files.deleteIfExists(claimPath.resolve(String.valueOf(ticketValue)))
                    , (Exception e) -> log.warn(String.format("WARN - tbdFSClearLeaseClaimFile " +
                            "- Unexpected Exception [manager_id=%d]", managerId), e)
                    , 10);
        } catch (Exception ignored) {
            // NOTE: No-Op
        }
    }

    private void tbdFSClearLeaseClaimStructure() {
        leaseQueueCache.clear();
        leaseClaimTicket = -1;
    }

    private void tbdFSClearLeaseClaim(boolean highPriorityFlag) {
        log.warn(String.format("tbdFSClearLeaseClaim - Clearing lease claim [manager_id=%d, ticket=%d]",
                managerId, leaseClaimTicket));
        tbdFSClearLeaseClaimFile(leaseClaimTicket, highPriorityFlag);
        tbdFSClearLeaseClaimStructure();
    }

    private int tbdFSUpdateWriteSegmentIndex() {
        int[] currIndexWrapper = {currentWriteSegmentIndex.get()};
        Path nextSegmentPath = Paths.get(logSegmentPrefixStr.concat(String.valueOf(currIndexWrapper[0])));
        while (Files.exists(nextSegmentPath)) {
            log.warn(String.format("Non-Terminal Error - commitToLog - Log roll-over collision " +
                    "- Log segment already exists [segment_path=%s]", nextSegmentPath));
            currIndexWrapper[0]++;
            nextSegmentPath = Paths.get(logSegmentPrefixStr.concat(String.valueOf(currIndexWrapper[0])));
        }
        return currentWriteSegmentIndex.updateAndGet(currentValue -> Math.max(currentValue, currIndexWrapper[0]));
    }

    private boolean validateExecutorState() {
        return !isRunning || !((watchExecutor.isShutdown() || watchExecutor.isTerminated())
                || (backgroundLeaseClaimer.isShutdown() || backgroundLeaseClaimer.isTerminated())
                || (backgroundLogReader.isShutdown() || backgroundLogReader.isTerminated()));
    }

    private void tbdFSEvaluateTimeoutState() {
        // NOTE: - confirm heartbeat file is missing
        //       - close this manager
//        while (true) {
            if (!Files.exists(heartbeatFilePath)) {
                log.error(String.format("CRITICAL ERROR - heartbeat file missing - terminating manager" +
                                " [manager_id=%d, path=%s]",
                        managerId, heartbeatFilePath));
                initiateTermination();
            }
//            } else {
//                return;
//            }
//        }
    }

    private void tbdFSWriteTimestampWithContent(Path filePath, byte[] content, boolean overwriteFlag)
            throws IOException {
        tbdFSWriteTimestampWithContent(filePath, content, overwriteFlag, false);
    }

    private void tbdFSWriteTimestampWithContent(Path filePath, byte[] content, boolean overwriteFlag, boolean lockFlag)
            throws IOException {
        Set<StandardOpenOption> openOptions = EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.SYNC);
        if (overwriteFlag) {
            openOptions.add(StandardOpenOption.CREATE);
        } else {
            openOptions.add(StandardOpenOption.CREATE_NEW);
        }

        // NOTE: Millisecond Timestamp will be appended at start of file, followed by content
        //       MappedByteBuffer for this filePath will be added to tbdPathToBuffer
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(System.currentTimeMillis());
        buffer.flip();
        try (FileChannel fileChannel = FileChannel.open(filePath, openOptions)) {
            if (lockFlag) {
                fileChannel.lock();
            }
//            MappedByteBuffer mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,
//                                                0, Long.BYTES + content.length);
//            mappedBuffer.putLong(TIMESTAMP_BYTE_OFFSET, System.currentTimeMillis());
//            mappedBuffer.position(CONTENT_BYTE_OFFSET);
//            // TODO: REMOVEME REMOVE ME
//            // if ((off | len | (off + len) | (size - (off + len))) < 0)
////            int off = 0;
////            int len = content.length;
////            int size = content.length;
////            if ((off | len | (off + len) | (size - (off + len))) < 0) {
////                log.info(String.format("\t\t REMOVEME - tbdFSWriteTimestampWithContent " +
////                        "- checkbounds print [off=%d, len=%d, size=%d]", off, len, size));
////            }
//            mappedBuffer.put(content);
//            mappedBuffer.force();
//            CSvcUtils.unmapMappedBuffer(mappedBuffer);
//            tbdPathToBuffer.put(filePath, mappedBuffer);
            //====
            fileChannel.position(TIMESTAMP_BYTE_OFFSET);
            fileChannel.write(buffer);
            fileChannel.position(CONTENT_BYTE_OFFSET);
            fileChannel.write(ByteBuffer.wrap(content));
            fileChannel.force(true);
        }
    }

    private MappedByteBuffer tbdFSMapFileTimestampBuffer(Path filePath, boolean createFlag) throws IOException {
        Set<StandardOpenOption> openOptions = EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE);
        if (createFlag) {
            openOptions.add(StandardOpenOption.CREATE);
        }

        try (FileChannel fileChannel = FileChannel.open(filePath, openOptions)) {
            MappedByteBuffer mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,
                    0, Long.BYTES);
//            tbdPathToBuffer.put(filePath, mappedBuffer);
            return mappedBuffer;
        }
    }

    private void tbdFSUpdateTimestamp(Path filePath, long timestamp, boolean createFlag) throws InvalidServiceStateException {
        Set<StandardOpenOption> openOptions = EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.SYNC);
        if (createFlag) {
            openOptions.add(StandardOpenOption.CREATE);
        }
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        CSvcUtils.retryCheckedOperation(() -> {
//                    try {
//                        MappedByteBuffer mappedBuffer = tbdPathToBuffer.getOrDefault(filePath,
//                                tbdFSMapFileTimestampBuffer(filePath, createFlag));
                        //====
//                        MappedByteBuffer mappedBuffer = tbdFSMapFileTimestampBuffer(filePath, createFlag);
//                        mappedBuffer.putLong(TIMESTAMP_BYTE_OFFSET, timestamp);
//                        mappedBuffer.force();
                        //====
                    try (FileChannel fileChannel = FileChannel.open(filePath, openOptions)) {
                        buffer.clear();
                        buffer.putLong(timestamp);
                        buffer.flip();
                        fileChannel.position(TIMESTAMP_BYTE_OFFSET);
                        fileChannel.write(buffer);
                        fileChannel.force(true);
//                            MappedByteBuffer mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE,
//                                    0, Long.BYTES);
//                            mappedBuffer.putLong(TIMESTAMP_BYTE_OFFSET, timestamp);
//                            mappedBuffer.force();
//                            CSvcUtils.unmapMappedBuffer(mappedBuffer);
                    }
//                    } catch (InternalError ignored) {
////                        tbdFSMapFileTimestampBuffer(filePath, createFlag);
//                    }
                }, (Exception e) -> {
                    // TODO: REMOVE ME ; Was preventing spewing
                    if (e instanceof NoSuchFileException) {
                        // NOTE: No-Op
                    } else if (DebugConstants.DEBUG_FLAG
                            || (!(e instanceof ClosedByInterruptException || (!Objects.isNull(e.getCause())
                                && e.getCause() instanceof ClosedByInterruptException)))) {
                        log.error(String.format("tbdFSUpdateTimestamp - Failed retries [managerId=%d]",
                                managerId), e);
                    }
                    throw new InvalidServiceStateException(e);
                }, 3);
    }

    private void tbdFSUpdateTimestamp(Path filePath, boolean createFlag) throws InvalidServiceStateException {
        Set<StandardOpenOption> openOptions = EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.SYNC);
        if (createFlag) {
            openOptions.add(StandardOpenOption.CREATE);
        }
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        CSvcUtils.retryCheckedOperation(() -> {
//                    try {
//                        MappedByteBuffer mappedBuffer = tbdPathToBuffer.getOrDefault(filePath,
//                                tbdFSMapFileTimestampBuffer(filePath, createFlag));
                        //===
//                        MappedByteBuffer mappedBuffer = tbdFSMapFileTimestampBuffer(filePath, createFlag);
                    try (FileChannel fileChannel = FileChannel.open(filePath, openOptions)) {
                        buffer.clear();
                        buffer.putLong(System.currentTimeMillis());
                        buffer.flip();
                        fileChannel.position(TIMESTAMP_BYTE_OFFSET);
                        fileChannel.write(buffer);
                        fileChannel.force(true);
                    }
//                    } catch (InternalError ignored) {
////                        tbdFSMapFileTimestampBuffer(filePath, createFlag);
//                    }
                }, (Exception e) -> {
                    // TODO: REMOVE ME ; Was preventing spewing
                    if (e instanceof NoSuchFileException) {
                        // NOTE: No-Op
                    } else if (!(e instanceof ClosedByInterruptException
                            || e.getCause() instanceof ClosedByInterruptException)) {
                        log.error(String.format("tbdFSUpdateTimestamp - Failed retries [managerId=%d]",
                                managerId), e);
                    }
                    throw new InvalidServiceStateException(e);
                },
                3);
    }

    private long tbdFSRetrieveTimestamp(Path filePath, boolean deleteFlag) throws IOException {
        // TODO: Properly finish desing
        //   Implement fail over logic
        //   Can throw: IndexOutOfBoundsException, BufferUnderflowException, NoSuchFileException

        // NOTE: fileChannel.map() better reflects changes to target filePath in shared file systems
//        MappedByteBuffer mappedBuffer;
        long timestamp;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
//            if (fileChannel.size() < TIMESTAMP_BYTE_OFFSET + Long.BYTES) {
//                if (deleteFlag) {
//                    Files.deleteIfExists(filePath);
//                }
//                throw new FileNotFoundException(String.format("tbdFSRetrieveTimestamp -" +
//                                " File invalid size [path=%s]", filePath));
//            }
            fileChannel.position(TIMESTAMP_BYTE_OFFSET);
            if (fileChannel.read(buffer) < Long.BYTES) {
                // TODO: Error out
                if (deleteFlag) {
                    Files.deleteIfExists(filePath);
                }
                throw new FileNotFoundException(String.format("tbdFSRetrieveTimestamp -" +
                        " File invalid size [path=%s]", filePath));
            }
            buffer.flip();
            timestamp = buffer.getLong();
//            mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, TIMESTAMP_BYTE_OFFSET, Long.BYTES);
//            timestamp = mappedBuffer.getLong(TIMESTAMP_BYTE_OFFSET);
//            CSvcUtils.unmapMappedBuffer(mappedBuffer);
        }
//        CSvcUtils.unmapMappedBuffer(mappedBuffer);
        return timestamp;
    }

    // TODO: Decide level of wrappers
    private byte[] tbdFSRetrieveContent(Path filePath, boolean deleteFlag) throws IOException {
        // TODO: Can return IndexOutOfBoundsException
        // NOTE: fileChannel.map() better reflects changes to target filePath in shared file systems
        try (FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            if (fileChannel.size() - CONTENT_BYTE_OFFSET < 0) {
                if (deleteFlag) {
                    Files.deleteIfExists(filePath);
                }
                throw new IOException(String.format("tbdFSRetrieveContent - Invalid file size [file_size=%d]",
                        fileChannel.size()));
            }
            return tbdFSRetrieveContent(fileChannel);
        }
    }

    private byte[] tbdFSRetrieveContent(FileChannel fileChannel) throws IOException {
//        MappedByteBuffer mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, CONTENT_BYTE_OFFSET,
//                fileChannel.size()-CONTENT_BYTE_OFFSET);
////        mappedBuffer.position(CONTENT_BYTE_OFFSET);
//        byte[] content = new byte[mappedBuffer.remaining()];
//        mappedBuffer.get(content);
//        CSvcUtils.unmapMappedBuffer(mappedBuffer);
        //====
        ByteBuffer buffer = ByteBuffer.allocate((int)fileChannel.size()-CONTENT_BYTE_OFFSET);
        fileChannel.position(CONTENT_BYTE_OFFSET);
        fileChannel.read(buffer);
        return buffer.array();
    }

    private boolean tbdFSEvaluateMaintenanceRole() throws InvalidServiceStateException, ClosedByInterruptException {
        long currentTimeMS = tbdRetrieveCurrentTime();
        long fileModifiedTS;
        long heartbeatDelta;

        // NOTE: Validation
        if (!CSvcUtils.retryCheckedOperation(() -> {
                if (maintenanceRoleTicket <= 0) {
                    log.warn("tbdFSEvaluateMaintenanceRole - Missing Maintenance Role Ticket Value");
                    tbdFSAddMaintenanceRoleTicket();
                }
                return true;
            }, (Exception e) -> {
                log.warn(String.format("WARN - tbdFSEvaluateMaintenanceRole " +
                        "- Missing Maintenance Role Ticket Value [manager_id=%d]", managerId), e);
                return false;
            }, 10, false)) {
            return false;
        }
//        int retryCount = 0;
//        int retryMax = 10;
//        while (maintenanceRoleTicket <= 0) {
//            if (retryCount >= retryMax) {
//                return false;
//            }
//            retryCount++;
//            // TODO: Remove system.out prints; properly add log config files
//            log.warn("tbdFSEvaluateMaintenanceRole - Missing Maintenance Role Ticket Value");
//            System.out.println("tbdFSEvaluateMaintenanceRole - Missing Maintenance Role Ticket Value");
//            tbdFSAddMaintenanceRoleTicket();
//        }

        // NOTE: Touch maintenance role ticket
        if (!CSvcUtils.retryCheckedOperation(() -> {
                tbdFSUpdateTimestamp(maintenanceRoleDirPath.resolve(String.valueOf(maintenanceRoleTicket)),
                                    false);
                return true;
//                try (FileChannel fileChannel = FileChannel.open(
//                        maintenanceRoleDirPath.resolve(String.valueOf(maintenanceRoleTicket)),
//                        StandardOpenOption.WRITE)) {
//                    // TODO: REMOVE ME
//                    log.debug(String.format("\t\t REMOVE ME - tbdFSEvaluateMaintenanceRole " +
//                                    "- Maintenance ticket file [path=%s, modified_time=%d]",
//                            maintenanceRoleDirPath.resolve(String.valueOf(maintenanceRoleTicket)),
//                            Files.getLastModifiedTime(maintenanceRoleDirPath.resolve(
//                                    String.valueOf(maintenanceRoleTicket))).toMillis()));
//                    int writeCount = fileChannel.write(ByteBuffer.wrap(new byte[1]), fileChannel.size());
//                    fileChannel.truncate(fileChannel.size()-writeCount);
//                    // TODO: REMOVE ME
//                    log.debug(String.format("\t\t REMOVE ME - tbdFSEvaluateMaintenanceRole " +
//                            "- Touched maintenance ticket file [path=%s, modified_time=%d]",
//                            maintenanceRoleDirPath.resolve(String.valueOf(maintenanceRoleTicket)),
//                            Files.getLastModifiedTime(maintenanceRoleDirPath.resolve(
//                                    String.valueOf(maintenanceRoleTicket))).toMillis()));
//                    return true;
//                }
            }, (Exception e) -> {
                Throwable rootCause = CSvcUtils.unwrapRootCause(e);
                if (rootCause instanceof ClosedByInterruptException) {
                    log.debug("tbdFSEvaluateMaintenanceRole - Thread Interrupted", e);
                    throw e;
                } else if (rootCause instanceof FileNotFoundException || rootCause instanceof NoSuchFileException) {
                    log.error("tbdFSEvaluateMaintenanceRole - Missing maintenance role ticket", e);
                    maintenanceRoleTicket = -1;
                    return false;
                }
                log.warn(String.format("tbdFSEvaluateMaintenanceRole " +
                        "- Unexpected Exception [manager_id=%d]", managerId), e);
                return false;
            }, 5, false)) {
            log.warn(String.format("tbdFSEvaluateMaintenanceRole " +
                    "- Failed to update maintenance role ticket [manager_id=%d]", managerId));
            return false;
        }

//        retryCount = 0;
//        boolean retryFlag = true;
//        while (retryFlag) {
//            try (FileChannel fileChannel = FileChannel.open(
//                                            maintenanceRoleDirPath.resolve(String.valueOf(maintenanceRoleTicket)),
//                                            StandardOpenOption.WRITE)) {
//                retryCount++;
//                int writeCount = fileChannel.write(ByteBuffer.wrap(new byte[1]), fileChannel.size());
//                fileChannel.truncate(fileChannel.size()-writeCount);
//                retryFlag = false;
//            } catch (ClosedByInterruptException e) {
//                if (DebugConstants.DEBUG_FLAG) {
//                    System.out.println("DEBUG - tbdFSEvaluateMaintenanceRole - Thread Interrupted");
//                    e.printStackTrace(System.out);
//                }
//                throw e;
//            } catch (FileNotFoundException | NoSuchFileException e) {
//                // NOTE: Reset ticket value. Will be added back on next execution.
//                System.out.println("tbdFSEvaluateMaintenanceRole - ERROR - Missing maintenance role ticket");
//                maintenanceRoleTicket = -1;
//                return false;
//            } catch (IOException e) {
//                // No-Op
//                if (retryCount >= retryMax) {
//                    // TODO: Properly implement logging
//                    System.out.println("tbdFSEvaluateMaintenanceRole - Unexpected Exception");
//                    e.printStackTrace(System.out);
//                    return false;
//                }
//            }
//        }

        // NOTE: Remove timed out tickets from queue
        int currIndex = 0;
        while (currIndex < maintenanceRoleQueueCache.size()) {
            try {
                fileModifiedTS = tbdFSRetrieveTimestamp(maintenanceRoleQueueCache.get(currIndex), true);
//                fileModifiedTS = Files.getLastModifiedTime(maintenanceRoleQueueCache.get(currIndex)).toMillis();
                heartbeatDelta = currentTimeMS - fileModifiedTS;
                if (heartbeatDelta > (long) tbdFSLeaseThreshold * tbdFSHeartbeatMultiplier) {
                    Files.deleteIfExists(maintenanceRoleQueueCache.get(currIndex));
                    log.info(String.format("tbdFSEvaluateMaintenanceRole - Removed queued claim [removed_path=%s," +
                                    " heartbeat_delta=%d]", maintenanceRoleQueueCache.get(currIndex), heartbeatDelta));
                    maintenanceRoleQueueCache.remove(currIndex);
                } else {
                    break;
                }
            } catch (FileNotFoundException | NoSuchFileException e) {
                maintenanceRoleQueueCache.remove(currIndex);
            } catch (IOException e) {
                log.warn("tbdFSEvaluateMaintenanceRole - Unexpected Exception Checking maintenanceRoleQueueCache", e);
                return false;
            }
        }

        return maintenanceRoleQueueCache.size() == 0;
    }

    private int tbdFSAddMaintenanceRoleTicket() throws InvalidServiceStateException, ClosedByInterruptException {
        long currentTimeMS = tbdRetrieveCurrentTime();
        ArrayList<Path> removedPaths = new ArrayList<>();
        maintenanceRoleQueueCache = new ArrayList<>();
        int maxTicketVal = 0;
        long fileModifiedTS;
        long heartbeatDelta;
        String fileName;
        ByteBuffer buffer;
        long ownerId;
        byte[] fileBytes;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(maintenanceRoleDirPath)) {
            for (Path ticketFile : files) {
                fileName = ticketFile.getFileName().toString();
                try {
                    maxTicketVal = Math.max(maxTicketVal, Integer.parseInt(fileName));
                } catch (NumberFormatException e) {
                    log.debug(String.format("tbdFSAddMaintenanceRoleTicket -" +
                            " Invalid file found in Maintenance Role Directory [path=%s]", ticketFile));
                    continue;
                }
                try {
//                    fileModifiedTS = Files.getLastModifiedTime(ticketFile).toMillis();
                    fileModifiedTS = tbdFSRetrieveTimestamp(ticketFile, true);
                    heartbeatDelta = currentTimeMS - fileModifiedTS;
//                    maxTicketVal = Math.max(maxTicketVal, Integer.parseInt(fileName));
                    if (heartbeatDelta > (long) tbdFSLeaseThreshold * tbdFSHeartbeatMultiplier) {
//                        fileBytes = Files.readAllBytes(ticketFile);
                        // TODO: Consider more efficient set up
                        fileBytes = tbdFSRetrieveContent(ticketFile, true);
                        if (fileBytes.length >= 8) {
                            buffer = ByteBuffer.wrap(fileBytes);
                            ownerId = buffer.getLong();
                            log.info(String.format("tbdFSAddMaintenanceRoleTicket -" +
                                    " Lease heartbeat Threshold Crossed [ownerId=%d, ticket=%s, heartbeat_delta=%d]",
                                    ownerId, fileName, heartbeatDelta));
                        } else {
                            log.warn(String.format("tbdFSAddMaintenanceRoleTicket -" +
                                            " Lease heartbeat Threshold Crossed " +
                                            "- Invalid Contents [ticket=%s, heartbeat_delta=%d]",
                                    fileName, heartbeatDelta));
                        }
                        removedPaths.add(ticketFile);
                    } else {
                        maintenanceRoleQueueCache.add(ticketFile);
                    }
                } catch (FileNotFoundException e) {
                    log.warn(String.format("tbdFSAddMaintenanceRoleTicket " +
                                    "- Missing file or unexpected file size [filename=%s]", fileName), e);
                } catch (IOException e) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.warn(String.format("DEBUG - tbdFSAddMaintenanceRoleTicket -" +
                                            " Exception checking ticket file [path=%s]\n", ticketFile), e);
                    }
                }
            }
        } catch (IOException e) {
            throw new InvalidServiceStateException(e);
        }

//        boolean retryFlag = true;
        buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(managerId);

        byte[] byteArray = buffer.array();
        maxTicketVal += 1;

        int[] valueHolder = new int[]{maxTicketVal};
        CSvcUtils.retryCheckedOperation(
                () -> {
                    Path ticketPath = maintenanceRoleDirPath.resolve(String.valueOf(valueHolder[0]));
                    try {
                        tbdFSWriteTimestampWithContent(ticketPath, byteArray, false);
//                        Files.write(ticketPath, byteArray, StandardOpenOption.CREATE);
                    } catch (FileAlreadyExistsException e) {
                        maintenanceRoleQueueCache.add(ticketPath);
                        valueHolder[0]++;
                    }},
                (Exception e) -> {
                    log.error(String.format(
                            "tbdFSAddMaintenanceRoleTicket - Unexpected exception [manager_id=%d]", managerId), e);
                    throw new InvalidServiceStateException(e);
                }, 5);
        maxTicketVal = valueHolder[0];
        log.info(String.format("tbdFSAddMaintenanceRoleTicket " +
                "- Maintenance Role Ticket Added [ticket=%d, manager_id=%d]", maxTicketVal, managerId));
//        Path ticketPath;
//        int retryCount = 0;
//        int retryMax = 5;
//        while (retryFlag) {
//            ticketPath = maintenanceRoleDirPath.resolve(String.valueOf(maxTicketVal));
//            try {
//                Files.write(ticketPath, byteArray, StandardOpenOption.CREATE);
//                retryFlag = false;
//            } catch (FileAlreadyExistsException e) {
//                retryCount = 0;
//                maintenanceRoleQueueCache.add(ticketPath);
//                maxTicketVal++;
//            } catch (IOException e) {
//                retryCount++;
//                if (DebugConstants.DEBUG_FLAG) {
//                    System.out.println("tbdFSAddMaintenanceRoleTicket - Unexpected Exception");
//                    e.printStackTrace(System.out);
//                }
//                if (retryCount >= retryMax) {
//                    throw new InvalidServiceStateException(e);
//                }
//            }
//        }

        for (Path removedPath : removedPaths) {
            try {
                Files.deleteIfExists(removedPath);
            } catch (IOException e) {
                // No-Op
                if (DebugConstants.DEBUG_FLAG) {
                    System.out.println("DEBUG - tbdFSAddMaintenanceRoleTicket -" +
                            " Exception validating heartbeat files");
                    e.printStackTrace(System.out);
                }
            }
        }

        if (maintenanceRoleQueueCache.size() > 1) {
            maintenanceRoleQueueCache.sort(Comparator.comparingInt(path
                                            -> Integer.parseInt(path.getFileName().toString())));
        }

        maintenanceRoleTicket = maxTicketVal;

        if (DebugConstants.DEBUG_FLAG) {
            log.debug(String.format("tbdFSAddMaintenanceRoleTicket - Maintenance Role Ticket Added" +
                        " [ticket=%d, role_queue=%s]", maintenanceRoleTicket, maintenanceRoleQueueCache));
        }

        return maxTicketVal;
    }

    private int tbdFSAddLeaseWaitList(boolean highPriorityFlag) throws InvalidServiceStateException, ClosedByInterruptException {
        Path claimPath = highPriorityFlag ? leasePriorityClaimDirPath : leaseClaimDirPath;
        CSvcUtils.retryCheckedOperation(() -> {
            if (leaseClaimTicket > 0) {
                // NOTE: Restart
                Thread.sleep(50);
                throw new InvalidServiceStateException(String.format("CRITICAL ERROR - tbdFSAddLeaseWaitList " +
                                "- Prior lease claim not properly cleared [manager_id=%d, ticket=%d]",
                        managerId, leaseClaimTicket));
            }
        }, (Exception e) -> {
            if (DebugConstants.DEBUG_FLAG) {
                log.warn(String.format("WARN - tbdFSAddLeaseWaitList " +
                                "- Prior lease claim not properly cleared [manager_id=%d, ticket=%d]",
                        managerId, leaseClaimTicket), e);
            }
            throw new InvalidServiceStateException(String.format("CRITICAL ERROR - tbdFSAddLeaseWaitList " +
                            "- Prior lease claim not properly cleared [manager_id=%d, ticket=%d]",
                    managerId, leaseClaimTicket));
        }, 10);

//        int retryCount = 0;
//        int retryMax = 10;
//        while (leaseClaimTicket > 0) {
//            retryCount++;
//            if (retryCount == retryMax) {
//                throw new InvalidServiceStateException(String.format("CRITICAL ERROR - tbdFSAddLeaseWaitList " +
//                                "- Prior lease claim not properly cleared [managerId=%d, ticket=%d]",
//                                managerId, leaseClaimTicket));
//            }
//            if (DebugConstants.DEBUG_FLAG) {
//                System.out.println(String.format("CRITICAL ERROR - tbdFSAddLeaseWaitList " +
//                                "- Prior lease claim not properly cleared - Waiting [managerId=%d, ticket=%d]",
//                        managerId, leaseClaimTicket));
//            }
//            Thread.sleep(50);
//        }
        long currentTimeMS = tbdRetrieveCurrentTime();
        ArrayList<Path> removedPaths = new ArrayList<>();
        leaseQueueCache = new ArrayList<>();
        int maxTicketVal = 0;
        long fileModifiedTS;
        long heartbeatDelta;
        String fileName;
        ByteBuffer buffer;
        long ownerId;
        byte[] fileBytes;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(claimPath)) {
            for (Path ticketFile : files) {
                fileName = ticketFile.getFileName().toString();
                try {
                    maxTicketVal = Math.max(maxTicketVal, Integer.parseInt(fileName));
                } catch (NumberFormatException e) {
                    log.debug(String.format("tbdFSAddLeaseWaitList -" +
                            " Invalid file found in Lease Claim Directory [path=%s]", ticketFile));
                    continue;
                }
                try {
//                    fileModifiedTS = Files.getLastModifiedTime(ticketFile).toMillis();
                    fileModifiedTS = tbdFSRetrieveTimestamp(ticketFile, true);
                    heartbeatDelta = currentTimeMS - fileModifiedTS;
//                    fileName = ticketFile.getFileName().toString();
//                    maxTicketVal = Math.max(maxTicketVal, Integer.parseInt(fileName));
                    if (heartbeatDelta > (long) tbdFSLeaseThreshold * tbdLeaseMultiplier) {
//                        fileBytes = Files.readAllBytes(ticketFile);
                        fileBytes = tbdFSRetrieveContent(ticketFile, true);
                        if (fileBytes.length >= Long.BYTES) {
                            buffer = ByteBuffer.wrap(fileBytes);
                            ownerId = buffer.getLong();
                            if (DebugConstants.DEBUG_FLAG) {
                                log.debug(String.format("tbdFSAddLeaseWaitList -" +
                                        " Lease heartbeat Threshold Crossed [owner_id=%d, ticket=%s," +
                                        " heartbeatDelta=%d]", ownerId, fileName, heartbeatDelta));
                            }
                        }
                        removedPaths.add(ticketFile);
                    } else {
                        leaseQueueCache.add(ticketFile);
                    }
                } catch (FileNotFoundException e) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.warn(String.format("tbdFSAddLeaseWaitList " +
                                        "- Missing file or unexpected file size [filename=%s]", fileName), e);
                    }
                } catch (IOException e) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.warn(String.format("tbdFSAddLeaseWaitList -" +
                                " Error checking lease claim timestamp [ticket=%s]", fileName), e);
                    }
                }
            }
        } catch (IOException e) {
            if (DebugConstants.DEBUG_FLAG) {
                log.warn(String.format("tbdFSAddLeaseWaitList -" +
                        " Error accessing lease claim directory [directory_path=%s]", claimPath), e);
            }
            throw new InvalidServiceStateException(e);
        }

        boolean retryFlag = true;
        buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(managerId);

        byte[] byteArray = buffer.array();
        maxTicketVal += 1;

        int[] valueHolder = new int[]{maxTicketVal};
        CSvcUtils.retryCheckedOperation(
                () -> {
                    Path ticketPath = claimPath.resolve(String.valueOf(valueHolder[0]));
                    try {
//                        Files.write(ticketPath, byteArray, StandardOpenOption.CREATE);
                        tbdFSWriteTimestampWithContent(ticketPath, byteArray, false);
                    } catch (FileAlreadyExistsException e) {
                        leaseQueueCache.add(ticketPath);
                        valueHolder[0]++;
                    }},
                (Exception e) -> {
                    log.error(String.format(
                            "tbdFSAddLeaseWaitList - Unexpected exception [manager_id=%d]", managerId), e);
                    throw new InvalidServiceStateException(e);
                }, 5);
        maxTicketVal = valueHolder[0];

//        Path ticketPath;
//        while (retryFlag) {
//            ticketPath = claimPath.resolve(String.valueOf(maxTicketVal));
//            try {
//                Files.write(ticketPath,
//                            byteArray, StandardOpenOption.CREATE);
//                retryFlag = false;
//            } catch (FileAlreadyExistsException e) {
//                leaseQueueCache.add(ticketPath);
//                maxTicketVal++;
//            } catch (IOException e) {
//                log.warn("tbdFSAddLeaseWaitList - Unexpected Exception", e);
//            }
//        }

        for (Path removedPath : removedPaths) {
            try {
                Files.deleteIfExists(removedPath);
            } catch (IOException e) {
                // No-Op
            }
        }

        if (leaseQueueCache.size() > 1) {
            leaseQueueCache.sort(Comparator.comparingInt(path -> Integer.parseInt(path.getFileName().toString())));
        }

        leaseClaimTicket = maxTicketVal;
        if (DebugConstants.DEBUG_FLAG) {
            log.debug(String.format("tbdFSAddLeaseWaitList " +
                            "- Added Lease Claim [manager_id=%d, ticket=%d,\n\tqueue=%s]",
                    managerId, leaseClaimTicket, leaseQueueCache));
        }
        return maxTicketVal;
    }

    private boolean ARCHIVEDtbdFSManageSoftLeaseClaim(int claimTicket)
            throws InvalidServiceStateException, InterruptedException, ClosedByInterruptException,
            FileNotFoundException, NoSuchFileException {
        long currentTimeMS = tbdRetrieveCurrentTime();
        long fileModifiedTS;
        long heartbeatDelta;
        int sleepMS = 0;
        // NOTE: Validation
        if (claimTicket <= 0) {
            // TODO: Remove system.out prints; properly add log config files
            log.warn("tbdFSManageSoftLeaseClaim - Missing Lease Claim Ticket Value");
            return false;
        }

        if (!Files.exists(leaseClaimDirPath.resolve(Integer.toString(claimTicket)))) {
            throw new InvalidServiceStateException("tbdFSManageSoftLeaseClaim - Missing Lease Claim Ticket File");
        }

        try {
            CSvcUtils.retryCheckedOperation(() -> {
                // NOTE: Touch lease claim ticket file
//                try {
                tbdFSUpdateTimestamp(leaseClaimDirPath.resolve(Integer.toString(claimTicket)), false);
//                    return true;
//                } catch (InvalidServiceStateException e) {
//                    // TODO: Improve logic readability
//                    if (!Objects.isNull(e.getCause())
//                            && (e.getCause() instanceof NoSuchFileException
//                            || e.getCause() instanceof  FileNotFoundException)) {
//                        return false;
//                    }
//                    throw e;
//                }
                //==
//            try (FileChannel fileChannel = FileChannel.open(
//                    leaseClaimDirPath.resolve(Integer.toString(claimTicket)), StandardOpenOption.WRITE)) {
//                int writeCount = fileChannel.write(ByteBuffer.wrap(new byte[1]), fileChannel.size());
//                fileChannel.truncate(fileChannel.size()-writeCount);
//                return true;
//            }
            }, (Exception e) -> {
                Throwable rootCause = CSvcUtils.unwrapRootCause(e);
//                if (e instanceof NoSuchFileException) {
                if (rootCause instanceof NoSuchFileException) {
                    // NOTE: No-Op
                    throw e;
//                } else if (DebugConstants.DEBUG_FLAG
//                        || (!(e instanceof ClosedByInterruptException || (!Objects.isNull(e.getCause())
//                        && e.getCause() instanceof ClosedByInterruptException)))) {
                } else if (DebugConstants.DEBUG_FLAG
                        || (!(rootCause instanceof ClosedByInterruptException))) {
                    log.warn(String.format("WARN - tbdFSManageSoftLeaseClaim " +
                            "- Failed to extend lease claim ticket [manager_id=%d]", managerId), e);
                }
//                return false;
            }, 3);
        } catch (InvalidServiceStateException e) {
            Throwable rootCause = CSvcUtils.unwrapRootCause(e);
            if ((rootCause instanceof NoSuchFileException
                    || rootCause instanceof  FileNotFoundException)) {
                throw e;
            } else {
                return false;
            }
        }
        //==
//        if (!CSvcUtils.retryCheckedOperation(() -> {
//            // NOTE: Touch lease claim ticket file
//            try {
//                tbdFSUpdateTimestamp(leaseClaimDirPath.resolve(Integer.toString(claimTicket)), false);
//                return true;
//            } catch (InvalidServiceStateException e) {
//                // TODO: Improve logic readability
//                if (!Objects.isNull(e.getCause())
//                        && (e.getCause() instanceof NoSuchFileException
//                            || e.getCause() instanceof  FileNotFoundException)) {
//                    return false;
//                }
//                throw e;
//            }
//            //==
////            try (FileChannel fileChannel = FileChannel.open(
////                    leaseClaimDirPath.resolve(Integer.toString(claimTicket)), StandardOpenOption.WRITE)) {
////                int writeCount = fileChannel.write(ByteBuffer.wrap(new byte[1]), fileChannel.size());
////                fileChannel.truncate(fileChannel.size()-writeCount);
////                return true;
////            }
//        }, (Exception e) -> {
//            if (e instanceof NoSuchFileException) {
//                // NOTE: No-Op
//            } else if (DebugConstants.DEBUG_FLAG
//                    || (!(e instanceof ClosedByInterruptException || (!Objects.isNull(e.getCause())
//                    && e.getCause() instanceof ClosedByInterruptException)))) {
//                log.warn(String.format("WARN - tbdFSManageSoftLeaseClaim " +
//                        "- Failed to extend lease claim ticket [manager_id=%d]", managerId), e);
//            }
//            return false;
//        }, 3, false)) {
////            return false;
//            throw new InvalidServiceStateException("Unable to extend ");
//        }
        //==

        // NOTE: Touch lease claim ticket file
//        int retryCount = 0;
//        int retryMax = 10;
//        while (retryCount < retryMax) {
//            retryCount++;
//            try (FileChannel fileChannel = FileChannel.open(
//                    leaseClaimDirPath.resolve(Integer.toString(claimTicket)), StandardOpenOption.WRITE)) {
//                int writeCount = fileChannel.write(ByteBuffer.wrap(new byte[1]), fileChannel.size());
//                fileChannel.truncate(fileChannel.size()-writeCount);
//                break;
//            } catch (ClosedByInterruptException | FileNotFoundException | NoSuchFileException e) {
//                return false;
//            } catch (IOException e) {
//                // No-Op
//                if (retryCount >= retryMax) {
//                    // TODO: Properly implement logging
//                    e.printStackTrace(System.out);
//                    return false;
//                }
//            }
//        }

        int currIndex = 0;
        while (currIndex < leaseQueueCache.size()) {
            try {
//                fileModifiedTS = Files.getLastModifiedTime(leaseQueueCache.get(currIndex)).toMillis();
                fileModifiedTS = tbdFSRetrieveTimestamp(leaseQueueCache.get(currIndex), true);
                heartbeatDelta = currentTimeMS - fileModifiedTS;
                if (heartbeatDelta > (long) tbdFSLeaseThreshold * tbdLeaseMultiplier) {
                    Files.deleteIfExists(leaseQueueCache.get(currIndex));
                    leaseQueueCache.remove(currIndex);
                } else {
                    break;
                }
            } catch (FileNotFoundException | NoSuchFileException e) {
                leaseQueueCache.remove(currIndex);
            } catch (IOException e) {
                log.warn("tbdFSManageSoftLeaseClaim - Unexpected Exception Checking leaseQueueCache", e);
                return false;
            }
        }

        if (leaseQueueCache.size() == 0) {
            if (claimSoftLease(false)) {
                if (DebugConstants.DEBUG_FLAG) {
                    log.debug(String.format("tbdFSManageSoftLeaseClaim - Claimed Soft Lease [manager_id=%d]",
                            managerId));
                }
                return true;
            }
        } else if (leaseQueueCache.size() > 3) {
            sleepMS = Math.min(leaseQueueCache.size() * 20, 100) * tbdLeaseMultiplier;
        }

        if (sleepMS > 0) {
            try {
                Thread.sleep(sleepMS);
            } catch (InterruptedException e) {
                if (DebugConstants.DEBUG_FLAG) {
                    log.error("ERROR - tbdFSManageSoftLeaseClaim - Interrupted", e);
                }
                throw e;
            }
        }
        return false;
    }

    private boolean evaluateHighPriorityClaimsQueue() throws InvalidServiceStateException {
        long currentTimeMS = System.currentTimeMillis();
        ArrayList<Path> removedPaths = new ArrayList<>();
        String fileName;
        long fileModifiedTS;
        long heartbeatDelta;
        boolean emptyQueueFlag = true;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(leasePriorityClaimDirPath)) {
            for (Path ticketFile : files) {
                fileName = ticketFile.getFileName().toString();
                try {
                    fileModifiedTS = tbdFSRetrieveTimestamp(ticketFile, true);
                    heartbeatDelta = currentTimeMS - fileModifiedTS;
                    if (heartbeatDelta > (long) tbdFSLeaseThreshold * tbdLeaseMultiplier) {
                        if (DebugConstants.DEBUG_FLAG) {
                            log.debug(String.format("tbdCheckHighPrioClaims -" +
                                    " Priority lease claim heartbeat threshold crossed [ticket=%s," +
                                    " heartbeatDelta=%d]", fileName, heartbeatDelta));
                        }
                        removedPaths.add(ticketFile);
                    } else {
                        log.debug(String.format("tbdCheckHighPrioClaims -" +
                                " Priority lease claim detected [ticket=%s," +
                                " heartbeatDelta=%d]", fileName, heartbeatDelta));
                        emptyQueueFlag = false;
                    }
                } catch (FileNotFoundException e) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.warn(String.format("tbdCheckHighPrioClaims " +
                                "- Missing file or unexpected file size [filename=%s]", fileName), e);
                    }
                } catch (IOException e) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.warn(String.format("tbdCheckHighPrioClaims -" +
                                " Error checking lease claim timestamp [ticket=%s]", fileName), e);
                    }
                }
            }
        } catch (IOException e) {
            if (DebugConstants.DEBUG_FLAG) {
                log.warn(String.format("tbdCheckHighPrioClaims -" +
                        " Error accessing lease claim directory [directory_path=%s]", leasePriorityClaimDirPath), e);
            }
            throw new InvalidServiceStateException(e);
        }

        for (Path removedPath : removedPaths) {
            try {
                Files.deleteIfExists(removedPath);
            } catch (IOException e) {
                // No-Op
            }
        }

        return emptyQueueFlag;
    }

    private boolean tbdFSManageSoftLeaseClaim(int claimTicket, boolean highPriorityFlag)
            throws InvalidServiceStateException, InterruptedException, FileNotFoundException, NoSuchFileException {
        Path claimPath = highPriorityFlag ? leasePriorityClaimDirPath : leaseClaimDirPath;
        long currentTimeMS = tbdRetrieveCurrentTime();
        long fileModifiedTS;
        long heartbeatDelta;
        int sleepMS = 0;
        // NOTE: Validation
        if (claimTicket <= 0) {
            // TODO: Remove system.out prints; properly add log config files
            log.warn("tbdFSManageSoftLeaseClaim - Missing Lease Claim Ticket Value");
            return false;
        }

        if (!Files.exists(claimPath.resolve(Integer.toString(claimTicket)))) {
            throw new InvalidServiceStateException("tbdFSManageSoftLeaseClaim - Missing Lease Claim Ticket File");
        }

        try {
            CSvcUtils.retryCheckedOperation(() -> {
                // NOTE: Touch lease claim ticket file
                tbdFSUpdateTimestamp(claimPath.resolve(Integer.toString(claimTicket)), false);
            }, (Exception e) -> {
                throw e;
            }, 3);
        } catch (InvalidServiceStateException e) {
            Throwable rootCause = CSvcUtils.unwrapRootCause(e);
            if (DebugConstants.DEBUG_FLAG || !(rootCause instanceof ClosedByInterruptException)) {
                log.warn(String.format("WARN - tbdFSManageSoftLeaseClaim " +
                        "- Failed to extend lease claim ticket [manager_id=%d]", managerId), e);
            }

            if ((rootCause instanceof NoSuchFileException
                    || rootCause instanceof  FileNotFoundException)) {
                // NOTE: Bubble up deleted claim ticket exception
                throw e;
            } else {
                return false;
            }
        }

//        int currIndex = 0;
//        while (currIndex < leaseQueueCache.size()) {
//            try {
//                fileModifiedTS = tbdFSRetrieveTimestamp(leaseQueueCache.get(currIndex), true);
//                heartbeatDelta = currentTimeMS - fileModifiedTS;
//                if (heartbeatDelta > (long) tbdFSLeaseThreshold * tbdLeaseMultiplier) {
//                    Files.deleteIfExists(leaseQueueCache.get(currIndex));
//                    leaseQueueCache.remove(currIndex);
//                } else {
//                    break;
//                }
//            } catch (FileNotFoundException | NoSuchFileException e) {
//                leaseQueueCache.remove(currIndex);
//            } catch (IOException e) {
//                log.warn("tbdFSManageSoftLeaseClaim - Unexpected Exception Checking leaseQueueCache", e);
//                return false;
//            }
//        }
//        int currIndex = 0;
        while (!leaseQueueCache.isEmpty()) {
            try {
                fileModifiedTS = tbdFSRetrieveTimestamp(leaseQueueCache.get(0), true);
                heartbeatDelta = currentTimeMS - fileModifiedTS;
                if (heartbeatDelta > (long) tbdFSLeaseThreshold * tbdLeaseMultiplier) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.debug(String.format("tbdFSManageSoftLeaseClaim -" +
                                " Removed queued claim [path=%s, " +
                                " heartbeat_delta=%d]", leaseQueueCache.get(0), heartbeatDelta));
                    }
                    Files.deleteIfExists(leaseQueueCache.get(0));
                    leaseQueueCache.remove(0);
                } else {
                    break;
                }
            } catch (FileNotFoundException | NoSuchFileException e) {
                leaseQueueCache.remove(0);
            } catch (IOException e) {
                log.warn("tbdFSManageSoftLeaseClaim - Unexpected Exception Checking leaseQueueCache", e);
                return false;
            }
        }

        if (leaseQueueCache.size() == 0) {
            // NOTE: Check high priority queue
//            boolean priorityClaimsEmptyFlag = evaluateHighPriorityClaims();
            if (DebugConstants.DEBUG_FLAG) {
                log.debug(String.format("tbdFSManageSoftLeaseClaim - Evaluating High Priority Claims [manager_id=%d]",
                        managerId));
            }
            if (highPriorityFlag || evaluateHighPriorityClaimsQueue()) {
                if (claimSoftLease(false)) {
                    if (DebugConstants.DEBUG_FLAG) {
                        log.debug(String.format("tbdFSManageSoftLeaseClaim - Claimed Soft Lease [manager_id=%d]",
                                managerId));
                    }
                    return true;
                }
            }
        } else if (leaseQueueCache.size() > 3) {
            sleepMS = Math.min(leaseQueueCache.size() * 20, 100) * tbdLeaseMultiplier;
        }

        if (sleepMS > 0) {
            try {
                Thread.sleep(sleepMS);
            } catch (InterruptedException e) {
                if (DebugConstants.DEBUG_FLAG) {
                    log.error("ERROR - tbdFSManageSoftLeaseClaim - Interrupted", e);
                }
                throw e;
            }
        } else {
            // NOTE: Sleep 1.0ms
            LockSupport.parkNanos(1000000);
        }
        return false;
    }

    private boolean tbdFSCheckActiveRecoveryState() {
        if (!Files.exists(recoveryLeasePath)) {
            return false;
        }

        try {
            long timeDelta = tbdRetrieveCurrentTime() - tbdFSRetrieveTimestamp(recoveryLeasePath, false);
            return timeDelta < RECOVERY_TS_DELTA_THRESHOLD;
        } catch (FileNotFoundException | NoSuchFileException e) {
            return false;
        } catch (IOException e) {
            log.warn(String.format("tbdFSCheckActiveRecoveryState -" +
                    " Error determining timestamp delta from log recovery -" +
                    " Defaulting to ongoing recovery [manager_id=%d, path=%s]",
                    managerId, recoveryLeasePath), e);
            return true;
        }
    }

    public boolean tbdFSClaimSoftLease(boolean waitFlag) throws InvalidServiceStateException {
        // TODO: Check recovery lease path and its timestamp
        while (tbdFSCheckActiveRecoveryState()) {
            if (!waitFlag) {
                return false;
            }
        }
        boolean leaseOccupiedFlag = Files.exists(leasePath);

        long heartbeatDelta;
        while (leaseOccupiedFlag) {
            try {
                heartbeatDelta = tbdRetrieveCurrentTime() - tbdFSRetrieveTimestamp(leasePath, true);
                leaseOccupiedFlag = Files.exists(leasePath)
                                && heartbeatDelta < (long) tbdFSLeaseThreshold * tbdLeaseMultiplier;
                log.debug(String.format("tbdFSClaimSoftLease - Lease Occupied [heartbeat_delta=%d," +
                        " heartbeat_threshold=%d]", heartbeatDelta, (long) tbdFSLeaseThreshold * tbdLeaseMultiplier));
                if (leaseOccupiedFlag && !waitFlag) {
                    // NOTE: Return False if No-Wait and lease in-use
                    return false;
                } else if (leaseOccupiedFlag) {
                    // NOTE: Sleep 1ms if lease in-use
                    LockSupport.parkNanos(1000000);
                }
            } catch (IOException e) {
                // No-Op
                leaseOccupiedFlag = Files.exists(leasePath);
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(managerId);

        CSvcUtils.retryCheckedOperation(
                () -> tbdFSWriteTimestampWithContent(leasePath, buffer.array(), true, true),
//                () -> Files.write(leasePath, buffer.array(), StandardOpenOption.CREATE,
//                            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING),
                (Exception e) -> {
                    log.error(String.format(
                            "tbdFSClaimSoftLease - Unexpected exception [managerId=%d]", managerId), e);
                    throw e;
                },
                5);

        try {
            return Arrays.equals(buffer.array(), tbdFSRetrieveContent(leasePath, false));
        } catch (IOException e) {
            throw new InvalidServiceStateException(e);
        }
    }

    public boolean tbdFSExtendSoftLease() throws InvalidServiceStateException {
        return CSvcUtils.retryCheckedOperation(() -> {
            long leaseOwnerId = ByteBuffer.wrap(tbdFSRetrieveContent(leasePath, false)).getLong();
            if (leaseOwnerId != managerId) {
                // NOTE: Restart
                throw new InvalidServiceStateException(String.format("tbdFSExtendSoftLease -" +
                        " Unexpected lease owner [lease_owner_id=%d, manager_id=%d]", leaseOwnerId, managerId));
            }

            tbdFSUpdateTimestamp(leasePath, false);
            return true;
        }, (Exception e) -> {
            Throwable rootCause = CSvcUtils.unwrapRootCause(e);
            if (DebugConstants.DEBUG_FLAG) {
                log.warn(String.format("DEBUG - tbdFSExtendSoftLease " +
                        "- Failed to extend lease [manager_id=%d]", managerId), e);
            } else if (!(rootCause instanceof NoSuchFileException
                    || rootCause instanceof ClosedByInterruptException)) {
                log.warn(String.format("tbdFSExtendSoftLease " +
                        "- Failed to extend lease [manager_id=%d]", managerId), e);
            }
            return false;
        }, 10, false);
    }

    public Future<Boolean> submitSoftLeaseClaim() {
        // TODO: REFACTOR THIS ; REMOVEME
        //       Not used
        throw new NotImplementedException();
//        return backgroundLeaseClaimer.submit(new FSLeaseClaimer(this));
    }

    @Override
    public Future<ManagerLeaseHandlerBase> claimSoftLeaseHandler(boolean highPriorityFlag) {
        CSvcUtils.PrioritizedTask<ManagerLeaseHandlerBase> task = new CSvcUtils.PrioritizedTask<>(
                highPriorityFlag ? 0 : 1,
                new TbdTestLeaseClaimer(this, highPriorityFlag));
        backgroundLeaseClaimer.execute(task);
        return task;
//        return backgroundLeaseClaimer.submit(new TbdTestLeaseClaimer(this, highPriorityFlag));
    }

    public boolean claimSoftLease() throws InvalidServiceStateException {
        // NOTE: NOT USED
        return claimSoftLease(true);
    }

    public boolean claimSoftLease(boolean waitFlag) throws InvalidServiceStateException {
        return tbdFSClaimSoftLease(waitFlag);
    }

    public boolean releaseSoftLease() {
        if (DebugConstants.DEBUG_FLAG) {
            System.out.println("DEBUG - releaseSoftLease - Trying to release lease");
        }
        try (FileChannel fileChannel = FileChannel.open(leasePath, StandardOpenOption.WRITE, StandardOpenOption.READ);
             FileLock ignored = fileChannel.lock()) {

//            byte[] leaseOwnerBytes = Files.readAllBytes(leasePath);
            byte[] leaseOwnerBytes = tbdFSRetrieveContent(fileChannel);
            if (leaseOwnerBytes.length < Long.BYTES) {
                if (DebugConstants.DEBUG_FLAG) {
                    System.out.format("DEBUG - releaseSoftLease " +
                            "- Failed to release lease [lease_content_length=%d]\n", leaseOwnerBytes.length);
                }
                return false;
            } else {
                if (ByteBuffer.wrap(leaseOwnerBytes).getLong() == managerId) {
                    Files.delete(leasePath);
                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println("DEBUG - releaseSoftLease - Successful Lease Release");
                    }
                    return true;
                }
            }
        } catch (IOException e) {
            if (DebugConstants.DEBUG_FLAG) {
                log.warn("DEBUG - releaseSoftLease - Failed to release lease", e);
                log.info("DEBUG - releaseSoftLease - Continuing execution");
            }
        } catch (Exception e) {
            log.warn("releaseSoftLease - Failed to release lease - Unexpected Exception", e);
        }
        return false;
    }

    // TODO: remove outdate apis from interface
    public void releaseLease() {
    }

    public boolean claimLease() throws IOException {
        // TODO: Currently redesigned. Check ARCHIVE for past versions
        return true;
    }

    public boolean validateEntry() {
        return false;
    }

//    static class FSLeaseClaimer implements Callable<Boolean> {
//
//        CSvcManagerFileSystem_CmdAgnostic manager;
//
//        public FSLeaseClaimer(CSvcManagerFileSystem_CmdAgnostic manager) {
//            this.manager = manager;
//        }
//
//        public Boolean call() {
//            int claimTicket;
//            try {
//                claimTicket = manager.tbdFSAddLeaseWaitList(false);
//            } catch (Exception e) {
//                log.warn(String.format("FSLeaseClaimer - Exception"));
//                log.warn(String.format("===\nSTACKTRACE\n==="), e);
//                manager.tbdFSClearLeaseClaim(false);
//                return false;
//            }
//
//            boolean leaseFlag = false;
//            while (manager.isRunning() && !Thread.currentThread().isInterrupted()) {
//                try {
//                    if (manager.tbdFSManageSoftLeaseClaim(claimTicket, false)) {
//                        leaseFlag = true;
//                        break;
//                    } else if (manager.leaseClaimTicket <= 0
//                                || manager.leaseClaimTicket != claimTicket) {
//                        break;
//                    }
//                } catch (FileNotFoundException | NoSuchFileException | InterruptedException e) {
//                    manager.tbdFSClearLeaseClaim(false);
//                    return false;
//                } catch (Exception e) {
//                    // No-Op
//                    log.warn(String.format("FSLeaseClaimer - Exception"));
//                    log.warn(String.format("===\nSTACKTRACE\n==="), e);
//                }
//            }
//            if (manager.leaseClaimTicket == claimTicket) {
//                manager.tbdFSClearLeaseClaim(false);
//            }
//            return leaseFlag;
//        }
//    }

    static class TbdTestLeaseClaimer implements Callable<ManagerLeaseHandlerBase> {

        DataLayerMgrSharedFS manager;
        boolean highPriorityFlag;

        public TbdTestLeaseClaimer(DataLayerMgrSharedFS manager, boolean highPriorityFlag) {
            this.manager = manager;
            this.highPriorityFlag = highPriorityFlag;
        }

        @Override
        public ManagerLeaseHandlerBase call() {
//            return null;
            int claimTicket;
            try {
                claimTicket = manager.tbdFSAddLeaseWaitList(highPriorityFlag);
            } catch (Exception e) {
                log.warn(String.format("FSLeaseClaimer - Exception"));
                log.warn(String.format("===\nSTACKTRACE\n==="), e);
                manager.tbdFSClearLeaseClaim(highPriorityFlag);
                // TODO: Consider best fit throwable
                throw new CompletionException(e);
//                return false;
            }

            boolean leaseFlag = false;
            TbdFSTestLeaseHandler leaseHandler = null;
            while (manager.isRunning() && !Thread.currentThread().isInterrupted()) {
                try {
                    if (manager.tbdFSManageSoftLeaseClaim(claimTicket, highPriorityFlag)) {
                        leaseFlag = true;
                        leaseHandler = new TbdFSTestLeaseHandler(manager, claimTicket,
                                                                manager.backgroundLeaseExtender, highPriorityFlag);
                        break;
                    } else if (manager.leaseClaimTicket <= 0
                            || manager.leaseClaimTicket != claimTicket) {
                        break;
                    }
                } catch (InterruptedException e) {
                    manager.tbdFSClearLeaseClaim(highPriorityFlag);
                    Thread.currentThread().interrupt();
                    throw new CompletionException(e);
//                    return false;
                } catch (InvalidServiceStateException e) {
                    manager.tbdFSClearLeaseClaim(highPriorityFlag);
                    throw new CompletionException(e);
                } catch (Exception e) {
                    // No-Op
                    log.warn(String.format("FSLeaseClaimer - Exception"));
                    log.warn(String.format("===\nSTACKTRACE\n==="), e);
                }
            }
            if (leaseFlag && !Objects.isNull(leaseHandler)) {
                manager.tbdFSClearLeaseClaimStructure();
                return leaseHandler;
//                return new FSLeaseHandler(manager);
            } else {
                manager.tbdFSClearLeaseClaim(highPriorityFlag);
                log.error(String.format("FSLeaseClaimer - Failed to retrieve lease [manager_id=%d]",
                        manager.managerId));
                throw new IllegalStateException("FSLeaseClaimer - Failed to retrieve lease");
            }
//            if (manager.leaseClaimTicket == claimTicket) {
//                manager.tbdFSClearLeaseClaim();
//            }
//            return leaseFlag;
        }
    }

    @Slf4j
    static class TbdFSTestLeaseHandler extends ManagerLeaseHandlerBase {
//        private ExecutorService leaseExtender;
        private DataLayerMgrSharedFS manager;
        private boolean highPriorityFlag;
        private int ticketValue;
        private Future<?> taskFuture;

        public TbdFSTestLeaseHandler(DataLayerMgrSharedFS manager,
                                     int ticketValue, ExecutorService executor, boolean highPriorityFlag) {
            this.highPriorityFlag = highPriorityFlag;
            this.manager = manager;
            this.ticketValue = ticketValue;
//            leaseExtender = Executors.newSingleThreadExecutor();
//            leaseExtender = executor;
            TbdLeaseHandlerFSTestRunnable leaseRunnable = new TbdLeaseHandlerFSTestRunnable(this.manager,
                    this.ticketValue, this.highPriorityFlag);
//            Future<?> test = leaseExtender.submit(leaseRunnable);
            taskFuture = executor.submit(leaseRunnable);
        }

        @Override
        public void close() {
            // TODO: Properly handle errors and logging
            if (DebugConstants.DEBUG_FLAG) {
                log.debug(String.format("FSLeaseHandler - Closing"));
            }
//            leaseExtender.shutdownNow();
            taskFuture.cancel(true);
            manager.tbdFSClearLeaseClaimFile(ticketValue, highPriorityFlag);
            manager.releaseSoftLease();
        }
    }

    @Slf4j
    static class TbdLeaseHandlerFSTestRunnable implements Runnable {
        private boolean highPriorityFlag;
        private DataLayerMgrSharedFS manager;
        private int claimTicket;

        TbdLeaseHandlerFSTestRunnable(DataLayerMgrSharedFS manager,
                                      int claimTicket, boolean highPriorityFlag) {
            this.highPriorityFlag = highPriorityFlag;
            this.manager = manager;
            this.claimTicket = claimTicket;
            if (DebugConstants.DEBUG_FLAG) {
                log.debug("New LeaseHandlerRunnable instance");
            }
        }

        @Override
        public void run() {
            Path claimPath = highPriorityFlag ? manager.leasePriorityClaimDirPath : manager.leaseClaimDirPath;
            if (DebugConstants.DEBUG_FLAG) {
                log.debug("LeaseHandler - Starting runnable");
            }
            int failureCount = 0;
            int failureMax = 5;
            while (manager.isRunning() && !Thread.currentThread().isInterrupted()) {
                if (DebugConstants.DEBUG_FLAG) {
                    log.debug("LeaseHandler - Starting loop");
                }

                try {
                    CSvcUtils.retryCheckedOperation(() -> {
                        // NOTE: Touch lease claim ticket file
                        manager.tbdFSUpdateTimestamp(claimPath.resolve(Integer.toString(claimTicket)),
                                                    false);
                    }, (Exception e) -> {
                        Throwable rootCause = CSvcUtils.unwrapRootCause(e);
                        if (DebugConstants.DEBUG_FLAG || (!(e instanceof ClosedByInterruptException
                                || (rootCause instanceof ClosedByInterruptException)))) {
                            log.warn(String.format("WARN - TbdLeaseHandlerFSTestRunnable " +
                                    "- Failed to extend lease claim ticket [manager_id=%d]", manager.managerId), e);
                        }
                        throw e;
                    }, 3);
                } catch (InvalidServiceStateException e) {
                    Throwable rootCause = CSvcUtils.unwrapRootCause(e);
                    if (DebugConstants.DEBUG_FLAG
                            || (!(rootCause instanceof ClosedByInterruptException))) {
//                            || (!(!Objects.isNull(e.getCause()) && e.getCause() instanceof ClosedByInterruptException))) {
                        log.warn("LeaseHandler - CRITICAL ERROR - Failure in background thread", e);
                    }
                }

                try {
                    if (!manager.tbdFSExtendSoftLease()) {
                        if (DebugConstants.DEBUG_FLAG) {
                            log.warn(String.format("DEBUG - LeaseHandler " +
                                            "- Failed to renew lease [managerId=%d]",
                                    manager.getManagerId()));
                        }
                        failureCount++;
                        if (failureCount >= failureMax) {
                            log.error(String.format("CRITICAL ERROR - LeaseHandler " +
                                            "- Failed to renew lease - Terminating [managerId=%d]",
                                    manager.getManagerId()));
                            return;
                        }
                    } else {
                        failureCount = 0;
                    }
                    // TODO: Add sleep by evaluating lease length * multiplier and sleeping if value > 20 ms
                    if (manager.tbdFSLeaseThreshold * manager.tbdLeaseMultiplier > 15) {
                        Thread.sleep(((long) (manager.tbdFSLeaseThreshold * manager.tbdLeaseMultiplier * 0.25)));
                    }
                } catch (InterruptedException e) {
                    // NOTE: Lease Handler Closed
                    if (DebugConstants.DEBUG_FLAG) {
                        log.warn("DEBUG - LeaseHandler - Failure in background thread", e);
                    }
                    return;
                } catch (InvalidServiceStateException e) {
                    log.warn("LeaseHandler - Failure in background thread", e);
                } catch (Exception e) {
                    log.warn("LeaseHandler - CRITICAL ERROR - Failure in background thread", e);
                }
            }
            if (DebugConstants.DEBUG_FLAG) {
                log.debug("LeaseHandler - Closing runnable");
            }
        }
    }

    @Slf4j
    static class FSLeaseHandler extends ManagerLeaseHandlerBase {
        private ExecutorService leaseExtender;
        private DataLayerMgrSharedFS manager;

        public FSLeaseHandler(DataLayerMgrSharedFS manager) {
            this.manager = manager;
            leaseExtender = Executors.newSingleThreadExecutor();
            LeaseHandlerFSRunnable leaseRunnable = new LeaseHandlerFSRunnable(manager);
            leaseExtender.submit(leaseRunnable);
        }

        @Override
        public void close() {
            // TODO: Properly handle errors and logging
            if (DebugConstants.DEBUG_FLAG) {
                log.debug(String.format("FSLeaseHandler - Closing"));
            }
            leaseExtender.shutdownNow();
            manager.releaseSoftLease();
        }
    }

    @Slf4j
    static class LeaseHandlerFSRunnable implements Runnable {
        DataLayerMgrSharedFS manager;

        LeaseHandlerFSRunnable(DataLayerMgrSharedFS manager) {
            this.manager = manager;
            if (DebugConstants.DEBUG_FLAG) {
                log.debug("New LeaseHandlerRunnable thread");
            }
        }

        @Override
        public void run() {
            if (DebugConstants.DEBUG_FLAG) {
                log.debug("LeaseHandler - Starting runnable");
            }
            int failureCount = 0;
            int failureMax = 5;
            while (manager.isRunning() && !Thread.currentThread().isInterrupted()) {
                if (DebugConstants.DEBUG_FLAG) {
                    log.debug("LeaseHandler - Starting loop");
                }
                try {
                    if (!manager.tbdFSExtendSoftLease()) {
                        if (DebugConstants.DEBUG_FLAG) {
                            log.warn(String.format("DEBUG - LeaseHandler " +
                                            "- Failed to renew lease [managerId=%d]",
                                    manager.getManagerId()));
                        }
                        failureCount++;
                        if (failureCount >= failureMax) {
                            log.error(String.format("CRITICAL ERROR - LeaseHandler " +
                                                    "- Failed to renew lease - Terminating [managerId=%d]",
                                        manager.getManagerId()));
                            return;
                        }
                    } else {
                        failureCount = 0;
                    }
                    // TODO: Add sleep by evaluating lease length * multiplier and sleeping if value > 10 ms
                    if (manager.tbdFSLeaseThreshold * manager.tbdLeaseMultiplier > 10) {
                        Thread.sleep(((long) (0.5 * manager.tbdFSLeaseThreshold * manager.tbdLeaseMultiplier)));
                    }
                } catch (InterruptedException e) {
                    // NOTE: Lease Handler Closed
                    if (DebugConstants.DEBUG_FLAG) {
                        log.warn("DEBUG - LeaseHandler - Failure in background thread", e);
                    }
                    return;
                } catch (InvalidServiceStateException e) {
                    log.warn("LeaseHandler - Failure in background thread", e);
                } catch (Exception e) {
                    log.warn("LeaseHandler - CRITICAL ERROR - Failure in background thread", e);
                }
            }
        }
    }

    @Slf4j
    static class TbdFSLogReader implements Runnable {
        DataLayerMgrSharedFS manager;
        int heartbeatCounter = 0;
        final int heartbeatThreshold = 20;

        TbdFSLogReader(DataLayerMgrSharedFS manager) {
            this.manager = manager;
            log.info(String.format("New TbdLogReaderFS thread [manager_id=%d]", manager.managerId));
        }

        @Override
        public void run() {
            if (heartbeatCounter >= heartbeatThreshold) {
                log.info(String.format("Heartbeat - LogReader Alive [heartbeat_threshold=%d]",
                        heartbeatThreshold));
                heartbeatCounter = 0;
            }
            heartbeatCounter++;

            // NOTE: Touch heartbeat file
            try {
                CSvcUtils.retryCheckedOperation(() -> {
                            manager.tbdFSUpdateTimestamp(manager.heartbeatFilePath, false);
//                            Files.write(manager.heartbeatFilePath, new byte[1], StandardOpenOption.TRUNCATE_EXISTING);
                        },
                        (Exception e) -> {
                            log.warn(String.format("TbdLogReaderFS " +
                                    "- Failed to update heartbeat file [manager_id=%d]", manager.managerId), e);
                            if (e instanceof FileNotFoundException || e instanceof NoSuchFileException) {
                                log.error(String.format("CRITICAL ERROR - TbdLogReaderFS " +
                                                "- Missing heartbeat file, evaluating manager timeout[manager_id=%d]",
                                        manager.managerId));
                                manager.tbdFSEvaluateTimeoutState();
                            }
                        }, 10);
            } catch (InvalidServiceStateException e) {
                // NOTE: No-Op
            }

            // NOTE: Check if active recovery ongoing
            if (manager.tbdFSCheckActiveRecoveryState()) {
                log.debug(String.format("TbdLogReaderFS - Active recovery detected, reads paused [manager_id=%d]"
                        , manager.managerId));
                return;
            }

            // NOTE: Check & Attempt to update log size metadata
            try {
                boolean logSizeMetadataFlag = CSvcUtils.retryCheckedOperation(() ->
                                manager.retrieveLogFileSizeFromMetadata() != Files.size(manager.logPath),
                        (Exception e) -> {
                            if (!(e instanceof NoSuchFileException)) {
                                log.debug(String.format("WARN - TbdLogReaderFS - Failed to update log size metadata" +
                                        " [manager_id=%d]", manager.managerId), e);
                            }
                            return false;
                        }, 3, false);
                if (logSizeMetadataFlag) {
                    CSvcUtils.retryCheckedOperation(() -> manager.tbdFSWriteTimestampWithContent(manager.logSizePath,
                                ByteBuffer.allocate(Long.BYTES).putLong(Files.size(manager.logPath)).array(), true),
                            (Exception e) -> log.debug(
                                    String.format("WARN - TbdLogReaderFS - Failed to update log size metadata" +
                                    " [manager_id=%d]", manager.managerId), e), 3);
                }
            } catch (InvalidServiceStateException e) {
                // NOTE: No-Op
            }

            try {
                synchronized (manager.tbdLogReadMonitor) {
                    manager.tbdConsumeEntries(true);
                    int currReadSegment = manager.currentReadSegmentIndex.get();
                    ArrayList<FSReadWaitWrapper> removedWaits = new ArrayList<>();
                    for (FSReadWaitWrapper waitingRead : manager.tbdLogReadWaitQueue) {
                        if (DebugConstants.DEBUG_FLAG) {
                            log.debug(String.format("DEBUG - TbdFSLogReader " +
                                "- Queued Wait Element [current_read_segment_index=%d, current_read_offset=%d," +
                                " wait_details=%s]", currReadSegment, manager.currentReadOffset, waitingRead));
                        }
                        if ((manager.currentReadOffset >= waitingRead.getTbdLogSizeThreshold()
                                && currReadSegment == waitingRead.getTbdLogSegmentThreshold())
                                || (currReadSegment > waitingRead.getTbdLogSegmentThreshold())) {
                            synchronized (waitingRead.getMonitorObject()) {
                                waitingRead.getMonitorObject().notifyAll();
                            }
                            removedWaits.add(waitingRead);
                        }
                    }
                    manager.tbdLogReadWaitQueue.removeAll(removedWaits);
                }
            } catch (InvalidServiceStateException e) {
                System.out.println(String.format("LOGREADER - Error "));
                System.out.println(String.format("===\nSTACKTRACE\n==="));
                e.printStackTrace(System.out);
                System.out.println(String.format("===\nLOG READER CONTINUING\n==="));
            } catch (Exception e) {
                System.out.println(String.format("LOGREADER - Critical Error "));
                System.out.println(String.format("===\nSTACKTRACE\n==="));
                e.printStackTrace(System.out);
                System.out.println(String.format("===\nLOG READER CONTINUING\n==="));
            }
        }
    }

    @Slf4j
    static class FSMaintenanceRunnable implements Runnable {
        DataLayerMgrSharedFS manager;

        int heartbeatCounter = 0;
        int gcCounter = 0;
        final int heartbeatThreshold = 10;
        final int gcThreshold = 5;
        FSMaintenanceRunnable(DataLayerMgrSharedFS manager) {
            this.manager = manager;
            System.out.println(String.format("New MaintenanceRunnable thread"));
        }

        private void tbdFSResetMaintenanceTrackers() {
            manager.recoveryAttemptCounter = 0;
            manager.readRecoveryAttemptCounter = 0;
        }

        @Override
        public void run() {
            if (heartbeatCounter >= heartbeatThreshold) {
                log.info(String.format("Heartbeat - FSMaintenanceRunnable Alive [heartbeat_threshold=%d]",
                        heartbeatThreshold));
//                gcCounter++;
//                if (gcCounter >= gcThreshold) {
//                    System.gc();
//                    gcCounter = 0;
//                }
                heartbeatCounter = 0;
            }

            try {
//                // NOTE: Touch heartbeat file
//                CSvcUtils.retryCheckedOperation(() -> {
//                            Files.write(manager.heartbeatFilePath, new byte[1], StandardOpenOption.TRUNCATE_EXISTING);
//                        },
//                        (Exception e) -> {
//                            log.warn(String.format("FSMaintenanceRunnable " +
//                                    "- Failed to update heartbeat file [manager_id=%d]", manager.managerId), e);
//                            if (e instanceof FileNotFoundException) {
//                                log.error(String.format("CRITICAL ERROR - FSMaintenanceRunnable " +
//                                                "- Missing heartbeat file, evaluating manager timeout[manager_id=%d]",
//                                        manager.managerId));
//                                manager.tbdFSEvaluateTimeoutState();
//                            }
//                        }, 10);

//                int retryCount = 0;
//                int retryMax = 10;
//                while (retryCount < retryMax) {
//                    try {
//                        retryCount++;
//                        Files.write(manager.heartbeatFilePath, new byte[1], StandardOpenOption.TRUNCATE_EXISTING);
//                        break;
//                    } catch (FileNotFoundException e) {
//                        if (retryCount >= retryMax) {
//                            System.out.println(String.format("FSMaintenanceRunnable - CRITICAL ERROR " +
//                                    "- Missing heartbeat file, evaluating manager timeout"));
//                            e.printStackTrace(System.out);
//                            manager.tbdFSEvaluateTimeoutState();
//                        }
//                    } catch (IOException e) {
//                        // No-Op
//                        if (retryCount >= retryMax) {
//                            System.out.println(String.format("FSMaintenanceRunnable - Unexpected Exception " +
//                                    "- Failed to update heartbeat file"));
//                            e.printStackTrace(System.out);
//                        }
//                    }
//                }

                // NOTE: Confirm all manager executors running
                if (!manager.validateExecutorState()) {
                    log.error("CRITICAL ERROR - FSMaintenanceRunnable " +
                            "- Manager Executors in Invalid State - TERMINATING");
                    manager.initiateTermination();
                    return;
//                    while (true) {
//                        try {
//                            manager.close();
//                            break;
//                        } catch (InterruptedException ignored) {
//                            // No-Op
//                            Thread.currentThread().interrupt();
//                        }
//                    }
                }

                boolean maintenanceRoleFlag = false;
                try {
                    maintenanceRoleFlag = manager.tbdFSEvaluateMaintenanceRole();
                } catch (ClosedByInterruptException e) {
                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println(String.format("DEBUG - FSMaintenanceRunnable - Thread Interrupted"));
                        e.printStackTrace(System.out);
                    }
                    return;
                } catch (InvalidServiceStateException e) {
                    if (e.getCause() instanceof ClosedByInterruptException) {
                        if (DebugConstants.DEBUG_FLAG) {
                            System.out.println(String.format("DEBUG - FSMaintenanceRunnable - Thread Interrupted"));
                            e.printStackTrace(System.out);
                        }
                        Thread.currentThread().interrupt();
                        return;
                    }
                    System.out.println(String.format("WARN - FSMaintenanceRunnable - Unexpected Exception"));
                    e.printStackTrace(System.out);
                }

                if (maintenanceRoleFlag) {
                    log.info(String.format("FSMaintenanceRunnable - Holding Maintenance Role [manager_id=%d]",
                            manager.managerId));
                    tbdFSExecuteMaintenanceTasks();
                } else {
                    tbdFSResetMaintenanceTrackers();
                }
            } catch (ClosedByInterruptException e) {
                if (DebugConstants.DEBUG_FLAG) {
                    System.out.println(String.format("CRITICAL ERROR - FSMaintenanceRunnable "));
                    System.out.println(String.format("===\nSTACKTRACE\n==="));
                    e.printStackTrace(System.out);
                    System.out.println(String.format("===\nFSMaintenanceRunnable TERMINATING\n==="));
                }
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.out.println(String.format("CRITICAL ERROR - FSMaintenanceRunnable "));
                System.out.println(String.format("===\nSTACKTRACE\n==="));
                e.printStackTrace(System.out);
                System.out.println(String.format("===\nFSMaintenanceRunnable CONTINUING\n==="));
            } finally {
                heartbeatCounter++;
            }
        }

        private void tbdFSExecuteMaintenanceTasks() throws ClosedByInterruptException {
            log.debug(String.format("tbdFSExecuteMaintenanceTasks - Evaluating Log Recovery [manager_id=%d]",
                    manager.managerId));
            if (manager.recoveryRequiredFlag.get() && manager.evaluateRecoveryState()) {
                manager.recoveryAttemptCounter++;
                log.warn(String.format("tbdFSExecuteMaintenanceTasks - Evaluated recovery as necessary [manager_id=%d]",
                        manager.managerId));
                // TODO: redesign recovery lease logic. This cannot be blocking
//                try (ManagerLeaseHandlerBase ignored = manager.claimSoftLeaseHandler().get()) {
                try {
                    manager.tbdFSUpdateTimestamp(manager.recoveryLeasePath, true);
                    if (manager.tbdLogRecovery()) {
                        manager.resetRecoveryMetadata();
                    }
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt(); // Restore interrupt flag
//                    log.error(String.format("tbdFSExecuteMaintenanceTasks " +
//                            "- Thread was interrupted during Log Recovery [manager_id=%d]", manager.managerId), e);
                } catch (Exception e) {
                    log.warn(String.format("tbdFSExecuteMaintenanceTasks - Failed Log Recovery [manager_id=%d]"
                            , manager.managerId), e);
                } finally {
                    try {
                        Files.deleteIfExists(manager.recoveryLeasePath);
                    } catch (IOException ignored) {
                        // NOTE: No-Op
                    }
                }
                if (manager.recoveryAttemptCounter >= RECOVERY_COUNT_THRESHOLD) {
                    log.error(String.format("CRITICAL ERROR - tbdFSExecuteMaintenanceTasks " +
                            "- Failed All Log Recovery Attempts - Terminating [manager_id=%d]", manager.managerId));
                    manager.initiateTermination();
                    return;
//                    while (true) {
//                        try {
//                            manager.close();
//                            break;
//                        } catch (InterruptedException e) {
//                            Thread.currentThread().interrupt();
//                            System.out.println(String.format("CRITICAL ERROR - tbdFSExecuteMaintenanceTasks - Looping"));
//                        }
//                    }
                }
            }

            if (manager.readFailureCounter.get() >= READ_FAILURE_THRESHOLD) {
                log.warn(String.format("tbdFSExecuteMaintenanceTasks - Triggered Read Log Recovery [manager_id=%d]",
                        manager.managerId));
                // NOTE: attempt to copy log to read
                manager.readRecoveryAttemptCounter++;
                try {
                    manager.tbdFSUpdateTimestamp(manager.recoveryLeasePath, true);
                    if (manager.tbdReadLogRecovery()) {
                        manager.readFailureCounter.set(0);
                        manager.readRecoveryAttemptCounter = 0;
                    }
                } catch (InvalidServiceStateException e) {
                    log.warn(String.format("tbdFSExecuteMaintenanceTasks - Failed Read Log Recovery [manager_id=%d]",
                            manager.managerId), e);
                } finally {
                    try {
                        Files.deleteIfExists(manager.recoveryLeasePath);
                    } catch (IOException ignored) {
                        // NOTE: No-Op
                    }
                }
                if (manager.readRecoveryAttemptCounter >= RECOVERY_COUNT_THRESHOLD) {
                    log.error(String.format("CRITICAL ERROR - tbdFSExecuteMaintenanceTasks " +
                            "- Failed All Read Log Recovery Attempts " +
                            "- Terminating [manager_id=%d]", manager.managerId));
                    manager.initiateTermination();
//                    while (true) {
//                        try {
//                            manager.close();
//                            break;
//                        } catch (InterruptedException e) {
//                            Thread.currentThread().interrupt();
//                            System.out.println(String.format("CRITICAL ERROR - tbdFSExecuteMaintenanceTasks - Looping"));
//                        }
//                    }
                }
            }
            try {
                manager.tbdFSManageReadLog();
            } catch (InvalidServiceStateException e) {
                log.warn(String.format("tbdFSExecuteMaintenanceTasks - Error on read log maintenance [manager_id=%d]",
                        manager.managerId), e);
            }
            manager.tbdFSManageEphemerals();
        }
    }
}
