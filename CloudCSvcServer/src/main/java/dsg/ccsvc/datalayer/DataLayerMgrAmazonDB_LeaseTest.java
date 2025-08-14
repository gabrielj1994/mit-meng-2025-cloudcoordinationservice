package dsg.ccsvc.datalayer;

//import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

import dsg.ccsvc.*;
import dsg.ccsvc.caching.CoordinationSvcCacheLayer;
import dsg.ccsvc.command.*;
import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.log.LogRecordMetadata;
import dsg.ccsvc.log.LogRecordStats;
import dsg.ccsvc.util.ConnectionPool;
import dsg.ccsvc.util.DebugConstants;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.SerializationUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static dsg.ccsvc.log.LogRecord.LogRecordType.*;
import static dsg.ccsvc.util.DebugConstants.DBState.LOCK_NOT_AVAILABLE;
import static org.postgresql.util.PSQLState.SERIALIZATION_FAILURE;

public class DataLayerMgrAmazonDB_LeaseTest extends DataLayerMgrBase implements AutoCloseable {

//    DynamoDbClient dbClient;

    //    UUID uuid;
    long managerId;

    String rootPath;
    Path logPath;
    Path leasePath;
    Path idPath;

    String server;
    int port;
    Path metadataPath;

    // TODO: consider other options for executing watches
    ExecutorService watchExecutor;
    // TODO: rename this properly
    ExecutorService backgroundLeaseHandler;
    ScheduledExecutorService backgroundLogReader;
    ScheduledExecutorService backgroundEphemeralHandler;
    AtomicBoolean holdingLeaseFlag = new AtomicBoolean(false);

    final Object monitorObj = new Object();
    // TODO: remove test executor
//    ScheduledExecutorService backgroundLogReaderTest;

    // TODO: Consider if there is a non blocking design
    final Object watchStructMonitor = new Object();
    // TODO: Consider if there is a better design for waiting / notifying
    // TODO: Consider if should be initialized in constructor
    int waitRowThreshold = -1;

//    final Object ephemeralMonitor = new Object();
//    AtomicBoolean ephemeralLeaseAtomic = new AtomicBoolean(false);

    Duration ephemeralLivenessThreshold = Duration.of(10, ChronoUnit.SECONDS);

    boolean isInitialized = false;

    boolean isRunning = false;

    //    String znodeRoot;
//    Map<String, LogRecordMetadata> pathMetadataMap;
//    Map<String, Set<String>> pathChildrenMap;
    // TODO: Implement connection framework
//    Map<Integer, String> sessionsMap;
//    Map<String, Integer> watchManager;

    // TODO: Implement proper UUID generation (or equivalent as needed)
    AtomicInteger logCounter = new AtomicInteger(0);
    Integer sessionCounter = 0;
    String leaderServer;
    String leaderPort;

    Properties properties;

    boolean cacheFlag = false;

    CoordinationSvcCacheLayer<String, LogRecord> recordDataCache;

    ConcurrentHashMap<String, Integer> pathToRowIndex;

    private static final long NONEPHEMERAL_OWNER_VALUE = 0;
    private static final String DELETED_RECORD_PATH = "DELETED_RECORD";

    private static final String CONNECTION_STRING = "jdbc:aws-wrapper:postgresql://csvc-db-2-instance-1.c7oym4pnfncu.us-east-1.rds.amazonaws.com:5432/postgres";
    private static final String USERNAME = "csvcadmin";
    private static final String PASSWORD = "!123Csvc";

    public DataLayerMgrAmazonDB_LeaseTest(Object credentials, String zNodeRoot)
            throws IOException, ClassNotFoundException, InvalidServiceStateException {
        Class.forName("org.postgresql.Driver");
        Class.forName("software.amazon.jdbc.Driver");
        properties = new Properties();

        // Configuring connection properties for the underlying JDBC driver.
        properties.setProperty("user", USERNAME);
        properties.setProperty("password", PASSWORD);
        properties.setProperty("loginTimeout", "100");

        // Configuring connection properties for the JDBC Wrapper.
        properties.setProperty("wrapperPlugins", "failover,efm");
        properties.setProperty("wrapperLogUnclosedConnections", "true");


        // TODO: move out of the constructor (can add cache before initialize)
        this.cSvcNodeRoot = zNodeRoot;
//        initialize(zNodeRoot);
        // TODO: move to initialize
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

    private void clearCacheLayer() {
        if (cacheFlag) {
            cacheFlag = false;
            recordDataCache.clear();
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (isInitialized) {
//        System.out.println("===\nManager close!\n===");
            disableRunningFlag();
            clearCacheLayer();
            backgroundLogReader.shutdown();
            watchExecutor.shutdown();
            backgroundLeaseHandler.shutdown();
            backgroundEphemeralHandler.shutdown();
//        backgroundLogReaderTest.shutdown();
            try {
                if (!backgroundLogReader.awaitTermination(20, TimeUnit.MILLISECONDS)) {
                    backgroundLogReader.shutdownNow();
                }
                if (!watchExecutor.awaitTermination(20, TimeUnit.MILLISECONDS)) {
                    watchExecutor.shutdownNow();
                }
                if (!backgroundLeaseHandler.awaitTermination(20, TimeUnit.MILLISECONDS)) {
                    backgroundLeaseHandler.shutdownNow();
                }
                if (!backgroundEphemeralHandler.awaitTermination(20, TimeUnit.MILLISECONDS)) {
                    backgroundEphemeralHandler.shutdownNow();
                }
            } catch (InterruptedException e) {
                backgroundLogReader.shutdownNow();
                watchExecutor.shutdownNow();
                backgroundLeaseHandler.shutdownNow();
                backgroundEphemeralHandler.shutdownNow();
                throw e;
//            backgroundLogReaderTest.shutdownNow();
            }
        }
    }

    public void disableRunningFlag() {
        this.isRunning = false;
    }

    public boolean isRunning() {
        return this.isRunning;
    }

    //TODO: testing
    private void sandboxTesting() {
        byte[] output = new byte[5];

        try (Connection conn = ConnectionPool.getConnection();
             Statement stmt = conn.createStatement()) {
//        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties);
//             Statement stmt = conn.createStatement()) {
//            ResultSet rs = stmt.executeQuery("SELECT 1")) {
            ResultSet rs = stmt.executeQuery("Select * from record_type");
            System.out.println(String.format("Successful create record_type."));
            System.out.println(String.format("Record Types:"));
            while (rs.next()) {
                System.out.println(String.format("%d \t\t %s", rs.getInt(1), rs.getString(2)));
            }
//            System.out.println(String.format("Successful connection."));
        } catch (SQLException e) {
            System.out.println(String.format("Error sandbox texting connection."));
            throw new RuntimeException(e);
        }
    }

    // TODO: remove this. testing only
    public static void testCleanup() {
        String sqlTx = "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE";
        String sqlLog = "DROP TABLE IF EXISTS log_record_table";
        String sqlRecordType = "DROP TABLE IF EXISTS record_type";
        String sqlRecordStats = "DROP TABLE IF EXISTS record_stats_table";
        String sqlEphemeralRecordTable = "DROP TABLE IF EXISTS ephemeral_record_table";
        String sqlEphemeralOwnerTable = "DROP TABLE IF EXISTS ephemeral_owner_table";
        String sqlWatchTable = "DROP TABLE IF EXISTS watch_table";
        String sqlLeaseTable = "DROP TABLE IF EXISTS lease_table";
        String sqlLeaseWaitlistTable = "DROP TABLE IF EXISTS lease_waitlist_table";
        String sqlEphemeralLeaseTable = "DROP TABLE IF EXISTS ephemeral_lease_table";


        // TODO: Consume log table
        // TODO: only insert if log table empty

        // TODO: refactor to connection pool
        try (Connection conn = ConnectionPool.getConnection()) {
            conn.createStatement().execute(sqlTx);
            conn.createStatement().execute(sqlLog);
            conn.createStatement().execute(sqlRecordType);
            conn.createStatement().execute(sqlRecordStats);
            conn.createStatement().execute(sqlEphemeralRecordTable);
            conn.createStatement().execute(sqlEphemeralOwnerTable);
            conn.createStatement().execute(sqlWatchTable);
            conn.createStatement().execute(sqlLeaseTable);
            conn.createStatement().execute(sqlEphemeralLeaseTable);
            conn.createStatement().execute(sqlLeaseWaitlistTable);
            conn.createStatement().execute("COMMIT");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    void initializeMetadata(String zNodeRoot) {
        super.initializeMetadata(zNodeRoot);
        pathToRowIndex = new ConcurrentHashMap<>();
        pathToRowIndex.put(zNodeRoot, 0);
    }

    @Override
    public void updateMetadataStore(LogRecord logRecord, long timestamp, int transactionId) {
        super.updateMetadataStore(logRecord, timestamp, transactionId);
        pathToRowIndex.put(logRecord.getPath(), transactionId);
    }

    @Override
    public ManagerLeaseHandlerBase startLeaseHandler() {
        return new DBLeaseHandler(this);
    }

    private void initialize(String zNodeRoot) throws IOException, InvalidServiceStateException {
        backgroundLogReader = Executors.newSingleThreadScheduledExecutor();
        watchExecutor = Executors.newSingleThreadExecutor();
        backgroundLeaseHandler = Executors.newSingleThreadExecutor();
        backgroundEphemeralHandler = Executors.newSingleThreadScheduledExecutor();

        initializeMetadata(zNodeRoot);
        initializeLog();
        initializeUniqueId();
        LogReader logReaderRunnable = new LogReader(this);
        EphemeralManager ephemeralManagerRunnable = new EphemeralManager(this);
//        backgroundLogReader.scheduleAtFixedRate(logReaderRunnable, 1500, 500, TimeUnit.MILLISECONDS);
        int periodicitySalt = (int)(Math.random()*17);
        if (DebugConstants.LOGREADER_FLAG) {
            backgroundLogReader.scheduleAtFixedRate(
                    logReaderRunnable, 50, 30+periodicitySalt, TimeUnit.MILLISECONDS);
            backgroundEphemeralHandler.scheduleAtFixedRate(
                    ephemeralManagerRunnable, 50, 127, TimeUnit.MILLISECONDS);
        }
//        backgroundLogReader.scheduleAtFixedRate(
//                                logReaderRunnable, 150, 200+periodicitySalt, TimeUnit.MILLISECONDS);
        if (claimEphemeralLease()) {
            cleanupEphemerals();
        }
        // TODO: properly set up shut down executor service

        isInitialized = true;
    }

    private void cleanupEphemerals() throws InvalidServiceStateException {
        Set<DeleteCommand> pendingDeletes;
        try (Connection conn = ConnectionPool.getConnection()) {
            if (DebugConstants.DEBUG_FLAG) {
                System.out.println(String.format("REMOVE ME - DEBUG - Init ephemeral check"));
            }
            // TODO: Purge expired ephemerals
            HashSet<Long> removedOwners = new HashSet<>();
            // NOTE: Check ephemeral owners
            Instant nowTs = Instant.now();
            Instant touchTs;
            Duration livenessDelta;

            ResultSet rs = conn.prepareStatement(String.format("SELECT * FROM ephemeral_owner_table" +
                            " WHERE touch_timestamp < CURRENT_TIMESTAMP - interval '%d seconds'",
                            ephemeralLivenessThreshold.getSeconds())).executeQuery();
            while (rs.next()) {
                // TODO: properly consume log. handle DELETE later
                touchTs = rs.getTimestamp("touch_timestamp").toInstant();
                long ownerId = rs.getLong("owner_id");
                livenessDelta = Duration.between(touchTs, nowTs);
//                    System.out.println(
//                            String.format("LogReader - Ephemeral Check [ownerId=%d, livenessDelta=%s]",
//                                    ownerId, livenessDelta.toString()));
                // TODO: can add the ephemeral check into the query (and this gets around desync clocks)
                if (livenessDelta.compareTo(ephemeralLivenessThreshold) >= 0) {
                    System.out.println(
                            String.format("Log Init - Ephemeral Liveness Threshold Crossed" +
                                            " [ownerId=%d, livenessDelta=%s]",
                                    ownerId, livenessDelta.toString()));
                    // TODO: delete ephemeral nodes
                    removedOwners.add(ownerId);
                }
            }

            pendingDeletes = generateEphemeralDeletes(conn, removedOwners);
        } catch (SQLException e) {
            // TODO: retry a few times
            System.out.println(String.format("Error initializing log table."));
            // TODO: refactor to how the other helper fns are doing
            throw new InvalidServiceStateException(e);
        }

        for (DeleteCommand cmd : pendingDeletes) {
            // TODO: Consider wrapping with retries? Multiple initializing managers
            //  can clash while deleting the same ephemerals; Can use the lease system;
            try {
                cmd.executeManager(this);
            } catch (InvalidServiceStateException e) {
                // NOTE: No-Op. Continue with other deletes
            }
        }
    }

    public void startManager() throws InvalidServiceStateException, IOException {
//        while (!isInitialized) {
//            Thread.sleep(50);
//        }
//        LogReader logReaderRunnable = new LogReader(this);
////        backgroundLogReader.scheduleAtFixedRate(logReaderRunnable, 1500, 500, TimeUnit.MILLISECONDS);
//        backgroundLogReader.scheduleAtFixedRate(logReaderRunnable, 150, 100, TimeUnit.MILLISECONDS);
        initialize(cSvcNodeRoot);
        isRunning = true;
    }

    private boolean waitForActiveTable(String tableName) throws InterruptedException {
//        TableStatus status = dbClient.describeTable(
//                DescribeTableRequest.builder()
//                        .tableName(tableName).build())
//                .table().tableStatus();
//        while (!(status.equals(TableStatus.ACTIVE) || status.equals(TableStatus.CREATING))) {
//            status = dbClient.describeTable(
//                            DescribeTableRequest.builder()
//                                    .tableName(tableName).build())
//                    .table().tableStatus();
//            Thread.sleep(25);
//        }
//        return status.equals(TableStatus.ACTIVE);
        return false;
    }

    private void initializeLog() throws InvalidServiceStateException {
        // TODO: rename, and improve java-to-DB operation management
        String sqlRecordType = "CREATE TABLE IF NOT EXISTS record_type(" +
                "   ID INT PRIMARY KEY     NOT NULL," +
                "   TYPE           TEXT    NOT NULL)";

        String sqlInsert = "INSERT INTO record_type(id, type) VALUES (0,'NOOP'), (1,'CREATE'), (2,'CONNECT')," +
                " (3,'CREATE_EPHEMERAL'), (4,'DELETE'), (5,'SET')" +
                " ON CONFLICT DO NOTHING;";
//        " ON CONFLICT (id) DO UPDATE SET type = excluded.type;";

        String sqlLog = "CREATE TABLE IF NOT EXISTS log_record_table " +
                "(row INTEGER GENERATED ALWAYS AS IDENTITY," +
                " record_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP," +
                " path TEXT NOT NULL," +
                " record_type INT NOT NULL," +
                " data BYTEA NOT NULL," +
                " manager_id INT8 DEFAULT 0," +
                " PRIMARY KEY (row) INCLUDE (path, manager_id, record_type, record_timestamp)," +
                " FOREIGN KEY (record_type) REFERENCES record_type (id))";

        String sqlRecordStats = "CREATE TABLE IF NOT EXISTS record_stats_table " +
                "(path TEXT NOT NULL," +
                " creation_transaction_id INT NOT NULL," +
                " mutation_transaction_id INT NOT NULL," +
                " child_list_transaction_id INT NOT NULL," +
                " creation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL," +
                " mutation_timestamp TIMESTAMP WITH TIME ZONE NOT NULL," +
                " version INT DEFAULT 0," +
                " child_count INT DEFAULT 0," +
                " child_list_version INT DEFAULT 0," +
                " child_sequential_version INT DEFAULT 0," +
                " data_length INT DEFAULT 0," +
                " ephemeral_owner_id INT8 DEFAULT 0," +
                " PRIMARY KEY (path) INCLUDE (path, mutation_transaction_id," +
                " mutation_timestamp, version, child_count, ephemeral_owner_id))";

        // TODO: consider adding triggers that link these two tables
        String sqlEphemeralRecordTable = "CREATE TABLE IF NOT EXISTS ephemeral_record_table " +
                "(path TEXT PRIMARY KEY NOT NULL," +
                " owner_id INT8 NOT NULL)";

        String sqlERTIndex = "CREATE INDEX IF NOT EXISTS owner_idx ON" +
                " ephemeral_record_table (owner_id) INCLUDE (path);";

        String sqlEphemeralOwnerTable = "CREATE TABLE IF NOT EXISTS ephemeral_owner_table " +
                "(owner_id INT8 PRIMARY KEY NOT NULL," +
                " touch_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP)";

        // TODO: watch table
        // TODO: add permanent watch
        // TODO: Consider if can add to index
        String sqlWatchTable = "CREATE TABLE IF NOT EXISTS watch_table " +
                "(path TEXT NOT NULL," +
                " owner_id INT8 NOT NULL," +
//                " transaction_id INT NOT NULL," +
                " transaction_id INT DEFAULT -1," +
                " watch_type INT NOT NULL," +
                " trigger_transaction_id INT DEFAULT -1," +
                " trigger_transaction_type INT DEFAULT -1," +
                " trigger_record_version INT DEFAULT -1," +
                " triggered_flag BOOLEAN DEFAULT FALSE," +
                " permanent_watch_flag BOOLEAN DEFAULT FALSE," +
                " PRIMARY KEY (path, owner_id, watch_type))";
//                " PRIMARY KEY (path, owner_id, transaction_id))";

        String sqlLeaseTable = "CREATE TABLE IF NOT EXISTS lease_table " +
                "(lock_row char(1) NOT NULL CONSTRAINT DF_LT_Lock DEFAULT 'L'," +
                " owner_id INT8 NOT NULL," +
                " expire_timestamp TIMESTAMP WITH TIME ZONE NOT NULL," +
                " CONSTRAINT PK_LT PRIMARY KEY (lock_row)," +
                " CONSTRAINT CK_LT_Locked CHECK (lock_row='L'))";

        String sqlLeaseWaitlistTable = "CREATE TABLE IF NOT EXISTS lease_waitlist_table " +
                "(ticket INT NOT NULL," +
                " owner_id INT8 NOT NULL," +
                " expire_timestamp TIMESTAMP WITH TIME ZONE NOT NULL," +
                " PRIMARY KEY (ticket) INCLUDE (owner_id, expire_timestamp))";

        String sqlEphemeralLeaseTable = "CREATE TABLE IF NOT EXISTS ephemeral_lease_table " +
                "(lock_row char(1) NOT NULL CONSTRAINT DF_LT_Lock DEFAULT 'L'," +
                " owner_id INT8 NOT NULL," +
                " expire_timestamp TIMESTAMP WITH TIME ZONE NOT NULL," +
                " CONSTRAINT PK_ELT PRIMARY KEY (lock_row)," +
                " CONSTRAINT CK_ELT_Locked CHECK (lock_row='L'))";

//        String sqlWTIndex = "CREATE INDEX IF NOT EXISTS owner_idx ON" +
//                " watch_table (owner_id) INCLUDE (path);";
        // TODO: Consume log table
        // TODO: only insert if log table empty
        // NOTE: Not using try-with-resource to be able to rollback on catch
        Connection conn;
        try {
            conn = ConnectionPool.getConnection();
        } catch (SQLException e) {
            System.out.println(String.format("DEBUG - commitToLog - Expected exception [SQLState=%s]",
                    e.getSQLState()));
            e.printStackTrace(System.out);
            throw new InvalidServiceStateException("CRITICAL ERROR - Log Init - Failed to retrieve Connection");
        }

        // TODO: refactor to connection pool
        try {
//        try (Connection conn = ConnectionPool.getConnection();) {
            Statement stmt = conn.createStatement();
            Statement tx_stmt = conn.createStatement();
            tx_stmt.execute("ROLLBACK");
            tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            stmt.addBatch(sqlRecordType);
            stmt.addBatch(sqlInsert);
            stmt.addBatch(sqlLog);
            stmt.addBatch(sqlRecordStats);
            stmt.addBatch(sqlEphemeralRecordTable);
            stmt.addBatch(sqlERTIndex);
            stmt.addBatch(sqlEphemeralOwnerTable);
            stmt.addBatch(sqlWatchTable);
            stmt.addBatch(sqlLeaseTable);
            stmt.addBatch(sqlEphemeralLeaseTable);
            stmt.addBatch(sqlLeaseWaitlistTable);
            stmt.addBatch("COMMIT");
//            stmt.addBatch(sqlWTIndex);
            // TODO: set up a dummy debug print
            boolean initTables = false;
            int retryCount = 0;
            int retryMax = 5;
            while (!initTables && retryCount < retryMax) {
                try {
                    stmt.executeBatch();
                    initTables = true;
                } catch (SQLException e) {
                    retryCount++;
                    System.out.println(String.format("CRITICAL ERROR - Log Init - Table init error."));
                    System.out.println(String.format("===\n"+e+"\n==="));
                    System.out.println(String.format("Retrying table inits"));
                    e.printStackTrace(System.out);
                }
            }

            if (retryCount == retryMax) {
//                throw new RuntimeException("CRITICAL ERROR - Log Init - Table init error");
                throw new InvalidServiceStateException("CRITICAL ERROR - Log Init - Table init error");
            }
//
//            // TODO: consider multiple parallel initializations. Probably have to transaction lock the init as well
//            // TODO: consume log
//            ResultSet rs = conn.prepareStatement("SELECT * FROM log_record_table ORDER BY row asc").executeQuery();
//            // TODO: Decouple counter and highest row read
//            int counter = 0;
//            while (rs.next()) {
//                // TODO: properly consume log. handle DELETE later
//                counter += 1;
//                Path path = Paths.get(rs.getString("path"));
//                LogRecord.LogRecordType type = LogRecord.LogRecordType.fromInteger(
//                        rs.getInt("record_type"));
//                long managerId = rs.getLong("manager_id");
//                long timestamp = rs.getTimestamp("record_timestamp").getTime();
//                int txId = rs.getInt("row");
//                logCounter.set(txId);
////                byte[] data = rs.getBytes("data");
//                if (DebugConstants.DEBUG_FLAG) {
//                    System.out.println(String.format("REMOVE ME - DEBUG - Init consume [path=%s, type=%s]",
//                            path.toString(), type.toString()));
//                }
//
//                updateMetadataStore(new LogRecord(path.toString(), new byte[0],
//                        type, managerId), timestamp, txId);
//            }

//            if (counter == 0) {
//                tx_stmt.execute("ROLLBACK");
//                tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
//                // NOTE: Insert root node
//                PreparedStatement execStmt = conn.prepareStatement(String.format("INSERT INTO log_record_table " +
//                        "(path, record_type, data) VALUES ('%s', 0, ''::bytea)", this.cSvcNodeRoot));
//                if (execStmt.executeUpdate() < 1) {
//                    // NOTE: Failed update
//                    System.out.println(String.format("DEBUG - Initialization failed [path=%s]",
//                            this.cSvcNodeRoot));
//                    tx_stmt.execute("ROLLBACK");
//                    throw new InvalidServiceStateException("CRITICAL ERROR - Log Init - Failed to insert root node");
//                }
//
//                execStmt = conn.prepareStatement("INSERT INTO record_stats_table " +
//                        "(path, creation_transaction_id, mutation_transaction_id, child_list_transaction_id," +
//                        " creation_timestamp, mutation_timestamp, version, child_list_version, child_sequential_version)" +
//                        " VALUES (?, 1, 1, 1, '1970-01-01 00:00:00.000000+00'," +
//                        " '1970-01-01 00:00:00.000000+00', 0, -1, -1)");
//                execStmt.setString(1, this.cSvcNodeRoot);
////                execStmt.executeUpdate();
//                if (execStmt.executeUpdate() < 1) {
//                    // NOTE: Failed update
//                    System.out.println(String.format("DEBUG - Initialization failed [path=%s]",
//                            this.cSvcNodeRoot));
//                    tx_stmt.execute("ROLLBACK");
////                    throw new RuntimeException();
//                    throw new InvalidServiceStateException(
//                            "CRITICAL ERROR - Log Init - Failed to insert root node stat");
//                }
//                tx_stmt.execute("COMMIT");
//            }
        } catch (SQLException e) {
            // TODO: retry a few times
            System.out.println(String.format("Error initializing log table."));
            e.printStackTrace(System.out);

            try {
                conn.rollback();
            } catch (SQLException ex) {
                System.out.println(String.format("CRITICAL ERROR - Failed rollback after ERROR"));
                e.printStackTrace(System.out);
            }
            throw new InvalidServiceStateException(e);

        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                System.out.println(String.format("CRITICAL ERROR - Failed closing SQL connection."));
                e.printStackTrace(System.out);
                throw new InvalidServiceStateException(e);
            }
        }
    }

    private void tbdVerifyEphemeralOwners(Connection conn, Set<Long> remainingOwners,
                                          HashMap<Long, HashSet<String>> ownersToPaths) throws SQLException {
        // TODO: select from ephemeral owners table
        ResultSet rsEphemOwners = conn.createStatement().executeQuery("SELECT owner_id FROM ephemeral_owner_table");
        Long trackedOwner;
        while (rsEphemOwners.next()) {
            trackedOwner = rsEphemOwners.getLong("owner_id");
            remainingOwners.remove(trackedOwner);
        }
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

    public long getLogValidationToken(Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("SELECT MAX(row) FROM log_record_table");
        ResultSet rs = stmt.executeQuery();
        long counter = 0;
        if (rs.next()) {
            counter = rs.getInt(1);
//                System.out.println(String.format("DEBUG - getlogtimestamp success [logCount=%d].", counter));
        } else {
            // TODO: remove DEBUG
            System.out.println(String.format("DEBUG - getLogValidationToken failure."));
        }
        return counter;
    }

    private boolean validateRecordVersion(String path, int version, Connection conn) throws SQLException {
        boolean isValid = false;
        String statsStr = String.format("SELECT path FROM record_stats_table WHERE path='%s' AND version=%d",
                                        path, version);
        ResultSet rs = conn.createStatement().executeQuery(statsStr);
        if (rs.next()) {
            System.out.println(String.format("REMOVE ME - validateRecordVersion [path=%s, version=%d]",
                    path, version));
            // TODO: Consider if any operations need to take place here
            isValid = true;
        }
        return isValid;
    }

    public boolean commitToLog(long logValidationValue, LogRecord logRecord,
                               int expectedVersion, boolean validateVersionFlag, boolean sequentialFlag,
                               Connection conn, boolean dryRun) throws SQLException {
        if (DebugConstants.DEBUG_FLAG) {
            System.out.println(String.format("REMOVE ME - commitToLog [path=%s]",
                    logRecord.getPath()));
        }
        // TODO: remove dryflag
        // TODO: TODO PRIORITY this needs to be tied together with the lease mechanism
        //  and proper API for handling validation/lease claim failures
//        boolean isValid = this.validateLog(logValidationValue);
        // TODO: REMOVEME REMOVE ME ; remove watch
//        if (DebugConstants.METRICS_FLAG) {
//            StopWatch watch = new StopWatch();
//        }

        boolean isValid = true;
        // TODO: refactor watch trigger tracking to be cleaner
        // bitmask style. 0: DATA_TRIGGER 1: CHILDREN_TRIGGER 2: EXIST_TRIGGER
        boolean []watchTriggerValues = {false,false,false};
        // TODO: Consider different approaches. -1 is invalid magic number
        int txId;
        int recordVersion = -1;
//        boolean validationFlag = true;
//        if (!dryFlag) {
//            isValid = this.validateLog(logInstant);
//        }
        String path = logRecord.getPath();
        if (isValid) {
//            try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
//            try (Connection conn = ConnectionPool.getConnection()) {
            Statement tx_stmt = conn.createStatement();

//            if (DebugConstants.METRICS_FLAG) {
//                watch.start();
//            }
            // TODO: Validate (that parent path exists ; not ending in /) after tx begin
            tx_stmt.execute("ROLLBACK");
            tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            try {
                // TODO: Consider how sequential is 'create' only
                if (sequentialFlag) {
                    // NOTE: Check parent stats table for children version
                    // TODO: Consider if inmemory stats store can be used. Any edge cases? Likely requires sync
                    LogRecordStats stats = retrieveStatsHelper(conn,
                            Paths.get(path).getParent().toString());
                    // NOTE: Update path
                    // TODO: Consider if ZK implementation is required (it's simple and easy)
                    path = path + String.format(Locale.ENGLISH, "%010d", stats.getChildSequentialVersion());
                    // TODO: Test if not changing the path fixes double sequential bug
//                    logRecord.setPath(path);
                }

//                if (DebugConstants.METRICS_FLAG) {
//                    watch.stop();
//                    System.out.println(String.format("REMOVE ME - DEBUG - commitToLog - Start Txn [execution_time_ms=%s]",
//                            watch.getTime(TimeUnit.MILLISECONDS)));
//                    watch.reset();
//                    watch.start();
//                }

                // TODO: Validations; Need 1) for path & 2) for parent path
                // TODO: Add version validation
                // NOTE: path validation
                boolean commitValid;
                switch (logRecord.getType()) {
                    case CREATE_EPHEMERAL:
                    case CREATE:
                        commitValid = tbdValidateCreateCommit(conn, path);
                        break;
                    case DELETE:
                        commitValid = tbdValidateDeleteCommit(conn, path, validateVersionFlag, expectedVersion);
                        break;
                    case SET:
                        commitValid = tbdValidateSetCommit(conn, path, validateVersionFlag, expectedVersion);
                        break;
                    default:
                        // TODO: Consider Error Out
                        commitValid = false;
                }

                if (!commitValid) {
                    tx_stmt.execute("ROLLBACK");
                    return false;
                }
                // ===
                // ===
//                if (DebugConstants.METRICS_FLAG) {
//                    watch.stop();
//                    System.out.println(String.format("REMOVE ME - DEBUG - commitToLog - Validation steps" +
//                            " [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
//
//                    StopWatch midWatch = new StopWatch();
//                    midWatch.start();
//
//                    watch.reset();
//                    watch.start();
//                }
                // ===
                PreparedStatement stmt = conn.prepareStatement("INSERT INTO log_record_table " +
                        "(path, record_type, data, manager_id) VALUES (?, ?, ?, ?)" +
                        " RETURNING row, record_timestamp");
//                int counter = 0;
//            stmt.setInt(1, 0);
//                stmt.setString(1, logRecord.getPath());
                stmt.setString(1, path);
                stmt.setInt(2, logRecord.getType().ordinal());
                stmt.setBytes(3, logRecord.getData());
                // TODO: Consider that the manager that commits knows its own ID
                stmt.setLong(4, logRecord.getManagerId());
//        int resultCount = stmt.execute();
//                int resultCount;
                if (!stmt.execute()) {
                    // NOTE: Consume result count
                    stmt.getUpdateCount();
                    if (!stmt.getMoreResults()) {
                        // NOTE: Failed update
                        System.out.println(String.format("DEBUG - SQL Insert Failure " +
                                        "- Invalid State [expected=%d, logCount=%d]",
                                logValidationValue, this.getLogValidationToken(conn)));
                        tx_stmt.execute("ROLLBACK");
                        return false;
                    }
                }
//                if (DebugConstants.METRICS_FLAG) {
//                    midWatch.stop();
//                    System.out.println(String.format("\t\tREMOVE ME - DEBUG - commitToLog -" +
//                            " MIDWATCH - log_record_table step" +
//                            " [execution_time_ms=%s]", midWatch.getTime(TimeUnit.MILLISECONDS)));
//                    midWatch.reset();
//                    midWatch.start();
//                }

                ResultSet resultSet = stmt.getResultSet();
                if (resultSet.next()) {
                    txId = resultSet.getInt("row");
                    Timestamp timestamp = resultSet.getTimestamp("record_timestamp");
//            long timestampMSDebug = resultSet.getTimestamp("record_timestamp").toInstant().toEpochMilli();
//            System.out.println(String.format("REMOVE ME! DEBUG timestamps [toInstant.epochMilli=%d, getTime=%d]",
//                    timestampMS, timestampMSDebug));
                    // TODO: Consider if this can be tackled with a trigger
                    // TODO: fix the conditional block logic structuring to reduce duplication
                    if (logRecord.getType() == CREATE
                            || logRecord.getType() == LogRecord.LogRecordType.SET
                            || logRecord.getType() == CREATE_EPHEMERAL) {
                        // TODO: Add validations against children of ephemeral node (and other creation validations)
                        if (logRecord.getType() == CREATE
                                || logRecord.getType() == CREATE_EPHEMERAL) {

                            watchTriggerValues[1] = true;
                            watchTriggerValues[2] = true;
                            recordVersion = 0;
                            // NOTE: Insert new path
                            stmt = conn.prepareStatement("INSERT INTO record_stats_table" +
                                    " (path, creation_transaction_id, mutation_transaction_id," +
                                    " child_list_transaction_id, creation_timestamp, mutation_timestamp," +
                                    " ephemeral_owner_id) VALUES (?, ?, ?, ?, ?, ?, ?)");
//                            stmt.setString(1, logRecord.getPath());
                            stmt.setString(1, path);
                            stmt.setInt(2, txId);
                            stmt.setInt(3, txId);
                            stmt.setInt(4, txId);
                            stmt.setTimestamp(5, timestamp);
                            stmt.setTimestamp(6, timestamp);
//                            stmt.setInt(7, 0);
//                            stmt.setInt(8, 0);
//                            stmt.setInt(9, 0);
                            if (logRecord.getType() == CREATE) {
                                stmt.setLong(7, NONEPHEMERAL_OWNER_VALUE);
                            } else {
                                // NOTE: Ephemeral Create

//                                System.out.println(String.format("REMOVE ME - Create Ephemeral Stats [path=%s, manager_id=%d]",
//                                        logRecord.getPath(), logRecord.getManagerId()));
                                if (DebugConstants.DEBUG_FLAG) {
                                    System.out.println(String.format("REMOVE ME " +
                                                    "- Create Ephemeral Stats [path=%s, manager_id=%d]",
                                            path, logRecord.getManagerId()));
                                }

                                stmt.setLong(7, logRecord.getManagerId());

                                // NOTE: update ephemeral tables
                                // TODO: consider different naming conventions
                                PreparedStatement stmtEph = conn.prepareStatement(
                                        "INSERT INTO ephemeral_record_table " +
                                                "(path, owner_id)" +
                                                " VALUES (?, ?)");
//                                stmtEph.setString(1, logRecord.getPath());
                                stmtEph.setString(1, path);
                                stmtEph.setLong(2, logRecord.getManagerId());
                                if (stmtEph.executeUpdate() < 1) {
                                    // NOTE: Failed update
                                    System.out.println(String.format(
                                            "ERROR - ephemeral_record_table update failed [path=%s]",
                                            path));
//                                    logRecord.getPath()));
                                    tx_stmt.execute("ROLLBACK");
                                    return false;
                                }

                                if (DebugConstants.DEBUG_FLAG) {
                                    System.out.println(String.format("REMOVE ME " +
                                                    "- Insert Ephemeral Owner [path=%s, manager_id=%d]",
                                            path, logRecord.getManagerId()));
                                }

                                // NOTE: Insert or update 'ephemeral_owner_table'
                                // TODO: Clean this up as SQL insert with on conflict do nothing
                                stmtEph = conn.prepareStatement("INSERT INTO ephemeral_owner_table " +
                                        "(owner_id) VALUES (?) ON CONFLICT DO NOTHING");
                                stmtEph.setLong(1, logRecord.getManagerId());
                                // TODO: consider better naming conventions
//                                ResultSet rsEphemeralQuery = stmtEph.executeQuery();
                                stmtEph.executeUpdate();
                            }

                            if (stmt.executeUpdate() < 1) {
                                // NOTE: Failed update
                                System.out.println(String.format("ERROR - Stats update failed [path=%s]",
                                        path));
//                                logRecord.getPath()));
                                tx_stmt.execute("ROLLBACK");
                                return false;
                            }

//                            if (DebugConstants.METRICS_FLAG) {
//                                midWatch.stop();
//                                System.out.println(String.format("\t\tREMOVE ME - DEBUG - commitToLog -" +
//                                        " MIDWATCH - record_stats_table create step" +
//                                        " [execution_time_ms=%s]", midWatch.getTime(TimeUnit.MILLISECONDS)));
//                                midWatch.reset();
//                                midWatch.start();
//                            }

                            if (DebugConstants.DEBUG_FLAG) {
                                System.out.println(String.format("REMOVE ME " +
                                                "- Update Record Stats [path=%s, manager_id=%d]",
                                        path, logRecord.getManagerId()));
                            }

                            // NOTE: Update parent path
                            stmt = conn.prepareStatement("UPDATE record_stats_table SET" +
                                    " child_list_transaction_id = ?," +
                                    " child_count = child_count + 1," +
                                    " child_list_version = child_list_version + 1," +
                                    " child_sequential_version = child_sequential_version + 1" +
                                    " WHERE path = ?");
//                            stmt = conn.prepareStatement("UPDATE record_stats_table SET" +
//                                    " child_list_transaction_id = ?," +
//                                    " child_sequential_version = child_sequential_version + 1" +
//                                    " WHERE path = ?");
                            stmt.setInt(1, txId);
                            stmt.setString(2, Paths.get(path).getParent().toString());
//                            stmt.setString(2, Paths.get(logRecord.getPath()).getParent().toString());
                            if (stmt.executeUpdate() < 1) {
                                // NOTE: Failed update
                                System.out.println(String.format("ERROR - Stats parent path update failed [path=%s]",
                                        path));
//                                logRecord.getPath()));
                                tx_stmt.execute("ROLLBACK");
                                return false;
                            }
//                            if (DebugConstants.METRICS_FLAG) {
//                                midWatch.stop();
//                                System.out.println(String.format("\t\tREMOVE ME - DEBUG - commitToLog -" +
//                                        " MIDWATCH - record_stats_table parent step" +
//                                        " [execution_time_ms=%s]", midWatch.getTime(TimeUnit.MILLISECONDS)));
//                                midWatch.reset();
//                                midWatch.start();
//                            }
                        } else {
                            // NOTE: SET Command

                            watchTriggerValues[0] = true;

                            stmt = conn.prepareStatement("UPDATE record_stats_table SET" +
                                    " mutation_transaction_id = ?," +
                                    " mutation_timestamp = ?," +
                                    " version = version + 1" +
                                    " WHERE path = ?" +
                                    " RETURNING version");
                            stmt.setInt(1, txId);
                            stmt.setTimestamp(2, timestamp);
                            stmt.setString(3, path);
//                            stmt.setString(3, logRecord.getPath());

//                            if (stmt.executeUpdate() < 1) {
//                                // NOTE: Failed update
//                                System.out.println(String.format("ERROR - Stats update failed [path=%s]",
//                                        logRecord.getPath()));
//                                tx_stmt.execute("ROLLBACK");
//                                return false;
//                            }

                            if (!stmt.execute()) {
                                // NOTE: Consume result count
//                                resultCount = stmt.getUpdateCount();
                                stmt.getUpdateCount();
                                if (!stmt.getMoreResults()) {
                                    // NOTE: Failed update
                                    System.out.println(String.format("ERROR - Stats update failed [path=%s]",
                                            path));
//                                    logRecord.getPath()));
                                    tx_stmt.execute("ROLLBACK");
                                    return false;
                                }
                            }

                            ResultSet rsStats = stmt.getResultSet();
                            if (rsStats.next()) {
                                recordVersion = rsStats.getInt("version");
                            } else {
                                System.out.println("An error occurred.");
                                System.out.println(String.format("CommitToLog Failed -" +
                                        " Failed to retrieve record version [path=%s]", path));
//                                " Failed to retrieve record version [path=%s]", logRecord.getPath()));
                                tx_stmt.execute("ROLLBACK");
                                return false;
                            }

//                            if (DebugConstants.METRICS_FLAG) {
//                                midWatch.stop();
//                                System.out.println(String.format("\t\tREMOVE ME - DEBUG - commitToLog -" +
//                                        " MIDWATCH - record_stats_table set step" +
//                                        " [execution_time_ms=%s]", midWatch.getTime(TimeUnit.MILLISECONDS)));
//                                midWatch.reset();
//                                midWatch.start();
//                            }
                        }
                    } else if (logRecord.getType() == LogRecord.LogRecordType.DELETE) {
                        watchTriggerValues[0] = true;
                        watchTriggerValues[1] = true;
                        watchTriggerValues[2] = true;
                        stmt = conn.prepareStatement("DELETE FROM record_stats_table" +
                                " WHERE path = ? AND child_count = 0");
//                        stmt = conn.prepareStatement("DELETE FROM record_stats_table" +
//                                " WHERE path = ?");

                        stmt.setString(1, path);

                        if (stmt.executeUpdate() < 1) {
                            // NOTE: Failed update
                            System.out.println(String.format("ERROR - DELETE -" +
                                            " Path missing or contains children [path=%s]",
                                            path));
                            tx_stmt.execute("ROLLBACK");
                            return false;
                        }
//
//                        if (DebugConstants.METRICS_FLAG) {
//                            midWatch.stop();
//                            System.out.println(String.format("\t\tREMOVE ME - DEBUG - commitToLog -" +
//                                    " MIDWATCH - record_stats_table delete step" +
//                                    " [execution_time_ms=%s]", midWatch.getTime(TimeUnit.MILLISECONDS)));
//                            midWatch.reset();
//                            midWatch.start();
//                        }

                        if (DebugConstants.DEBUG_FLAG) {
                            System.out.println(String.format(
                                    "REMOVE ME - DELETE - Confirmed record_stats_table delete [path=%s]",
                                    path));
                        }

                        if (DebugConstants.DEBUG_FLAG) {
                            System.out.println(String.format(
                                    "REMOVE ME - DELETE - Confirmed node in log [path=%s]", path));
                        }

                        // NOTE: Update parent path
                        stmt = conn.prepareStatement("UPDATE record_stats_table SET" +
                                " child_list_transaction_id = ?," +
                                " child_count = child_count - 1," +
                                " child_list_version = child_list_version + 1" +
                                " WHERE path = ?");
//                        stmt = conn.prepareStatement("UPDATE record_stats_table SET" +
//                                " child_list_transaction_id = ?" +
//                                " WHERE path = ?");
                        stmt.setInt(1, txId);
                        stmt.setString(2, Paths.get(path).getParent().toString());
                        if (stmt.executeUpdate() < 1) {
                            // NOTE: Failed update
                            System.out.println(String.format("ERROR - Stats parent path update failed [path=%s]",
                                    path));
                            tx_stmt.execute("ROLLBACK");
                            return false;
                        }

//                        if (DebugConstants.METRICS_FLAG) {
//                            midWatch.stop();
//                            System.out.println(String.format("\t\tREMOVE ME - DEBUG - commitToLog -" +
//                                    " MIDWATCH - record_stats_table parent step" +
//                                    " [execution_time_ms=%s]", midWatch.getTime(TimeUnit.MILLISECONDS)));
//                            midWatch.reset();
//                            midWatch.start();
//                        }

                        // NOTE: Update ephemeral nodes
                        stmt = conn.prepareStatement(
                                "DELETE FROM ephemeral_record_table" +
                                " WHERE path = ? RETURNING *");


                        stmt.setString(1, path);
                        boolean ephemeralDeleteFlag = stmt.execute();
                        if (!ephemeralDeleteFlag) {
                            // NOTE: Consume result count
                            int deleteCount = stmt.getUpdateCount();
//                    System.out.println(String.format(
//                            "REMOVE ME - DEBUG - LogReader - WATCH SQL [updateReturnFlag=%b, count=%d]",
//                            updateReturnFlag, resultCount));
                            if (deleteCount > 0 && !stmt.getMoreResults()) {
                                // NOTE: Failed update
                                if (DebugConstants.DEBUG_FLAG) {
                                    System.out.println(String.format(
                                            "REMOVE ME - DEBUG - commitToLog " +
                                                    "- Deleting Ephemeral Fail [managerId=%d, path=%s]",
                                            managerId, path));
                                }
                                tx_stmt.execute("ROLLBACK");
                                return false;
                            }
                        }

                        ResultSet rsEphemDeletes = stmt.getResultSet();
                        if (!Objects.isNull(rsEphemDeletes) && rsEphemDeletes.next()) {
                            System.out.println(String.format(
                                    "REMOVE ME - DEBUG - commitToLog " +
                                    "- Deleted Ephemeral Node [path=%s]", path));
                            // TODO: get owner_id from returning
                            //  count(*) on owner_id; if 0 delete from ephemeral_owner_table
                            // TODO: Consider just tracking count like child count
                            long ownerId = rsEphemDeletes.getLong("owner_id");
                            ResultSet rsEphemCount = conn.createStatement().executeQuery(
                                    String.format(
                                            "SELECT COUNT(*) FROM ephemeral_record_table" +
                                            " WHERE owner_id = %d", ownerId));
                            if (!rsEphemCount.next()) {
                                // TODO: throw exception because count failed;
                                System.out.println(String.format(
                                        "REMOVE ME - CRITICAL FAILURE - commitToLog " +
                                                "- SELECT COUNT FAILED [managerId=%d, path=%s]",
                                        managerId, path));
                            }
                            int ownerCount = rsEphemCount.getInt(1);
                            if (ownerCount == 0) {
                                // TODO: Delete from ephemeral owner table
                                System.out.println(String.format(
                                        "REMOVE ME - DEBUG - commitToLog " +
                                        "- Deleting Ephemeral Owner [ownerId=%d]", ownerId));
                                conn.createStatement().executeUpdate(String.format(
                                        "DELETE FROM ephemeral_owner_table" +
                                        " WHERE owner_id = %d", ownerId));
                            }
                        }

//                        if (DebugConstants.METRICS_FLAG) {
//                            midWatch.stop();
//                            System.out.println(String.format("\t\tREMOVE ME - DEBUG - commitToLog -" +
//                                    " MIDWATCH - ephemeral step" +
//                                    " [execution_time_ms=%s]", midWatch.getTime(TimeUnit.MILLISECONDS)));
//                            midWatch.reset();
//                            midWatch.start();
//                        }
                    }
//                    tx_stmt.execute("COMMIT");
                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println(String.format("REMOVE ME - commitToLog COMMIT [path=%s]",
                                path));
                    }

                    // TODO: Consider where logreader wait should be placed
//                    synchronized(monitorObj) {
//                        while (logCounter < txId) {
//                            writeRowThreshold = txId;
//                            monitorObj.wait();
//                        }
//                    }

//                    tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                    // TODO: watch table check
                    // bitmask style. 0: DATA_TRIGGER 1: CHILDREN_TRIGGER 2: EXIST_TRIGGER
                    /*
                    public enum WatchType {
                    DATA,
                    CHILDREN,
                    EXIST;
                     */
                    if (watchTriggerValues[0]) {
                        if (DebugConstants.DEBUG_FLAG) {
                            System.out.println(String.format("REMOVE ME - commitToLog - DATA watch [path=%s]",
                                    path));
                        }
                        // NOTE: DATA watch trigger
//                        conn.createStatement().executeUpdate(String.format(
//                                "UPDATE watch_table SET triggered_flag = TRUE, trigger_transaction_id = %d," +
//                                        " trigger_record_version = %d, trigger_transaction_type = %d" +
//                                        " WHERE path = '%s' AND transaction_id < %d AND watch_type = %d",
//                                txId, recordVersion, logRecord.getType().ordinal(), logRecord.getPath(),
//                                txId, WatchedEvent.WatchType.DATA.ordinal()));
                        conn.createStatement().executeUpdate(String.format(
                                "UPDATE watch_table SET triggered_flag = TRUE, trigger_transaction_id = %d," +
                                        " trigger_record_version = %d, trigger_transaction_type = %d" +
                                        " WHERE path = '%s' AND watch_type = %d",
                                txId, recordVersion, logRecord.getType().ordinal(), path,
                                WatchedEvent.WatchType.DATA.ordinal()));
                    }

                    if (watchTriggerValues[1]) {
                        if (DebugConstants.DEBUG_FLAG) {
                            System.out.println(String.format("REMOVE ME - commitToLog - CHILDREN watch [path=%s]",
                                    path));
                        }
                        // NOTE: CHILDREN watch trigger
//                        conn.createStatement().executeUpdate(String.format(
//                                "UPDATE watch_table SET triggered_flag = TRUE, trigger_transaction_id = %d," +
//                                        " trigger_record_version = %d, trigger_transaction_type = %d" +
//                                        " WHERE path = '%s' AND transaction_id < %d AND watch_type = %d",
//                                txId, recordVersion, logRecord.getType().ordinal(), Paths.get(logRecord.getPath()).getParent().toString(), txId,
//                                WatchedEvent.WatchType.CHILDREN.ordinal()));
                        // TODO: Remove debug
                        int debugIntValue = conn.createStatement().executeUpdate(String.format(
                                "UPDATE watch_table SET triggered_flag = TRUE, trigger_transaction_id = %d," +
                                        " trigger_record_version = %d, trigger_transaction_type = %d" +
                                        " WHERE path = '%s' AND watch_type = %d",
                                txId, recordVersion, logRecord.getType().ordinal(),
                                Paths.get(path).getParent().toString(),
                                WatchedEvent.WatchType.CHILDREN.ordinal()));

                        if (DebugConstants.DEBUG_FLAG) {
                            System.out.println(String.format(
                                    "REMOVE ME - commitToLog - CHILDREN watch [path=%s, debugIntValue=%d]",
                                    path, debugIntValue));
                        }
                    }

                    if (watchTriggerValues[2]) {
                        if (DebugConstants.DEBUG_FLAG) {
                            System.out.println(String.format("REMOVE ME - commitToLog - EXIST watch [path=%s]",
                                    path));
                        }
                        // NOTE: EXIST watch trigger
//                        conn.createStatement().executeUpdate(String.format(
//                                "UPDATE watch_table SET triggered_flag = TRUE, trigger_transaction_id = %d," +
//                                        " trigger_record_version = %d, trigger_transaction_type = %d" +
//                                        " WHERE path = '%s' AND transaction_id < %d AND watch_type = %d",
//                                txId, recordVersion, logRecord.getType().ordinal(), logRecord.getPath(), txId,
//                                WatchedEvent.WatchType.EXIST.ordinal()));
                        conn.createStatement().executeUpdate(String.format(
                                "UPDATE watch_table SET triggered_flag = TRUE, trigger_transaction_id = %d," +
                                        " trigger_record_version = %d, trigger_transaction_type = %d" +
                                        " WHERE path = '%s' AND watch_type = %d",
                                txId, recordVersion, logRecord.getType().ordinal(), path,
                                WatchedEvent.WatchType.EXIST.ordinal()));
                    }

//                    if (DebugConstants.METRICS_FLAG) {
//                        midWatch.stop();
//                        System.out.println(String.format("\t\tREMOVE ME - DEBUG - commitToLog -" +
//                                " MIDWATCH - watch triggers step" +
//                                " [execution_time_ms=%s]", midWatch.getTime(TimeUnit.MILLISECONDS)));
//                        midWatch.reset();
//                        midWatch.start();
//                    }

                    // NOTE: Update path in case of sequential node
                    synchronized (outstandingChangesMonitor) {
                        conn.commit();
                        logRecord.setPath(path);
                        addToOutstandingRecords(txId, path, logRecord);
                    }

//                    if (DebugConstants.METRICS_FLAG) {
//                        watch.stop();
//                        System.out.println(String.format("REMOVE ME - DEBUG - commitToLog - Waiting on logreader" +
//                                " [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
//                    }
                    return true;
                } else {
                    System.out.println("An error occurred.");
                    System.out.println(String.format("CommitToLog Failed - Failed to retrieve inserted row " +
                            "and timestamp [path=%s]", path));
                    tx_stmt.execute("ROLLBACK");
                    return false;
                }
//            } catch (SQLException e) {
//                System.out.println(String.format("Error - commitToLog - Failed to commit."));
//                throw new RuntimeException(e);
//            }
            } catch (SQLException e) {
                tx_stmt.execute("ROLLBACK");
                if (e.getSQLState().equalsIgnoreCase(SERIALIZATION_FAILURE.getState())) {
                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println(String.format("DEBUG - commitToLog - Expected exception [SQLState=%s]",
                                e.getSQLState()));
                    }
                } else {
                    System.out.println(String.format("ERROR - commitToLog failed [path=%s]",
                            path));
                    // TODO: consider refactoring to better design
                    System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                            e.getSQLState()));
                    e.printStackTrace(System.out);
                }
//                throw e;
                return false;
            } catch (InterruptedException e) {
                System.out.println(String.format("FATAL ERROR - interrupted while waiting on logreader [path=%s]",
                        path));
                tx_stmt.execute("ROLLBACK");
                throw new RuntimeException(e);
            }
        } else {
            // TODO: implement retry and target_path version validation
            System.out.println(String.format("DEBUG - commitToLog Basic - Validation failed [expected=%d, logCount=%d]",
                    logValidationValue, this.getLogValidationToken(conn)));
            return false;
        }
    }

    private boolean tbdValidateCreateCommit(Connection conn, String path) throws SQLException {
        // NOTE: Still valid if first query fails
        boolean isValid = true;
        String sqlStr = "select record_type from log_record_table where" +
                " row = (select mutation_transaction_id from record_stats_table where path=?)";
        PreparedStatement statement;
        statement = conn.prepareStatement(sqlStr);
        statement.setString(1, path);
        ResultSet rs = statement.executeQuery();
        if (rs.next()) {
            // TODO: Add record to outstanding changes
            // TODO: Consider that if exist in record_stats_table, can't have DELETE tx
            isValid = rs.getInt("record_type") == LogRecord.LogRecordType.DELETE.ordinal();
        }

        if (!isValid) {
            // NOTE: Short-circuit if node exists
            return isValid;
        }

        // NOTE: Check parent exists and not ephemeral
        sqlStr = "select ephemeral_owner_id from record_stats_table where path=?";
        path = Paths.get(path).getParent().toString();
        statement = conn.prepareStatement(sqlStr);
        statement.setString(1, path);
        rs = statement.executeQuery();
        if (rs.next()) {
            // TODO: Add record to outstanding changes
            // TODO: Consider that if exist in record_stats_table, can't have DELETE tx
            isValid = rs.getLong("ephemeral_owner_id") == NONEPHEMERAL_OWNER_VALUE;
        } else {
            // NOTE: Parent path does not exist
            isValid = false;
        }
        return isValid;
    }

    private boolean tbdValidateSetCommit(Connection conn, String path,
                                         boolean validateVersionFlag, int expectedVersion) throws SQLException {
        // NOTE: If query fails, invalid due to missing node
        boolean isValid = false;
        String sqlStr = "select version from record_stats_table where path=?";
        PreparedStatement statement;
        statement = conn.prepareStatement(sqlStr);
        statement.setString(1, path);
        ResultSet rs = statement.executeQuery();
        if (rs.next()) {
            isValid = true;
            if (validateVersionFlag) {
                isValid = rs.getInt("version") == expectedVersion;
            }
        }
        return isValid;
    }

    private boolean tbdValidateDeleteCommit(Connection conn, String path,
                                            boolean validateVersionFlag, int expectedVersion) throws SQLException {
        // NOTE: If query fails, invalid due to missing node
        boolean isValid = false;
        String sqlStr = "select child_count, version from record_stats_table where path=?";
        PreparedStatement statement;
        statement = conn.prepareStatement(sqlStr);
        statement.setString(1, path);
        ResultSet rs = statement.executeQuery();
        if (rs.next()) {
            // TODO: Add record to outstanding changes
            // TODO: Consider that if exist in record_stats_table, can't have DELETE tx
            // TODO: FIX CHILD COUNT VALIDATION
            isValid = rs.getInt("child_count") == 0;
            if (validateVersionFlag) {
                isValid = rs.getInt("version") == expectedVersion;
            }
        }
        return isValid;
    }

    private Set<DeleteCommand> generateEphemeralDeletes(Connection conn, Set<Long> removedOwners) throws SQLException {
        HashSet<DeleteCommand> pendingDeletes = new HashSet<>();
//        try (Connection conn = ConnectionPool.getConnection()) {
        // TODO: Delete ephemerals; Consider helper function. UseWatchexecutor?
        //   select from ephemeral_record_table based on owner_id
        //   generatedeletecmd; execute;
        ResultSet rsEphem;
        for (Long ownerId : removedOwners) {
            rsEphem = conn.createStatement().executeQuery(String.format(
                    "SELECT * FROM ephemeral_record_table" +
                            " WHERE owner_id = %d", ownerId));
            while (rsEphem.next()) {
                // TODO: immediate todo
                System.out.println(String.format("REMOVE ME - tbdDeleteEphemeralHelperFn " +
                        "- [path=%s]", rsEphem.getString("path")));

                pendingDeletes.add(DeleteCommand.generateDeleteCommand(
                        rsEphem.getString("path"), false, -1, true));
            }
        }
        return pendingDeletes;
    }

    // TODO: Consider a better design
    private void tbdDeleteEphemeralHelperFn(Set<Long> removedOwners) {
        Set<DeleteCommand> pendingDeletes;
        try (Connection conn = ConnectionPool.getConnection()) {
            pendingDeletes = generateEphemeralDeletes(conn, removedOwners);
        } catch (SQLException e) {
            // TODO: retry a few times
            // TODO: fix error message
            System.out.println(String.format("CRITICAL ERROR - Error in background log reader thread."));
            System.out.println(String.format("===\n"+e+"\n==="));
            e.printStackTrace(System.out);
            return;
        }

        int deleteIndex = 0;
        int deleteNum = pendingDeletes.size();
        for (DeleteCommand cmd : pendingDeletes) {
            deleteIndex++;
            // TODO: Consider creating another executor or renaming watch
            // TODO: Consider implementing retrying
            if (watchExecutor.isShutdown() || watchExecutor.isTerminated() ) {
                System.out.println(String.format(
                        "CRITICAL ERROR - LogReader - Manager Watch Executor Not Running"));
            }

            if (deleteIndex < deleteNum) {
                watchExecutor.submit(() -> {
                    try {
                        cmd.executeManager(this);
                    } catch (InvalidServiceStateException e) {
                        System.out.println(String.format(
                                "CRITICAL ERROR - Ephemeral Delete Failed"));
                        e.printStackTrace(System.out);
                    }
                });
            } else {
                watchExecutor.submit(() -> {
                    try {
                        cmd.executeManager(this);
                    } catch (InvalidServiceStateException e) {
                        System.out.println(String.format(
                                "CRITICAL ERROR - Ephemeral Delete Failed"));
                        e.printStackTrace(System.out);
                    }
                    releaseEphemeralLease();
                });
            }
        }
    }

    private boolean validateLog(long logValidationValue) {
        return logValidationValue == getLogValidationToken();
    }

    private boolean validateLog(long logValidationValue, Connection conn) throws SQLException {
        return logValidationValue == getLogValidationToken(conn);
    }

    // TODO: Clean up API calls
    public boolean commitToLog(long logValidationValue, LogRecord logRecord,
                                CommitOptions options) throws InvalidServiceStateException {
        return commitToLog(logValidationValue, logRecord, options.getExpectedVersion(),
                            options.getValidateVersionFlag(), options.getSequentialFlag(), options.isDryRun());
    }

    public boolean commitToLog(long logValidationValue, LogRecord logRecord, int expectedVersion,
                               boolean validateVersionFlag, boolean sequentialFlag, boolean dryRun)
            throws InvalidServiceStateException {
        try (Connection conn = ConnectionPool.getConnection()) {
            // NOTE: ephemeral flag
//            return this.commitToLog(logValidationValue, logRecord,
//                    expectedVersion, validateVersionFlag, ephemeralFlag,
//                    conn, dryRun);
            return this.commitToLog(logValidationValue, logRecord,
                    expectedVersion, validateVersionFlag, sequentialFlag,
                    conn, dryRun);
        } catch (SQLException e) {
            System.out.println(String.format("Error - commitToLog - Failed to commit."));
            System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                    e.getSQLState()));
            e.printStackTrace(System.out);
            throw new InvalidServiceStateException("CRITICAL ERROR - Log Init - Failed to insert root node");
//            throw new RuntimeException(e);
        }
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

    // TODO: Standardize parameter order
    // TODO: Consider if assuming tx started is ok
    private void addWatcherTxHelper(Connection conn, String path,
                                    WatchConsumerWrapper<WatchInput> watchCallable, boolean txStartedFlag) throws SQLException {
//        Consumer<CommandOutput> watchCallable, boolean txStartedFlag) throws SQLException {
        // NOTE: Confirmed in ZK that if node does not exit, watch is not registered
        // TODO: properly implement watches after testing
        System.out.println(String.format(
                "REMOVE ME - addWatcherHelper - adding watch [table=log, filepath=%s]", path));

        WatchedEvent watch = new WatchedEvent(path, watchCallable);
        watch.setManagerId(managerId);
        // TODO: implement watch type
//        watch.setWatchType(WatchedEvent.WatchType.DATA);

        // TODO: add to table
        // TODO: Consider if transaction_id needed with new watch set up (multiple watches allowed)
        if (!txStartedFlag) {
            conn.createStatement().execute("ROLLBACK");
            conn.createStatement().execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        }
        PreparedStatement statement = conn.prepareStatement("INSERT INTO watch_table " +
                "(path, watch_type, owner_id)" +
                " VALUES (?, ?, ?) ON CONFLICT DO NOTHING");
        statement.setString(1, path);
        statement.setInt(2, watch.getWatchType().ordinal());
        statement.setLong(3, managerId);

        // TODO: Consider how insert can be validated
        statement.executeUpdate();
        // TODO: Consider where synchronized should go. This prevents table+struct desync.
        synchronized (watchStructMonitor) {
            // TODO: Properly design begin tx / commit
            conn.createStatement().execute("COMMIT");
            if (Objects.isNull(pathWatchMap.get(watch.getWatchType()).get(path))) {
                pathWatchMap.get(watch.getWatchType()).put(path, new HashSet<>());
            }
            pathWatchMap.get(watch.getWatchType()).get(path).add(watch);
        }
    }

    private ResultSet queryLog(Connection conn, String path, int rowFilter) throws SQLException {
//        String sqlStr = "select row, record_type, data, manager_id from log_record_table" +
//                " where path=? and row<=? order by row desc limit 1";
        if (pathToRowIndex.containsKey(path)) {
            String sqlStr = "select * from log_record_table where row=?";
            PreparedStatement statement;
            statement = conn.prepareStatement(sqlStr);
            statement.setInt(1, pathToRowIndex.get(path));
//            statement.setString(2, path);
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                if (rs.getString("path").equals(path)) {
                    return rs;
                }
                // TODO: Critical error if inmemory structs don't reflect log
            }
            // TODO: Critical error if query for a consumed path fails
        }
        return null;
    }

    private LogRecordStats retrieveStatsHelper(Connection conn, String path) throws SQLException, InterruptedException {
        // TODO: begin transaction?
        String statsStr = "SELECT * FROM record_stats_table WHERE path=?";
        PreparedStatement statement = conn.prepareStatement(statsStr);
        statement.setString(1, path);
        ResultSet rs = statement.executeQuery();
        LogRecordStats stats = null;
        int rowFilter;
        if (rs.next()) {
            rowFilter = rs.getInt("mutation_transaction_id");
            stats = new LogRecordStats(
                    rs.getInt("creation_transaction_id"), rowFilter,
                    rs.getInt("child_list_transaction_id"),
                    rs.getTimestamp("creation_timestamp").getTime(),
                    rs.getTimestamp("mutation_timestamp").getTime(),
                    rs.getInt("version"), rs.getInt("child_list_version"),
                    rs.getInt("child_sequential_version"), rs.getInt("child_count"),
                    rs.getInt("data"), rs.getLong("ephemeral_owner_id"));

            if (logCounter.get() < rowFilter && !localOutstandingTxnToPath.getMap().containsKey(rowFilter)) {
                rs = conn.createStatement().executeQuery(String.format(
                        "SELECT data, record_type, manager_id FROM log_record_table WHERE row = %d", rowFilter));
                if (!rs.next()) {
                    // TODO: HANDLE CRITICAL ERROR; should never happen with valid tx id
                    throw new RuntimeException("REMOVE ME - CRITICAL ERROR - retrieveStatsHelper");
                }
                addToOutstandingRecords(rowFilter, path,
                        new LogRecord(path, rs.getBytes("data"),
                                        LogRecord.LogRecordType.fromInteger(rs.getInt("record_type")),
                                        stats, rs.getLong("manager_id")));
                // TODO: Consider not waiting; and instead just adding to outstanding changes if above logcounter
//                synchronized(monitorObj) {
//                    while (DebugConstants.LOGREADER_FLAG && logCounter < rowFilter) {
//                        waitRowThreshold = Math.max(waitRowThreshold, rowFilter);
//                        monitorObj.wait();
//                    }
//                }
            }
        }
        return stats;
    }

    private Set<String> logSyncRetrieveChildren(Connection conn, String path) throws SQLException {
        HashSet<String> children;
        synchronized (localStructsMonitor) {
            children = Objects.isNull(pathChildrenMap.get(path))
                    ? null
                    : new HashSet<>(pathChildrenMap.get(path));
        }
//        HashSet<String> children = new HashSet<>(pathChildrenMap.get(path));
//        Path targetParentPath = Paths.get(path);
        // TODO: children can be null, should throw exception or properly reflect that node is missing
        ResultSet rs = conn.prepareStatement(String.format(
                "SELECT * FROM log_record_table WHERE row >= %d order by row asc",
                logCounter.get())).executeQuery();
        Path recordPath;
        LogRecord record;
        while (rs.next()) {
            recordPath = Paths.get(rs.getString("path"));
            // NOTE: Root node has 'null' parent
            if (Objects.isNull(recordPath.getParent())
                    || !path.equalsIgnoreCase(recordPath.getParent().toString())) {
                continue;
            }

            if (Objects.isNull(children)) {
                // NOTE: Path created between logCounter and end of log
                children = new HashSet<>();
            }

            // NOTE: Add matching path to pending changes local struct
            LogRecord.LogRecordType type = LogRecord.LogRecordType.fromInteger(
                    rs.getInt("record_type"));
//            long managerId = rs.getLong("manager_id");
//            long timestamp = rs.getTimestamp("record_timestamp").getTime();
//            int txId = rs.getInt("row");
//            byte[] data = rs.getBytes("data");
            record = new LogRecord(recordPath.toString(), rs.getBytes("data"),
                        type, null, rs.getLong("manager_id"));
            synchronized (outstandingChangesMonitor) {
                addToOutstandingRecords(rs.getInt("row"), recordPath.toString(), record);
            }

            // TODO: IMMEDIATE here; initialize maps, properly populate in LS and GET logic
            if (DebugConstants.DEBUG_FLAG) {
                System.out.println(String.format("REMOVE ME - DEBUG - sqlLogRetrieveChildren [path=%s, type=%s]",
                        recordPath.toString(), type.toString()));
            }
            String filename = Paths.get(recordPath.toString()).getFileName().toString();
            switch (Objects.requireNonNull(type)) {
                case CREATE_EPHEMERAL:
                case CREATE:
                    children.add(filename);
                    break;
                case DELETE:
                    children.remove(filename);
                    break;
                default:
                    //NOOP
                    break;
            }
        }
        return children;
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

    // TODO: Clean up api calls
    public CommandOutput retrieveChildren(String path, boolean statsFlag,
                                          WatchConsumerWrapper<WatchInput> watchCallable,
                                          boolean blockingFlag, boolean syncLogFlag) {
//        Consumer<CommandOutput> watchCallable, boolean blockingFlag) {
        // TODO: Validate flags; stats, watch, synclog, require blocking
        CommandOutput result = new CommandOutput();
        String output = "";
        Set<String> children = null;
        int txId = logCounter.get();
        if (blockingFlag) {
            try (Connection conn = ConnectionPool.getConnection()) {
                // TODO: Refactor getConnection so as to be able to rollback in catch
                conn.createStatement().execute("ROLLBACK");
                // TODO: Consider changing rowfilter to int
                long rowFilter = getLogValidationToken(conn);
                txId = Math.max(txId, (int)rowFilter);
                // TODO: Consider if this wait is needed
//                synchronized(monitorObj) {
//                    while (DebugConstants.LOGREADER_FLAG && logCounter < rowFilter) {
//                        waitRowThreshold = Math.max(waitRowThreshold, (int)rowFilter);
//                        monitorObj.wait();
//                    }
//                }
//                conn.createStatement().execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");

                if (statsFlag) {
                    LogRecordStats stats = retrieveStatsHelper(conn, path);
                    if (!Objects.isNull(stats)) {
                        result.setOutput(stats.toString());
                        txId = Math.max(stats.getChildrenTransactionId(), txId);
                    } else {
                        // TODO: consider proper missing node path
                        // TODO: consider one run while loop, to be able to break (java goto) ;
                        //  labeled break
                        conn.createStatement().execute("COMMIT");
                        output = String.format("List Children Operation Failed - Missing Node.");
                        result.populateError(1, output);
                        if (DebugConstants.DEBUG_FLAG) {
                            System.out.println(String.format("REMOVE ME - ERROR - retrieveChildren" +
                                            " [output=%s]", output));
                        }
                        return result;
                    }
                }

                // TODO: Check if path exists
                if (syncLogFlag) {
                    children = logSyncRetrieveChildren(conn, path);
                } else {
                    children = pathChildrenMap.get(path);
                }

                if (Objects.isNull(children)) {
                    conn.createStatement().execute("COMMIT");
                    output = String.format("List Children Operation Failed - Missing Node.");
                    result.populateError(1, output);
                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println(String.format("REMOVE ME - ERROR - retrieveChildren" +
                                " [output=%s]", output));
                    }
                    return result;
                }

                // TODO: Consider if sync is needed for watch add here
                // TODO: add flag check
                if (!Objects.isNull(watchCallable)) {
                    if (watchCallable.getEvaluateThresholdFlag()) {
                        watchCallable.setTxIdThreshold(txId);
                    }
                    addWatcherTxHelper(conn, path, watchCallable, true);
                } else {
                    conn.createStatement().execute("COMMIT");
                }
            } catch (SQLException e) {
                System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                        e.getSQLState()));
                e.printStackTrace(System.out);
                System.out.println(String.format(
                        "REMOVE ME - retrieveChildren - SQL Error [path=%s]", path));
                output = String.format("List Children Operation Failed - SQL Error");
                // TODO: Add error code constants
                result.populateError(2, output);
                return result;
//                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                System.out.println(String.format(
                        "REMOVEME - retrieveChildren - InterruptedException [path=%s]", path));
                output = String.format("List Children Operation Failed - InterruptedException");
                result.populateError(1, output);
                return result;
//                throw new RuntimeException(e);
            }
        } else {
            children = Objects.isNull(pathChildrenMap.get(path))
                    ? null
                    : new HashSet<>(pathChildrenMap.get(path));
        }

        if (Objects.isNull(children)) {
//            System.out.println(String.format("REMOVEME - cSvcInitializationHelper - [fetchMaster=%s]", fetchMaster));

            System.out.println(String.format(
                    "REMOVEME - retrieveChildren " +
                    "- null output - missing node [path=%s]", path));
            output = String.format("List Children Operation Failed - Missing Node.");
            result.populateError(1, output);
            return result;
        }
//        result.setChildrenSet(Collections.unmodifiableSet(pathChildrenMap.get(path)));
        result.setChildrenSet(children);
        result.setErrorCode(0);
        result.setExistFlag(true);
        return result;
    }

    public LogRecord retrieveFromCache(String path, boolean statsFlag, boolean readThroughFlag) {
        if (cacheFlag) {
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


    // TODO: Properly implement retrievefromlog with watches
    public LogRecord retrieveFromLog(String path, boolean getStatsFlag,
                                     WatchConsumerWrapper<WatchInput> watchCallable) {
//        public LogRecord retrieveFromLog(String path, boolean getStatsFlag, Consumer<CommandOutput> watchCallable) {
        boolean queryLogFlag = true;
        String output;
//        boolean missingFlag = false;
        // TODO: consider other options for 'missing record' return
        LogRecord resultRecord = new LogRecord(DELETED_RECORD_PATH, new byte[0], LogRecord.LogRecordType.DELETE);
        // More Validations
        // TODO: Cache
//        if (!pathMetadataMap.containsKey(key)) {
//            output = String.format("RetrieveFromLog Failed - Invalid Node Path [filepath=%s]", key);
////            result.populateError(1, output);
////            return result;
//            System.out.println(output);
//            return null;
//        }
        // TODO: Check if in 'outstandingRecords' local struct
        // TODO: Consider sync on outstandingRecords monitor obj
        if (localOutstandingTxnToPath.getReverseMap().getOrDefault(path, -1) > logCounter.get()) {
            if (!getStatsFlag || localOutstandingPathToRecord.get(path).containsStats()) {
//                return localOutstandingPathToRecord.get(path);
                // TODO: redesign retrievefromlog; logic too convoluted
                resultRecord = localOutstandingPathToRecord.get(path);
                if (DebugConstants.DEBUG_FLAG) {
                    System.out.println(String.format("\t\tREMOVE ME - DEBUG - retrieveFromLog " +
                            "- localOutstandingPathToRecord [path=%s,\n\t\tlocalOutstandingPathToRecord=%s]",
                            path, localOutstandingPathToRecord.keySet()));
                }
                if (Objects.isNull(watchCallable)) {
                    return resultRecord;
                } else {
                    // NOTE: Disable statsflag due to pre-populated record
                    getStatsFlag = false;
                    queryLogFlag = false;
                }
            }
        }
        if (DebugConstants.DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - retrieveFromLog " +
                            "- skipped outstanding structs [path=%s,\n\t\tlocalOutstandingPathToRecord=%s]",
                    path, localOutstandingPathToRecord.keySet()));
        }
        // TODO: Consider leveraging inmemory structs more rather than querying

        // TODO: figure out what validation needs to be done
//        if (validateLog(logTimestamp)) {
        try (Connection conn = ConnectionPool.getConnection()) {
            Statement tx_stmt = conn.createStatement();

            // TODO: Figure out where to begin tx
//            tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");

            String sqlStr = "select row, record_type, data, manager_id from log_record_table" +
                    " where path=? and row<=? order by row desc limit 1";
            PreparedStatement statement;
            // TODO: consider other options for tagging watches with a water mark
            int txId = logCounter.get();
            int rowFilter = txId;
            LogRecordStats stats = null;
            WatchedEvent watch = null;
            ResultSet rs = null;
            if (getStatsFlag) {
                // TODO: Test if helper is good enough
                synchronized (outstandingChangesMonitor) {
                    stats = retrieveStatsHelper(conn, path);
                    if (Objects.isNull(stats)) {
                        return resultRecord;
                    }
                    // TODO: sync local structs
                    if (localOutstandingTxnToPath.getReverseMap().containsKey(path)
                            && stats.getModifiedTransactionId() == localOutstandingTxnToPath.getReverseMap().get(path)) {
                        resultRecord = localOutstandingPathToRecord.get(path);
                        resultRecord.setStats(stats);
                        txId = stats.getModifiedTransactionId();
                        queryLogFlag = false;
                        // TODO: make this cleaner; should not be possible for it to be DELETE type
                        if (resultRecord.getType().equals(DELETE)) {
                            return resultRecord;
                        }
                        // TODO: Check if watch if not delete type
//                    return resultRecord;
                    }
                }
            }

            // TODO: Move to helper function
            if (queryLogFlag) {
                rs = queryLog(conn, path, rowFilter);
                if (Objects.isNull(rs)) {
                    System.out.println(String.format("RetrieveFromLog Failed - Missing node path [path=%s]", path));
                    return resultRecord;
                }
                txId = Math.max(txId, rs.getInt("row"));
                resultRecord = new LogRecord(path,
                        rs.getBytes("data"),
                        LogRecord.LogRecordType.fromInteger(rs.getInt("record_type")),
                        stats,
                        rs.getLong("manager_id"));
            }

            if (!Objects.isNull(watchCallable) && !resultRecord.getType().equals(DELETE)) {
                if (watchCallable.getEvaluateThresholdFlag()) {
                    watchCallable.setTxIdThreshold(txId);
                }
                addWatcherTxHelper(conn, path, watchCallable, false);
            }
            return resultRecord;
        } catch (SQLException e) {
            output = String.format("RetrieveFromLog Failed. [\nerrorTrace=\n%s\n]", e.getMessage());
            System.out.println("An error occurred.");
            System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                    e.getSQLState()));
            e.printStackTrace(System.out);
            System.out.println(output);
            return null;
        } catch (InterruptedException e) {
            System.out.println(String.format("FATAL ERROR - interrupted while waiting on logreader [path=%s]",
                    path));
            throw new RuntimeException(e);
        }
    }

    // TODO: Properly implement retrievefromlog with watches
    public LogRecord retrieveFromLogOriginal(String path, boolean getStatsFlag,
                                     WatchConsumerWrapper<WatchInput> watchCallable) {
//        public LogRecord retrieveFromLog(String path, boolean getStatsFlag, Consumer<CommandOutput> watchCallable) {
        boolean logQueryFlag = true;
        String output;
//        boolean missingFlag = false;
        // TODO: consider other options for 'missing record' return
        LogRecord resultRecord = new LogRecord(DELETED_RECORD_PATH, new byte[0], LogRecord.LogRecordType.DELETE);
        // More Validations
        // TODO: Cache
//        if (!pathMetadataMap.containsKey(key)) {
//            output = String.format("RetrieveFromLog Failed - Invalid Node Path [filepath=%s]", key);
////            result.populateError(1, output);
////            return result;
//            System.out.println(output);
//            return null;
//        }
        // TODO: Check if in 'outstandingRecords' local struct
        // TODO: Consider sync on outstandingRecords monitor obj
        if (localOutstandingTxnToPath.getReverseMap().getOrDefault(path, -1) > logCounter.get()) {
            if (!getStatsFlag || localOutstandingPathToRecord.get(path).containsStats()) {
//                return localOutstandingPathToRecord.get(path);
                // TODO: redesign retrievefromlog; logic too convoluted
                resultRecord = localOutstandingPathToRecord.get(path);
                if (Objects.isNull(watchCallable)) {
                    return resultRecord;
                } else {
                    // NOTE: Disable statsflag due to pre-populated record
                    getStatsFlag = false;
                    logQueryFlag = false;
                }
            }
        }
        // TODO: Consider leveraging inmemory structs more rather than querying

        // TODO: figure out what validation needs to be done
//        if (validateLog(logTimestamp)) {
        try (Connection conn = ConnectionPool.getConnection()) {
            Statement tx_stmt = conn.createStatement();

            // TODO: Figure out where to begin tx
//            tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");

            String sqlStr = "select row, record_type, data, manager_id from log_record_table" +
                    " where path=? and row<=? order by row desc limit 1";
            PreparedStatement statement;
            // TODO: consider other options for tagging watches with a water mark
            int txId = logCounter.get();
            int rowFilter = txId;
            LogRecordStats stats = null;
            WatchedEvent watch = null;
            ResultSet rs = null;
            if (getStatsFlag) {
                // TODO: Test if helper is good enough
                stats = retrieveStatsHelper(conn, path);
                if (Objects.isNull(stats)) {
                    return resultRecord;
                }
                // TODO: sync local structs
                if (stats.getModifiedTransactionId() == localOutstandingTxnToPath.getReverseMap().get(path)) {
                    resultRecord = localOutstandingPathToRecord.get(path);
                    resultRecord.setStats(stats);
                    txId = stats.getModifiedTransactionId();
                    logQueryFlag = false;
                    // TODO: make this cleaner; should not be possible for it to be DELETE type
                    if (resultRecord.getType().equals(DELETE)) {
                        return resultRecord;
                    }
                    // TODO: Check if watch if not delete type
//                    return resultRecord;
                }
            }

            tx_stmt.execute("ROLLBACK");
            tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
            statement = conn.prepareStatement(sqlStr);
            statement.setString(1, path);
            statement.setInt(2, rowFilter);
            rs = statement.executeQuery();
            if (rs.next()) {
                // TODO: properly implement processing deleted nodes
//                missingFlag = rs.getInt("record_type") != LogRecord.LogRecordType.DELETE.ordinal();
//                if (!missingFlag) {
                if (rs.getInt("record_type") != LogRecord.LogRecordType.DELETE.ordinal()) {
                    txId = Math.max(txId, rs.getInt("row"));
                    // NOTE: Confirmed in ZK that if node does not exit, watch is not registered
                    // TODO: properly implement watches after testing
                    if (!Objects.isNull(watchCallable)) {
                        if (watchCallable.getEvaluateThresholdFlag()) {
                            watchCallable.setTxIdThreshold(txId);
                        }
                        addWatcherTxHelper(conn, path, watchCallable, true);
                    }

                    return new LogRecord(path,
                            rs.getBytes("data"),
                            LogRecord.LogRecordType.fromInteger(rs.getInt("record_type")),
                            stats,
                            rs.getLong("manager_id"));
                } else {
//                    missingFlag = true;
                    tx_stmt.execute("COMMIT");
                    return resultRecord;
                }
            } else {
//                System.out.println("An error occurred.");
                System.out.println(String.format("RetrieveFromLog Failed - Missing node path [path=%s]", path));
//                tx_stmt.execute("ROLLBACK");
                tx_stmt.execute("COMMIT");
                return resultRecord;
//                return null;
            }
        } catch (SQLException e) {
            output = String.format("RetrieveFromLog Failed. [\nerrorTrace=\n%s\n]", e.getMessage());
            System.out.println("An error occurred.");
            System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                    e.getSQLState()));
            e.printStackTrace(System.out);
            System.out.println(output);
            return null;
        } catch (InterruptedException e) {
            System.out.println(String.format("FATAL ERROR - interrupted while waiting on logreader [path=%s]",
                    path));
            throw new RuntimeException(e);
        }
    }

    public LogRecord retrieveFromLog(String path, boolean getStatsFlag) {
        return retrieveFromLog(path, getStatsFlag, null);
    }

    public LogRecord retrieveFromLog(String key) {
        return retrieveFromLog(key, false);
    }

    public void testWrite(byte[] data) {
        try (Connection conn = ConnectionPool.getConnection()) {
//        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
            Statement tx_stmt = conn.createStatement();
//            PreparedStatement stmt = conn.prepareStatement("INSERT INTO log_record_table (row, path, record_type, data) VALUES (?, ?, ?, ?)");
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO log_record_table (path, record_type, data) VALUES (?, ?, ?)");
            int counter = 0;
//            stmt.setInt(1, 0);
            stmt.setInt(2, 1);
            stmt.setBytes(3, data);
            long logCount = this.getLogValidationToken(conn);
            while (counter < 20 * 20 + 1) {
                stmt.setString(1, "test/" + counter);
                counter += 1;
//                tx_stmt.execute("BEGIN TRANSACTION");
                tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
//                logCount = this.getLogTimestamp();
//                logCount = this.getLogValidationToken();
//                logCount = this.getLogValidationToken(conn);
                int result = stmt.executeUpdate();
                if (this.getLogValidationToken(conn) == logCount + 1) {
                    logCount += 1;
                } else {
                    System.out.println(String.format("DEBUG - Commit failed [logCount=%d, validationToken=%d]",
                            logCount, this.getLogValidationToken(conn)));
                    break;
                }
                if (result <= 0) {
                    // NOTE: Failed update
                    System.out.println(String.format("DEBUG - Commit failed [validationToken=%d]",
                            this.getLogValidationToken(conn)));
                    tx_stmt.execute("ROLLBACK");
                }
                tx_stmt.execute("COMMIT");
//                if (this.validateLog(logCount, conn)) {
//                    tx_stmt.execute("COMMIT");
//                } else {
//                    // TODO: remove debug
//                    System.out.println(String.format("DEBUG - Validation failed [expected=%d, logCount=%d]", logCount, this.getLogTimestamp()));
//                    tx_stmt.execute("ROLLBACK");
//                    break;
//                }
            }
        } catch (SQLException e) {
            System.out.println(String.format("Error sandbox texting connection."));
            System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                    e.getSQLState()));
            e.printStackTrace(System.out);
            throw new RuntimeException(e);
        }

    }

    class WriteSleepThread implements Runnable {

        Connection conn;
        int dummyInt;

        public WriteSleepThread(Connection conn, int dummyInt) {
            this.conn = conn;
            this.dummyInt = dummyInt;
        }

        private long getLogTimeStampWithConn() {
            // TODO: deprecated
            try {
                Statement tx_stmt = conn.createStatement();
//            PreparedStatement stmt = conn.prepareStatement("INSERT INTO log_record_table (row, path, record_type, data) VALUES (?, ?, ?, ?)");
                PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) FROM log_record_table");
                ResultSet rs = stmt.executeQuery();
                int counter = 0;
                if (rs.next()) {
                    counter = rs.getInt(1);
//                System.out.println(String.format("DEBUG - getlogtimestamp success [logCount=%d].", counter));
                } else {
                    // TODO: remove DEBUG
                    System.out.println(String.format("DEBUG - getlogtimestamp failure."));
                }
                return counter;
            } catch (SQLException e) {
                System.out.println(String.format("Error - getLogTimestamp - Failed to connect."));
                System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                        e.getSQLState()));
                e.printStackTrace(System.out);
                throw new RuntimeException(e);
            }
        }


        public void run() {
            System.out.println(String.format("WriteSleepThread - Start"));
//            try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
            try {
                String generatedString = RandomStringUtils.randomAlphanumeric(1000);
                byte[] data = generatedString.getBytes(StandardCharsets.UTF_8);
                Statement txStmt = conn.createStatement();
                txStmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                long logLength = this.getLogTimeStampWithConn();
                System.out.println(String.format("WriteSleepThread - Begin Tx [logLength=%d]", logLength));
//            PreparedStatement stmt = conn.prepareStatement("INSERT INTO log_record_table (row, path, record_type, data) VALUES (?, ?, ?, ?)");
                PreparedStatement stmt = conn.prepareStatement("INSERT INTO log_record_table (path, record_type, data) VALUES (?, ?, ?)");
                PreparedStatement stmtCntrQry = conn.prepareStatement("SELECT log_counter FROM log_validation_counter");
                ResultSet rs = stmtCntrQry.executeQuery();
                int counter = 0;
                if (rs.next()) {
                    counter = rs.getInt(1);
//                System.out.println(String.format("DEBUG - getlogtimestamp success [logCount=%d].", counter));
                } else {
                    // TODO: remove DEBUG
                    System.out.println(String.format("DEBUG - getlogtimestamp failure."));
                }
//                PreparedStatement validationStmt = conn.prepareStatement(String.format("UPDATE log_validation_counter SET log_counter=log_counter+1;"));
//                PreparedStatement validationStmt = conn.prepareStatement(String.format("UPDATE log_validation_counter SET log_counter=%d", this.dummyInt+1));
                PreparedStatement validationStmt = conn.prepareStatement(String.format("UPDATE log_validation_counter SET log_counter=%d", counter + 1));
//            stmt.setInt(1, 0);
                stmt.setInt(2, 1);
                stmt.setBytes(3, data);
                long logCount = 0;
                stmt.setString(1, "test/" + counter);
//                counter += 1;
//                    logCount = this.getLogTimestamp();
                stmt.executeUpdate();
                validationStmt.executeUpdate();
                logLength = this.getLogTimeStampWithConn();
                System.out.println(String.format("WriteSleepThread - Sleep [logLength=%d]", logLength));
                try {
                    Thread.sleep(3 * 1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                txStmt.execute("COMMIT");
                System.out.println(String.format("WriteSleepThread - Commit"));
//                    if (this.validateLog(logCount)) {
//                        tx_stmt.execute("COMMIT");
//                    } else {
//                        // TODO: remove debug
//                        System.out.println(String.format("DEBUG - Validation failed [expected=%d, logCount=%d]", logCount, this.getLogTimestamp()));
//                        tx_stmt.execute("ROLLBACK");
//                        break;
//                    }
            } catch (SQLException e) {
                System.out.println(String.format("Error sandbox texting connection."));
                System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                        e.getSQLState()));
                e.printStackTrace(System.out);
                throw new RuntimeException(e);
            }
        }
    }

    public boolean testSlowCommit(long logValidationValue, LogRecord logRecord,
                                  int expectedVersion, boolean validateVersionFlag,
                                  Connection conn, boolean dryRun) throws SQLException {
        // TODO: Currently disabled
        return false;
    }

    // NOTE: Purpose is to check what happens if 2 transactions successfully validate log count and then try to commit.
    public void testParallelCommit() throws InterruptedException {
        long logCount = this.getLogTimestamp();
        Connection conn1 = null;
        Connection conn2 = null;
        try {
//        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
            conn1 = DriverManager.getConnection(CONNECTION_STRING, properties);
            conn2 = DriverManager.getConnection(CONNECTION_STRING, properties);
            Thread workerOne = new Thread(new WriteSleepThread(conn1, (int) logCount));
            Thread workerTwo = new Thread(new WriteSleepThread(conn2, (int) logCount));
            workerOne.start();
            workerTwo.start();
            workerOne.join();
            workerTwo.join();
            conn1.close();
            conn2.close();
        } catch (SQLException e) {
            System.out.println(String.format("Error sandbox texting connection."));
//            if (!Objects.isNull(conn1)) {
//                conn1.close();
//            }
//
//            if (!Objects.isNull(conn2)) {
//                conn2.close();
//            }
            System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                    e.getSQLState()));
            e.printStackTrace(System.out);
            throw new RuntimeException(e);
        }
        System.out.println(String.format("DEBUG - testParallelCommit [startCount=%d, endCount=%d]", logCount, this.getLogTimestamp()));

    }

    private void initializeMetadataStore() {
//        // TODO: Refactor this to use a proper persistence store (AmazonDB?)
//        // TODO: add base root to table
    }

    private void initializeServerMetadata() throws IOException {
//        // TODO: properly transform into table
    }

    private void initializeLeasePath() throws IOException {
//        // TODO: properly transform into table
    }

    public boolean initializeUniqueId() throws InvalidServiceStateException {
//        if (Objects.isNull(uuid)) {
//            uuid = UUID.randomUUID();
//            // TODO: check for id clashes in filesystem this.idpath/this.uuid.toString()
//        }
//        Random rndm = new Random();
//        byte[] rndmB = new byte[8];
//        rndm.nextBytes(rndmB);
        // TODO: check for collisions
        // TODO: consider general liveness table?
        try (Connection conn = ConnectionPool.getConnection()) {
            boolean collisionFlag = true;
            while (collisionFlag) {
                managerId = new Random().nextLong();
                managerId = managerId >= 0 ? managerId : -1 * managerId;
                PreparedStatement stmtEph = conn.prepareStatement("INSERT INTO ephemeral_owner_table " +
                        "(owner_id) VALUES (?) ON CONFLICT DO NOTHING");
                stmtEph.setLong(1, managerId);
                // TODO: consider better naming conventions
                try {
                    collisionFlag = stmtEph.executeUpdate() == 0;
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                    collisionFlag = true;
                }
            }
        } catch (SQLException e) {
            // TODO: Change runtime exception to InvalidServiceStateException
            throw new RuntimeException(e);
        }
        return true;
    }

    public boolean extendSoftLease(Connection conn) throws InvalidServiceStateException {
        try {
            int extensionLength = 500;
            PreparedStatement stmt = conn.prepareStatement(String.format("UPDATE lease_table" +
                    " SET expire_timestamp = CURRENT_TIMESTAMP + interval '%d milliseconds'" +
                    " WHERE owner_id = ?", extensionLength));
            stmt.setLong(1, managerId);
            int rowCount = stmt.executeUpdate();
            conn.commit();
            return rowCount > 0;
        } catch (SQLException e) {
            throw new InvalidServiceStateException(e);
        }
    }

    public boolean extendSoftLease() throws InvalidServiceStateException {
        try (Connection conn = ConnectionPool.getConnection()) {
            return extendSoftLease(conn);
        } catch (SQLException e) {
            throw new InvalidServiceStateException(e);
        }
    }


    public boolean extendSoftLeaseOld() throws InvalidServiceStateException {
        try (Connection conn = ConnectionPool.getConnection()) {
            Statement tx_stmt = conn.createStatement();
            tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
//            "INSERT INTO lease_table (owner_id, expire_timestamp) VALUES (%d, CURRENT_TIMESTAMP + interval '2 seconds')" +
//                    " ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
//                    " SET owner_id = %d, expire_timestamp = CURRENT_TIMESTAMP + interval '2 seconds'" +
//                    " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP"
            int leaseLength = 2;
//            PreparedStatement stmtOld = conn.prepareStatement(String.format("INSERT INTO lease_table (owner_id, expire_timestamp)" +
//                    " VALUES (?, CURRENT_TIMESTAMP + interval '%d seconds') ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
//                    " SET owner_id = ?, expire_timestamp = CURRENT_TIMESTAMP + interval '%d seconds'" +
//                    " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP", leaseLength, leaseLength));
            PreparedStatement stmt = conn.prepareStatement(String.format("UPDATE lease_table" +
                    " SET expire_timestamp = CURRENT_TIMESTAMP + interval '%d seconds'" +
                    " WHERE owner_id = ?", leaseLength));
            stmt.setLong(1, managerId);
            int rowCount = stmt.executeUpdate();
            tx_stmt.execute("COMMIT");
            return rowCount > 0;
//            if (stmt.executeUpdate() < 1) {
//                return false;
//            } else {
//                return true;
//            }
        } catch (SQLException e) {
            System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                    e.getSQLState()));
            e.printStackTrace(System.out);
            throw new InvalidServiceStateException(e);
        }
    }

    public boolean claimEphemeralLease() {
        try (Connection conn = ConnectionPool.getConnection()) {
            Statement tx_stmt = conn.createStatement();
//            tx_stmt.execute("ROLLBACK");
            conn.rollback();
            tx_stmt.execute("BEGIN TRANSACTION");
            // TODO: exit quickly if table locked
            tx_stmt.execute("LOCK TABLE ephemeral_lease_table NOWAIT");
//            "INSERT INTO lease_table (owner_id, expire_timestamp) VALUES (%d, CURRENT_TIMESTAMP + interval '2 seconds')" +
//                    " ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
//                    " SET owner_id = %d, expire_timestamp = CURRENT_TIMESTAMP + interval '2 seconds'" +
//                    " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP"
            int leaseLength = 1;
            PreparedStatement stmt = conn.prepareStatement(String.format(
                "INSERT INTO ephemeral_lease_table (owner_id, expire_timestamp)" +
                " VALUES (?, CURRENT_TIMESTAMP + interval '%d seconds') ON CONFLICT ON CONSTRAINT pk_elt DO UPDATE" +
                " SET owner_id = ?, expire_timestamp = CURRENT_TIMESTAMP + interval '%d seconds'" +
                " WHERE ephemeral_lease_table.expire_timestamp < CURRENT_TIMESTAMP", leaseLength, leaseLength));
            stmt.setLong(1, managerId);
            stmt.setLong(2, managerId);
            int rowCount = stmt.executeUpdate();
//            tx_stmt.execute("COMMIT");
            conn.commit();
//            ephemeralLeaseAtomic.set(rowCount > 0);
            return rowCount > 0;
//            if (stmt.executeUpdate() < 1) {
//                return false;
//            } else {
//                return true;
//            }
        } catch (SQLException e) {
            // TODO: Ignore exception and properly handle failed lease claim
            if (e.getSQLState().equalsIgnoreCase("55P03")
                || e.getSQLState().equalsIgnoreCase("40001")) {
                // NOTE: 55P03->lock_not_available
                // NOTE: 40001->serialization_failure
                if (DebugConstants.DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - claimEphemeralLease - Expected exception [SQLState=%s]",
                            e.getSQLState()));
                }
            } else {
                System.out.println(String.format("ERROR - claimEphemeralLease - Unexpected Exception",
                        e.getSQLState()));
                System.out.println(String.format("ERROR - claimEphemeralLease - SQLState [SQLState=%s]",
                        e.getSQLState()));
                e.printStackTrace(System.out);
            }
            return false;
//            throw new RuntimeException(e);
        }
    }

    public void releaseEphemeralLease() {
        try (Connection conn = ConnectionPool.getConnection()) {
            Statement tx_stmt = conn.createStatement();
            conn.rollback();
//            tx_stmt.execute("ROLLBACK");
//            synchronized (ephemeralMonitor) {
            tx_stmt.execute("BEGIN TRANSACTION");
            tx_stmt.execute("LOCK TABLE ephemeral_lease_table");
//            "INSERT INTO lease_table (owner_id, expire_timestamp) VALUES (%d, CURRENT_TIMESTAMP + interval '2 seconds')" +
//                    " ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
//                    " SET owner_id = %d, expire_timestamp = CURRENT_TIMESTAMP + interval '2 seconds'" +
//                    " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP"
            PreparedStatement stmt = conn.prepareStatement("DELETE FROM ephemeral_lease_table WHERE owner_id = ?");
            stmt.setLong(1, managerId);
            // TODO: Consider if lease release should give any information
            stmt.executeUpdate();
//            tx_stmt.execute("COMMIT");
            conn.commit();
//            }

//            if (stmt.executeUpdate() < 1) {
//                return false;
//            } else {
//                return true;
//            }
        } catch (SQLException e) {
            if (e.getSQLState().equalsIgnoreCase(LOCK_NOT_AVAILABLE.getState())
                    || e.getSQLState().equalsIgnoreCase(SERIALIZATION_FAILURE.getState())) {
                if (DebugConstants.DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - releaseEphemeralLease - Expected exception [SQLState=%s]",
                            e.getSQLState()));
                }
            } else {
                System.out.println(String.format("ERROR - releaseEphemeralLease - Unexpected Exception",
                        e.getSQLState()));
                System.out.println(String.format("ERROR - releaseEphemeralLease - SQLState [SQLState=%s]",
                        e.getSQLState()));
                e.printStackTrace(System.out);
            }
//            throw new RuntimeException(e);
        }
    }

    public Future<Boolean> submitSoftLeaseClaim() {
        return backgroundLeaseHandler.submit(new DBLeaseClaimer(this));
    }

    // TODO: Clean up lease api (test / outdated)
    @Override
    public boolean claimSoftLease() {
        return claimSoftLease(false);
    }

    public int updateLeaseClaim(Connection conn, int ticket) throws SQLException {
        // TODO: Outdated; Could not be contained in helper fn
//        PreparedStatement stmt = conn.prepareStatement(String.format(
//            "INSERT INTO ephemeral_lease_table (owner_id, expire_timestamp)" +
//            " VALUES (?, CURRENT_TIMESTAMP + interval '%d seconds') ON CONFLICT ON CONSTRAINT pk_elt DO UPDATE" +
//            " SET owner_id = ?, expire_timestamp = CURRENT_TIMESTAMP + interval '%d seconds'" +
//            " WHERE ephemeral_lease_table.expire_timestamp < CURRENT_TIMESTAMP", leaseLength, leaseLength));
//        "UPDATE ephemeral_lease_table SET expire_timestamp = CURRENT_TIMESTAMP + interval '%d seconds'" +
//                    " WHERE ticket = ? AND owner_id = ?", leaseLength))
        PreparedStatement stmt;
        Statement tx_stmt = conn.createStatement();
        tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");

        stmt = conn.prepareStatement("SELECT COUNT(*) FROM ephemeral_lease_table" +
                " WHERE ticket < ? AND expire_timestamp < CURRENT_TIMESTAMP");
        stmt.setInt(1, ticket);
        ResultSet rs = stmt.executeQuery();
        if (!rs.next()) {
            // TODO: Error out
        }
        int queueCount = rs.getInt(1);
        // TODO: Concern
        int sleepMS = 20;

        if (queueCount == 0) {
            // TODO: Attempt to claim lease
        }

        if (queueCount < 3) {
            sleepMS = 0;
        } else {
            sleepMS = Math.min(queueCount*4, 30);
        }

        // TODO: Create class variable
//        int leaseLength = 4;
        stmt = conn.prepareStatement(String.format(
                    "UPDATE ephemeral_lease_table" +
                    " SET expire_timestamp = CURRENT_TIMESTAMP + interval '%d milliseconds'" +
                    " WHERE ticket = ? AND owner_id = ?", sleepMS+6));
        stmt.setInt(1, ticket);
        stmt.setLong(2, managerId);
        if (stmt.executeUpdate() < 1) {
            // TODO: Throw CRITICAL ERROR as ticket or owner_id are missing
        }


        return -1;
    }

    public boolean releaseSoftLease() {
        try (Connection conn = ConnectionPool.getConnection()) {
            conn.createStatement().execute(String.format("DELETE FROM lease_table" +
                                            " WHERE lock_row = 'L' AND owner_id = %d", managerId));
            conn.commit();
        } catch (SQLException e) {
            // TODO: Log
            System.out.println(String.format("DEBUG - testReleaseSoftLease - Failed to release lease [SQLState=%s]",
                    e.getSQLState()));
            throw new RuntimeException(e);
        }
        return false;
    }

    public int addLeaseWaitlist(Connection conn) {
//        try (Connection conn = ConnectionPool.getConnection()) {
        int ticket = -1;
        // TODO: Consider proper retry design
//        int retry = 0;
//        int retryMax = 10;
//        while (ticket == -1 && retry < retryMax) {
        while (ticket == -1) {
            try {
//                retry++;
                Statement tx_stmt = conn.createStatement();
                tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
//            "INSERT INTO lease_table (owner_id, expire_timestamp) VALUES (%d, CURRENT_TIMESTAMP + interval '2 seconds')" +
//                    " ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
//                    " SET owner_id = %d, expire_timestamp = CURRENT_TIMESTAMP + interval '2 seconds'" +
//                    " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP"
                // COALESCE(MAX(column1), 0) + 1
                // SELECT COALESCE(MAX(column1), 0) + 1 FROM my_table
                // INSERT INTO lease_waitlist_table (ticket, owner_id, expire_timestamp) VALUES ((SELECT COALESCE(MAX(ticket), 0) + 1 FROM lease_waitlist_table), 534, CURRENT_TIMESTAMP + interval '2 seconds') RETURNING ticket
                conn.prepareStatement(String.format("DELETE FROM lease_waitlist_table" +
                        " WHERE expire_timestamp < CURRENT_TIMESTAMP")).executeUpdate();

                int leaseLength = 6;
                PreparedStatement stmt = conn.prepareStatement(String.format(
                        "INSERT INTO lease_waitlist_table (ticket, owner_id, expire_timestamp)" +
                            " VALUES ((SELECT COALESCE(MAX(ticket), 0) + 1 FROM lease_waitlist_table), ?," +
                            " CURRENT_TIMESTAMP + interval '%d milliseconds')" +
                            " RETURNING ticket",
                        leaseLength));
                stmt.setLong(1, managerId);
//            int rowCount = 0;
                if (!stmt.execute()) {
                    // NOTE: Consume result count
//                rowCount = stmt.getUpdateCount();
                    if (!stmt.getMoreResults()) {
                        // NOTE: Failed update
//                    System.out.println(String.format("DEBUG - SQL Insert Failure " +
//                                    "- Invalid State [expected=%d, logCount=%d]",
//                            logValidationValue, this.getLogValidationToken(conn)));
                        tx_stmt.execute("ROLLBACK");
//                    return false;
//                        return -1;
                    }
                }
                ResultSet rs = stmt.getResultSet();
                if (!rs.next()) {
                    // TODO: Error out
                }
                ticket = rs.getInt("ticket");

//            int rowCount = stmt.execute();
                conn.commit();
//            tx_stmt.execute("COMMIT");
//            return rowCount > 0;
//            if (stmt.executeUpdate() < 1) {
//                return false;
//            } else {
//                return true;
//            }
            } catch (SQLException e) {
                // TODO: Ignore exception and properly handle failed lease claim
                // TODO: Wrap logging on debug flag
                System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                        e.getSQLState()));
                e.printStackTrace(System.out);
                ticket = -1;
//            throw new RuntimeException(e);
            }
        }
        return ticket;
    }

    // TODO: Consider if expected expire time
    // TODO: Adjust to throw a custom exception that wraps exceptions
    @Override
    public boolean claimSoftLease(boolean waitFlag) {
        try (Connection conn = ConnectionPool.getConnection()) {
            Statement tx_stmt = conn.createStatement();
            tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
//            "INSERT INTO lease_table (owner_id, expire_timestamp) VALUES (%d, CURRENT_TIMESTAMP + interval '2 seconds')" +
//                    " ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
//                    " SET owner_id = %d, expire_timestamp = CURRENT_TIMESTAMP + interval '2 seconds'" +
//                    " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP"
            int leaseLength = 2;
            PreparedStatement stmt = conn.prepareStatement(String.format(
                    "INSERT INTO lease_table (owner_id, expire_timestamp)" +
                    " VALUES (?, CURRENT_TIMESTAMP + interval '%d seconds') ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
                    " SET owner_id = ?, expire_timestamp = CURRENT_TIMESTAMP + interval '%d seconds'" +
                    " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP", leaseLength, leaseLength));
            stmt.setLong(1, managerId);
            stmt.setLong(2, managerId);
            int rowCount = stmt.executeUpdate();
            tx_stmt.execute("COMMIT");
            return rowCount > 0;
//            if (stmt.executeUpdate() < 1) {
//                return false;
//            } else {
//                return true;
//            }
        } catch (SQLException e) {
            // TODO: Ignore exception and properly handle failed lease claim
            System.out.println(String.format("ERROR - SQLState [SQLState=%s]",
                    e.getSQLState()));
            e.printStackTrace(System.out);
            return false;
//            throw new RuntimeException(e);
        }
    }

    public boolean testClaimSoftLease(Connection conn) throws SQLException {
        Statement tx_stmt = conn.createStatement();
        tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
//            "INSERT INTO lease_table (owner_id, expire_timestamp) VALUES (%d, CURRENT_TIMESTAMP + interval '2 seconds')" +
//                    " ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
//                    " SET owner_id = %d, expire_timestamp = CURRENT_TIMESTAMP + interval '2 seconds'" +
//                    " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP"
        int leaseLength = 2;
        PreparedStatement stmt = conn.prepareStatement(String.format(
                "INSERT INTO lease_table (owner_id, expire_timestamp)" +
                        " VALUES (?, CURRENT_TIMESTAMP + interval '%d seconds') ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
                        " SET owner_id = ?, expire_timestamp = CURRENT_TIMESTAMP + interval '%d seconds'" +
                        " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP", leaseLength, leaseLength));
        stmt.setLong(1, managerId);
        stmt.setLong(2, managerId);
        int rowCount = stmt.executeUpdate();
        tx_stmt.execute("COMMIT");
        return rowCount > 0;
//            if (stmt.executeUpdate() < 1) {
//                return false;
//            } else {
//                return true;
//            }
    }

    // TODO: Implement Retry
    public boolean claimSoftLeaseRetry() {
        int attemptCount = 0;
        int maxRetry = 5;
        // TODO: Bakery algorithm
        while (attemptCount < maxRetry) {
            try (Connection conn = ConnectionPool.getConnection()) {
                Statement tx_stmt = conn.createStatement();
                tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                PreparedStatement stmt = conn.prepareStatement("INSERT INTO lease_table (owner_id, expire_timestamp)" +
                        " VALUES (?, CURRENT_TIMESTAMP + interval '2 seconds') ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
                        " SET owner_id = ?, expire_timestamp = CURRENT_TIMESTAMP + interval '2 seconds'" +
                        " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP");
                stmt.setLong(1, managerId);
                stmt.setLong(2, managerId);
                int rowCount = stmt.executeUpdate();
                tx_stmt.execute("COMMIT");
                if (rowCount > 0) {
                    return true;
                }
                // TODO: Bakery algorithm
                Thread.sleep(250);
            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            attemptCount++;
        }
        return false;
    }

    public void releaseSoftLeaseOutdated() {
        try (Connection conn = ConnectionPool.getConnection()) {
            Statement tx_stmt = conn.createStatement();
            tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
//            "INSERT INTO lease_table (owner_id, expire_timestamp) VALUES (%d, CURRENT_TIMESTAMP + interval '2 seconds')" +
//                    " ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE" +
//                    " SET owner_id = %d, expire_timestamp = CURRENT_TIMESTAMP + interval '2 seconds'" +
//                    " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP"
            PreparedStatement stmt = conn.prepareStatement("DELETE FROM lease_table WHERE owner_id = ?");
            stmt.setLong(1, managerId);
            // TODO: Consider if lease release should give any information
            stmt.executeUpdate();
            tx_stmt.execute("COMMIT");
//            if (stmt.executeUpdate() < 1) {
//                return false;
//            } else {
//                return true;
//            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void releaseLease() {
    }
    public boolean claimLease() throws IOException {
        // TODO: Currently redesigned. Check ARCHIVE for past versions
        return true;
    }

    public CommandOutput create(CreateCommand cmd) {
        // TODO: output class refactor/clean up
        CommandOutput result = new CommandOutput();

        // TODO: set these all as part of transaction or at least as part of batch
//        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
        try (Connection conn = ConnectionPool.getConnection()) {
            String filepath = cmd.getArgIndex(0);
            if (DebugConstants.DEBUG_FLAG) {
                System.out.println(String.format("REMOVE ME [table=log, filepath=%s].", filepath));
            }
            String parentPath = Paths.get(filepath).getParent().toString();

            String sqlStr = "INSERT INTO log(path,record_type,data) "
                    + "VALUES(?,?,?)";
            PreparedStatement statement = conn.prepareStatement(sqlStr);
            statement.setString(1, filepath);
            statement.setInt(2, CREATE.ordinal());
            statement.setBytes(3, cmd.getArgIndex(1).getBytes(StandardCharsets.UTF_8));
            statement.execute();
            if (DebugConstants.DEBUG_FLAG) {
                System.out.println(String.format("Create Operation Success [table=log, path=%s].", filepath));
            }

            // TODO: figure out best way to get the value from the row that was inserted
            sqlStr = "INSERT INTO path_info(path) VALUES(?)";
            statement = conn.prepareStatement(sqlStr);
            statement.setString(1, filepath);
            statement.execute();
            System.out.println(String.format("Create Operation Success [table=path_info, path=%s].", filepath));


            sqlStr = "INSERT INTO path_children(path, child_path) VALUES(?,?)";
            statement = conn.prepareStatement(sqlStr);
            statement.setString(1, parentPath);
            statement.setString(2, filepath);
            statement.execute();
            System.out.println(String.format("Create Operation Success [table=path_children, path=%s].", filepath));
        } catch (SQLException e) {
            System.out.println(String.format("Create Operation Failed."));
            System.out.println(String.format(e.getMessage()));
            result.populateError(1, "Create Operation Failed.");
            return result;
        }
        result.setOutput("Create Operation Succeeded.");
        return result;
    }

    public CommandOutput delete(DeleteCommand cmd) {
        CommandOutput result = new CommandOutput();

        // NOTE: Claim lease
        boolean leaseFlag = false;
        long logTimestamp = 0L;
//        try {
            while (!leaseFlag) {
                // TODO: implement waiting to retry
                leaseFlag = claimSoftLease();
            }
            logTimestamp = getLogTimestamp();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

        //TODO: proper error handling
        //        if(tokens.length < 2) return "Delete Operation Failed
        //        - Insufficient param count.";
        String output = "";

        String filepath = cmd.getArgIndex(0);
        String parentPath = Paths.get(filepath).getParent().toString();
        String filename = Paths.get(filepath).getFileName().toString();

        // NOTE: Validations
        if (!pathChildrenMap.containsKey(filepath)) {
            output = "Delete Operation Failed - ZNode path not found.";
            result.populateError(1, output);
            return result;
        } else if (!pathChildrenMap.get(filepath).isEmpty()) {
            output = "Delete Operation Failed - Node not empty.";
            result.populateError(1, output);
            return result;
        }

        LogRecord logRecord = new LogRecord(filepath, new byte[0], LogRecord.LogRecordType.DELETE);
//        return logRecord;
        if (validateLog(logTimestamp)) {
            byte[] recordBytes = SerializationUtils.serialize(logRecord);
            try (FileOutputStream outputStream = new FileOutputStream(logPath.toFile())) {
                LogRecordMetadata metadata = new LogRecordMetadata(Files.size(logPath), recordBytes.length);
                outputStream.write(recordBytes);

                pathChildrenMap.remove(filepath);
                pathMetadataMap.remove(filepath);
                pathChildrenMap.get(parentPath).remove(filename);
                if (watchManager.containsKey(parentPath)) {
                    System.out.println(String.format("Watch triggered - membership change [members = %s]",
                            pathChildrenMap.get(parentPath).toString()));
                }
                output = "Delete Operation Succeeded.";
                result.setErrorMsg(output);
                return result;
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
                output = "Delete Operation Failed.";
                result.populateError(1, output);
                return result;
            }
        } else {
            result.populateError(1, "Delete Operation Failed - Invalid log timestamp");
            return result;
        }

    }

    public CommandOutput get(GetCommand cmd) {
        CommandOutput result = new CommandOutput();

        // TODO: set these all as part of transaction or at least as part of batch
        try (Connection conn = ConnectionPool.getConnection()) {
//        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
            String filepath = cmd.getArgIndex(0);
//            String parentPath = Paths.get(filepath).getParent().toString();

            String sqlStr = "select row,data from log where path=? order by row desc limit 1";
            PreparedStatement statement = conn.prepareStatement(sqlStr);
            statement.setString(1, filepath);
//            statement.setInt(2, LogRecord.LogRecordType.CREATE.ordinal());
//            statement.setBytes(3, cmd.getArgIndex(2).getBytes(StandardCharsets.UTF_8));
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                String output = new String(rs.getBytes("data"), StandardCharsets.UTF_8);
                System.out.println(String.format("Get Operation Success [path=%s, output=%s].", filepath, output));
                result.setOutput(String.format("Get Operation Success [path=%s, output=%s].", filepath, output));
            } else {
                System.out.println(String.format("Get Operation Empty Set [path=%s, output=''].", filepath));
                result.setOutput(String.format("Get Operation Empty Set [path=%s, output=''].", filepath));
            }
            return result;
        } catch (SQLException e) {
            System.out.println(String.format("Get Operation Failed."));
            System.out.println(String.format(e.getMessage()));
            result.populateError(1, "Get Operation Failed.");
            return result;
        }
    }

    private long getLogTimestamp() {
        // TODO: redesign this if log table is allowed to be truncated. Currently only tracks count of log table.
        try (Connection conn = ConnectionPool.getConnection()) {
//        try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
            Statement tx_stmt = conn.createStatement();
//            PreparedStatement stmt = conn.prepareStatement("INSERT INTO log_record_table (row, path, record_type, data) VALUES (?, ?, ?, ?)");
            PreparedStatement stmt = conn.prepareStatement("SELECT COUNT(*) FROM log_record_table");
            ResultSet rs = stmt.executeQuery();
            int counter = 0;
            if (rs.next()) {
                counter = rs.getInt(1);
//                System.out.println(String.format("DEBUG - getlogtimestamp success [logCount=%d].", counter));
            } else {
                // TODO: remove DEBUG
                System.out.println(String.format("DEBUG - getlogtimestamp failure."));
            }
            return counter;
        } catch (SQLException e) {
            System.out.println(String.format("Error - getLogTimestamp - Failed to connect."));
            throw new RuntimeException(e);
        }
    }

    public boolean validateEntry() {
        return false;
    }

    static class DBLeaseClaimer implements Callable<Boolean> {

        DataLayerMgrAmazonDB_LeaseTest manager;

        public DBLeaseClaimer(DataLayerMgrAmazonDB_LeaseTest manager) {
            this.manager = manager;
        }

        public Boolean call() {
            // TODO: Consider refactoring to be cleaner
            // NOTE: Try to claim soft lease
            boolean leaseFlag = manager.claimSoftLease();
            if (leaseFlag) {
                return leaseFlag;
            }

            int queueCount = 100;
            int leaseLength = 6;
            int sleepMS;
            int ticket;
            try (Connection conn = ConnectionPool.getConnection()) {
                ticket = manager.addLeaseWaitlist(conn);
            } catch (SQLException e) {
                System.out.println(String.format("DBLeaseClaimer - Exception [SQLState=%s]",
                        e.getSQLState()));
                System.out.println(String.format("===\nSTACKTRACE\n==="));
                e.printStackTrace(System.out);
                return false;
            }

            while (!Thread.currentThread().isInterrupted() && !leaseFlag) {
                try (Connection conn = ConnectionPool.getConnection()) {
                    PreparedStatement stmt;
                    ResultSet rs;
                    Statement tx_stmt = conn.createStatement();
                    tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");

                    if (queueCount != 0) {
                        stmt = conn.prepareStatement("SELECT COUNT(*) FROM lease_waitlist_table" +
                                " WHERE ticket < ? AND expire_timestamp < CURRENT_TIMESTAMP");
                        stmt.setInt(1, ticket);
                        rs = stmt.executeQuery();
                        if (rs.next()) {
                            queueCount = rs.getInt(1);
                        }
//                        } else {
//                            // TODO: Consider error-out
//                            // NOTE: No-op
//                        }
                    }

                    if (queueCount == 0) {
                        // TODO: Attempt to claim lease
                        stmt = conn.prepareStatement(String.format(
                                "INSERT INTO lease_table (owner_id, expire_timestamp)" +
                                " VALUES (?, CURRENT_TIMESTAMP + interval '%d milliseconds')" +
                                " ON CONFLICT ON CONSTRAINT pk_lt DO UPDATE SET owner_id = ?," +
                                " expire_timestamp = CURRENT_TIMESTAMP + interval '%d milliseconds'" +
                                " WHERE lease_table.expire_timestamp < CURRENT_TIMESTAMP", leaseLength, leaseLength));
                        stmt.setLong(1, manager.managerId);
                        stmt.setLong(2, manager.managerId);
                        leaseFlag = stmt.executeUpdate() > 0;
                        conn.commit();
                        if (leaseFlag) {
                            return leaseFlag;
                        }
                    }

                    if (queueCount < 3) {
                        sleepMS = 0;
                    } else {
                        sleepMS = Math.min(queueCount*4, 24);
                    }

                    // TODO: Create class variable
//        int leaseLength = 4;
                    stmt = conn.prepareStatement(String.format(
                            "UPDATE lease_waitlist_table" +
                            " SET expire_timestamp = CURRENT_TIMESTAMP + interval '%d milliseconds'" +
                            " WHERE ticket = ? AND owner_id = ?", sleepMS+6));
                    stmt.setInt(1, ticket);
                    stmt.setLong(2, manager.managerId);
                    if (stmt.executeUpdate() < 1) {
                        // TODO: Throw CRITICAL ERROR as ticket or owner_id are missing
                        System.out.println(String.format("CRITICAL ERROR - DBLeaseClaimer - Lease Waitlist Lost"));
                        return false;
                    }
                    conn.commit();
                } catch (SQLException e) {
                    if (e.getSQLState().equalsIgnoreCase(SERIALIZATION_FAILURE.getState())) {
                        if (DebugConstants.DEBUG_FLAG) {
                            System.out.println(String.format("DEBUG - DBLeaseClaimer - Expected exception [SQLState=%s]",
                                    e.getSQLState()));
                        }
                    } else {
                        System.out.println(String.format("DBLeaseClaimer - Exception [SQLState=%s]",
                                e.getSQLState()));
                        System.out.println(String.format("===\nSTACKTRACE\n==="));
                        e.printStackTrace(System.out);
                    }
                    System.out.println(String.format("===\nDBLeaseClaimer CONTINUING\n==="));
                    // NOTE: No sleep on error
                    sleepMS = 0;
                }
                if (sleepMS > 0) {
                    try {
                        Thread.sleep(sleepMS);
                    } catch (InterruptedException e) {
                        // TODO: Log background thread error
                        if (DebugConstants.DEBUG_FLAG) {
                            System.out.println(String.format("CRITICAL ERROR - DBLeaseClaimer - Interrupted"));
                            e.printStackTrace(System.out);
                        }
                        throw new RuntimeException(e);
                    }
                }
            }
            return leaseFlag;
        }
    }

    static class DBLeaseHandler extends ManagerLeaseHandlerBase {

        private ExecutorService leaseExtender;
        private DataLayerMgrAmazonDB_LeaseTest manager;

        public DBLeaseHandler(DataLayerMgrAmazonDB_LeaseTest manager) {
            this.manager = manager;
            leaseExtender = Executors.newSingleThreadExecutor();
            LeaseHandlerRunnable leaseRunnable = new LeaseHandlerRunnable(manager);
            leaseExtender.submit(leaseRunnable);
        }

        @Override
        public void close() {
            // TODO: Properly handle errors and logging
            if (DebugConstants.DEBUG_FLAG) {
                System.out.println(String.format("REMOVE ME - DEBUG - DBLeaseHandler - Closing"));
            }
            leaseExtender.shutdownNow();
            manager.releaseSoftLease();
        }
    }

    // TODO: figure out how to properly couple the data stores
    @Slf4j
    static class LogReader implements Runnable {
//        String threadId;
//        String rootPath;
//        String hostname;
//        int port;
        DataLayerMgrAmazonDB_LeaseTest manager;

        boolean dummyTestingFlag = false;
        int heartbeatCounter = 0;
        final int heartbeatThreshold = 200;

        long minExecutionTIme = Long.MAX_VALUE;
        long maxExecutionTime = 0;
        double averageExecutionTime = 0;
        long cumulativeExecutionTime = 0;

        // TODO: Clean up metric variables
        long startExecutionTime = 0;
        long midExecutionTime = 0;
        long endExecutionTime = 0;

        //        Thread t;
        LogReader(DataLayerMgrAmazonDB_LeaseTest manager) {
            this.manager = manager;
//            t = new Thread(this, threadId);
            System.out.println(String.format("New LogReader thread"));
//            t.start();
        }

        @Override
        public void run() {
            long currentExecutionTime;
//            StopWatch watch = new StopWatch();
//            StopWatch resetWatch = new StopWatch();
//            watch.start();
//            resetWatch.start();
            if (heartbeatCounter >= heartbeatThreshold) {
                log.info(String.format("Heartbeat - LogReader Alive [heartbeat_threshold=%d]",
                                heartbeatThreshold));
//                log.info(String.format("Heartbeat - LogReader Metrics [average_execution_time=%f," +
//                                        " max_execution_time=%d, min_execution_time=%d]",
//                                (double) cumulativeExecutionTime / heartbeatThreshold,
//                                maxExecutionTime, minExecutionTIme));
//                log.info(String.format("Heartbeat - LogReader Metrics [max_start_execution_time=%d," +
//                                        " max_mid_execution_time=%d, max_end_execution_time=%d]",
//                                startExecutionTime, midExecutionTime, endExecutionTime));
//                System.out.println(
//                        String.format("Heartbeat - LogReader Alive [heartbeat_threshold=%d]",
//                                heartbeatThreshold));
//                System.out.println(
//                        String.format("Heartbeat - LogReader Metrics [average_execution_time=%f," +
//                                        " max_execution_time=%d, min_execution_time=%d]",
//                                        (double) cumulativeExecutionTime / heartbeatThreshold,
//                                        maxExecutionTime, minExecutionTIme));
//                System.out.println(
//                        String.format("Heartbeat - LogReader Metrics [max_start_execution_time=%d," +
//                                        " max_mid_execution_time=%d, max_end_execution_time=%d]",
//                                        startExecutionTime, midExecutionTime, endExecutionTime));
                startExecutionTime = 0;
                midExecutionTime = 0;
                endExecutionTime = 0;
                maxExecutionTime = 0;
                minExecutionTIme = Long.MAX_VALUE;
                cumulativeExecutionTime = 0;
                heartbeatCounter = 0;
            }

//            resetWatch.stop();
//            startExecutionTime = Math.max(startExecutionTime, resetWatch.getTime(TimeUnit.MILLISECONDS));
//            resetWatch.reset();
//            resetWatch.start();

            // TODO: consider impact of triggered watches being outside or inside the connection and which is better
            // TODO: consider this being a map of watched event and relevant output
//            ArrayList<WatchedEvent> triggeredWatches = new ArrayList<>();
//            ArrayList<CommandOutput> watchInputs = new ArrayList<>();
            HashMap<WatchedEvent, WatchInput> watchInputMap = new HashMap<>();
            HashSet<Long> removedOwners = new HashSet<>();
//            try (Connection conn = ConnectionPool.getTestConnection()) {
            try (Connection conn = ConnectionPool.getConnection()) {
                Statement tx_stmt = conn.createStatement();
                // TODO: Consider rollback to unblock background reader
                // TODO: Consider only rollback if in failed transaction; IN_FAILED_SQL_TRANSACTION("25P02")
//                tx_stmt.execute("ROLLBACK");
//                resetWatch.stop();
//                midExecutionTime = Math.max(midExecutionTime, resetWatch.getTime(TimeUnit.MILLISECONDS));
//                resetWatch.reset();
//                resetWatch.start();

                // TODO: REMOVE CONDITIONAL WATCH TRIGGERING FOR TESTING
//                if (!dummyTestingFlag && Math.random() <= 0.2) {
//                    System.out.println("REMOVE ME - DEBUG - LogReader Conditional Watch Trigger");
//                    if (conn.createStatement().executeUpdate(String.format(
//                            "UPDATE watch_table SET" +
//                                    " trigger_transaction_id = %d," +
//                                    " triggered_flag = TRUE" +
//                                    " WHERE owner_id = %d", manager.logCounter, manager.managerId)) > 0) {
//                        dummyTestingFlag = true;
//                    }
////                    PreparedStatement dummyStmt = conn.prepareStatement("UPDATE watch_table SET" +
////                            " trigger_transaction_id = ?," +
////                            " triggered_flag = TRUE" +
////                            " WHERE owner_id = ?");
////                    dummyStmt.setInt(1, manager.logCounter);
////                    dummyStmt.setLong(2, manager.managerId);
////                    dummyStmt.executeUpdate();
//                }

                // TODO: Consider moving this out into a wrapper/helper function
                // TODO: watch table needs the triggering row id added to it, and then relevant manager
                //  uses it to fetch relevant output

                // TODO: Update ephemeral owner table liveness timestamp
                // TODO: This is causing issues with commits
//                try {
//                    // TODO:
//                    tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
////                    tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
//                    conn.createStatement()
//                            .executeUpdate(String.format(
//                                    "UPDATE ephemeral_owner_table SET touch_timestamp = CURRENT_TIMESTAMP" +
//                                    " WHERE owner_id = %d",
//                                    manager.managerId));
//                    conn.commit();
////                    tx_stmt.execute("COMMIT");
//                } catch (SQLException e) {
//                    conn.rollback();
////                    conn.createStatement().execute("ROLLBACK");
//                }
//
//                tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                try {
                    conn.createStatement().execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                } catch (SQLException e) {
                    conn.rollback();
                    conn.createStatement().execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                }

                // NOTE: Check Watch Table
                ResultSet rsWatches;
                boolean updateReturnFlag;
                boolean consumeWatchesFlag = false;
                PreparedStatement stmt;
                // TODO: Consider dynamics between different locks
                synchronized (manager.watchStructMonitor) {
                    // TODO: Consider best practices on which conn.<stmt> fn to use
//                    rsWatches = conn.prepareStatement(String.format(
//                            "SELECT * FROM watch_table" +
//                                " WHERE owner_id = %d AND triggered_flag = TRUE", manager.managerId)).executeQuery();
                    // NOTE: If table not locked, a triggered watch can be missed
                    stmt = conn.prepareStatement(String.format(
                            "UPDATE watch_table SET triggered_flag = FALSE" +
                            " WHERE owner_id = %d AND triggered_flag = TRUE RETURNING *",
                            manager.managerId));
                    // NOTE: True if the first result is a ResultSet object; false if the first result
                    //   is an update count or there is no result
                    updateReturnFlag = stmt.execute();
                }

                if (!updateReturnFlag) {
                    // NOTE: Consume result count
//                    int resultCount = stmt.getUpdateCount();
                    stmt.getUpdateCount();
//                    System.out.println(String.format(
//                            "REMOVE ME - DEBUG - LogReader - WATCH SQL [updateReturnFlag=%b, count=%d]",
//                            updateReturnFlag, resultCount));
                    if (!stmt.getMoreResults()) {
                        // NOTE: Failed update
                        System.out.println(String.format("REMOVE ME - DEBUG - LogReader Watch failed [managerId=%d]",
                                manager.managerId));
                        tx_stmt.execute("ROLLBACK");
                        return;
                    }
                }

                // TODO: Consider where tx begin/commit should be placed
                tx_stmt.execute("COMMIT");
////                if (!updateReturnFlag && stmt.getUpdateCount() > 0) {
////                    System.out.println(String.format("REMOVE ME - DEBUG - LogReader get watch result"));
////                    stmt.getMoreResults();
                rsWatches = stmt.getResultSet();
                if (!Objects.isNull(rsWatches)) {
                    consumeWatchesFlag = rsWatches.next();
                }
//                }

                while (consumeWatchesFlag) {
//                while (rsWatches.next()) {
                    String path = rsWatches.getString("path");
                    int watchType = rsWatches.getInt("watch_type");
                    int triggerTxId = rsWatches.getInt("trigger_transaction_id");
//                    boolean permanentFlag = rsWatches.getBoolean("permanent_watch_flag");
//                    int txId = rsWatches.getInt("transaction_id");
                    LogRecord.LogRecordType triggerType = LogRecord.LogRecordType.fromInteger(
                            rsWatches.getInt("trigger_transaction_type"));
                    int recordVersion = rsWatches.getInt("trigger_record_version");

                    // TODO: Process watch
                    System.out.println(String.format("REMOVE ME - DEBUG - LogReader watch" +
                                    " [path=%s, type=%s, triggerTxId=%d]",
                                    path, watchType, triggerTxId));
//                    if (Math.random() <= 0.2) {
//                        for (String path : manager.pathWatchMap.keySet()) {
                            // TODO: need to be able to retrieve commandoutput.
                            //  Need row of triggering logrecord
                    // TODO: Consider if querying triggering.
                    //  children watch: child path; child data
                    //  get watch: change/create path; new data; version
                    //  exist: path; can be delete
                    WatchInput watchInput = WatchInput.builder()
                            .path(path).recordVersion(recordVersion)
                            .triggerRecordType(triggerType)
                            .triggerTransactionId(triggerTxId).build();
                    HashSet<WatchedEvent> watches = manager.pathWatchMap.get(
                            WatchedEvent.WatchType.fromInteger(watchType)).get(path);
                    HashSet<WatchedEvent> removedWatches = new HashSet<>();
                    // TODO: Track watch removal
                    System.out.println(String.format("REMOVE ME - DEBUG - Watches Set" +
                                    " [watches_size=%d]", watches.size()));

//                    WatchedEvent registeredWatch = manager.pathWatchMap.get(path);
//                    if (!Objects.isNull(registeredWatch)) {
//                    if (!Objects.isNull(watches)) {
                    boolean permanentFlag;
//                        registeredWatch.triggerWatch(dummyOutput);
                    for(WatchedEvent registeredWatch : watches) {
                        if (registeredWatch.getTriggerTransactionThreshold() > triggerTxId) {
                            // NOTE: Skip watch if below tx id threshold
                            System.out.println(String.format("REMOVE ME - DEBUG - Watch Skipped" +
                                                " [path=%s, txThreshold=%d]", registeredWatch.getPath(),
                                                registeredWatch.getTriggerTransactionThreshold()));
                            continue;
                        }
//                        triggeredWatches.add(registeredWatch);
                        // TODO: consider better options that more strongly couple watch and relevant input (ordered map?)
//                        watchInputs.add(dummyOutput);
                        watchInputMap.put(registeredWatch, watchInput);
                        permanentFlag = registeredWatch.getWatchPersistentFlag();
                        if (!permanentFlag) {
                            removedWatches.add(registeredWatch);
//                                manager.pathWatchMap.remove(path);
//                                conn.createStatement().executeUpdate(String.format("DELETE FROM watch_table" +
//                                                " WHERE path = '%s' AND owner_id = %d AND watch_type = %d",
//                                        path, manager.managerId, watchType));
//                                conn.createStatement().executeUpdate(String.format("DELETE FROM watch_table" +
//                                                " WHERE path = '%s' AND owner_id = %d AND transaction_id = %d",
//                                        path, manager.managerId, txId));
                        }
                    }
//                    // TODO: test watch removal
                    watches.removeAll(removedWatches);
                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println(String.format("REMOVE ME - DEBUG - Watches Set POST REMOVE" +
                                        " [watches_size=%d]",
                                manager.pathWatchMap.get(
                                            WatchedEvent.WatchType.fromInteger(watchType)).get(path).size()));
                    }

////                    } else {
////                        // TODO: ERROR, WATCH NOT REGISTERED PROCESS
////                        System.out.println(String.format("ERROR - Watch not registered in manager [path=%s]", path));
////
////                    }
////                        }
////                    }
                    consumeWatchesFlag = rsWatches.next();
                }
                // TODO: If watch for this manager triggered, trigger callable
                // NOTE: temporary randomizer
//                if (Math.random() <= 0.2) {
//                    for (String path : manager.pathWatchMap.keySet()) {
//                        // TODO: need to be able to retrieve commandoutput.
//                        //  Need row of triggering logrecord
//                        CommandOutput dummyOutput = new CommandOutput();
//                        dummyOutput.setOutput("Watch triggered! Dummy output!");
//                        manager.pathWatchMap.get(path).triggerWatch(dummyOutput);
//                    }
//                }

                // TODO: Consider submitting watch triggers individually
                // TODO: Consider what happens if watch never triggered?
                // TODO: When should watches be triggered? Should be before
                //  log consume to be analogous with ZK
                for (Map.Entry<WatchedEvent, WatchInput> watchInputEntry : watchInputMap.entrySet()) {
                    if (manager.watchExecutor.isShutdown() || manager.watchExecutor.isTerminated() ) {
                        System.out.println(String.format(
                                "CRITICAL ERROR - LogReader - Manager Watch Executor Not Running"));
                    }
                    manager.watchExecutor.submit(()
                            -> watchInputEntry.getKey().triggerWatch(manager, watchInputEntry.getValue()));
                }
//                resetWatch.stop();
//                endExecutionTime = Math.max(endExecutionTime, resetWatch.getTime(TimeUnit.MILLISECONDS));

//                try {
//                    conn.createStatement().execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
//                } catch (SQLException e) {
//                    conn.rollback();
//                    conn.createStatement().execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
//                }

                ResultSet rs = conn.prepareStatement(String.format(
                                    "SELECT * FROM log_record_table" +
                                    " WHERE row > %d ORDER BY row asc", manager.logCounter.get()))
                                .executeQuery();
//                int counter = 0;
//                HashMap<String, Integer> consumedRecords = new HashMap<>();
                while (rs.next()) {
                    // TODO: properly consume log. handle DELETE later
//                    counter += 1;
                    Path path = Paths.get(rs.getString("path"));
                    LogRecord.LogRecordType type = LogRecord.LogRecordType.fromInteger(
                            rs.getInt("record_type"));
                    long managerId = rs.getLong("manager_id");
                    long timestamp = rs.getTimestamp("record_timestamp").getTime();
                    int txId = rs.getInt("row");
                    byte[] data = rs.getBytes("data");

                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println(String.format("REMOVE ME - DEBUG - LogReader consume [path=%s, type=%s]",
                                path.toString(), type.toString()));
                    }

                    manager.updateMetadataStore(new LogRecord(path.toString(), new byte[0],
                            type, managerId), timestamp, txId);
                    // TODO: Consider moving cache managing to base abstract class
                    if (manager.cacheFlag) {
                        switch (Objects.requireNonNull(type)) {
                            case SET:
                            case CREATE_EPHEMERAL:
                            case CREATE:
                                manager.recordDataCache.put(path.toString(), new LogRecord(path.toString(), data,
                                        type, managerId));
                                break;
                            case DELETE:
                                manager.recordDataCache.remove(path.toString());
                                break;
                            case CONNECT:
                                break;
                            default:
                        }
                    }
                    // TODO: Make logcounter atomic integer
                    synchronized (manager.outstandingChangesMonitor) {
                        manager.logCounter.set(txId);
                        manager.removeFromOutstandingRecords(txId, path.toString());
                    }
                }
//            } catch (SQLException | InterruptedException e) {
            } catch (SQLException e) {
                // TODO: retry a few times
                // TODO: fix error message
                if (e.getSQLState().equalsIgnoreCase(SERIALIZATION_FAILURE.getState())) {
                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println(String.format("DEBUG - LOGREADER - Expected exception [SQLState=%s]",
                                e.getSQLState()));
                    }
                } else {
                    System.out.println(String.format("LOGREADER - Exception [SQLState=%s]",
                            e.getSQLState()));
                    System.out.println(String.format("===\nSTACKTRACE\n==="));
                    e.printStackTrace(System.out);
                }
                System.out.println(String.format("===\nLOG READER CONTINUING\n==="));
            } finally {
                heartbeatCounter++;
            }
        }
    }

    @Slf4j
    static class LeaseHandlerRunnable implements Runnable {
        DataLayerMgrAmazonDB_LeaseTest manager;
//        boolean testFlag;

//        int heartbeatCounter = 0;
//        final int heartbeatThreshold = 100;
        LeaseHandlerRunnable(DataLayerMgrAmazonDB_LeaseTest manager) {
            this.manager = manager;
//            testFlag = true;
            if (DebugConstants.DEBUG_FLAG) {
                System.out.println(String.format("New LeaseHandlerRunnable thread"));
            }
        }

        @Override
        public void run() {
            try (Connection conn = ConnectionPool.getConnection()) {
                while (manager.isRunning() && !Thread.currentThread().isInterrupted()) {
                    // NOTE: No-wait loop is preferable as schedulers are not reliable at least duration scale
                    try {
                        manager.extendSoftLease(conn);
                    } catch (InvalidServiceStateException e) {
                        log.warn("LeaseHandler - Failure in background thread", e);
                    }
                }
            } catch (SQLException e) {
                System.out.println(String.format("CRITICAL ERROR - LeaseHandler - Failed to extend lease"));
                throw new RuntimeException(e);
            }
        }
    }

    @Slf4j
    static class EphemeralManager implements Runnable {
        DataLayerMgrAmazonDB_LeaseTest manager;

        int heartbeatCounter = 0;
        final int heartbeatThreshold = 200;
        EphemeralManager(DataLayerMgrAmazonDB_LeaseTest manager) {
            this.manager = manager;
            System.out.println(String.format("New EphemeralManager thread"));
        }

        @Override
        public void run() {
            if (heartbeatCounter >= heartbeatThreshold) {
                log.info(String.format("Heartbeat - EphemeralManager Alive [heartbeat_threshold=%d]",
                        heartbeatThreshold));
                heartbeatCounter = 0;
            }

            // TODO: consider impact of triggered watches being outside or inside the connection and which is better
            // TODO: consider this being a map of watched event and relevant output
            HashSet<Long> removedOwners = new HashSet<>();
            try (Connection conn = ConnectionPool.getConnection()) {
                Statement tx_stmt = conn.createStatement();

                try {
                    tx_stmt.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED");
                    conn.createStatement()
                            .executeUpdate(String.format(
                                    "UPDATE ephemeral_owner_table SET touch_timestamp = CURRENT_TIMESTAMP" +
                                            " WHERE owner_id = %d",
                                    manager.managerId));
                    conn.commit();
                } catch (SQLException e) {
                    conn.rollback();
                }

                try {
                    conn.createStatement().execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                } catch (SQLException e) {
                    conn.rollback();
                    conn.createStatement().execute("BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE");
                }

                boolean ephemeralLease = false;
                int retryCount = 0;
                // NOTE: Testing to see if I can get it to step triggering itself
//                int retryMax = manager.ephemeralLeaseAtomic.get() ? 0 : 1;
                int retryMax = 1;
                while (manager.isInitialized && !ephemeralLease && retryCount < retryMax) {
                    retryCount++;
                    ephemeralLease = manager.claimEphemeralLease();
//                    if (!ephemeralLease && retryCount < retryMax) {
//                        Thread.sleep(1+(int)(Math.random()*13));
//                    }
                }
                if (ephemeralLease) {
//                    System.out.println(String.format("REMOVEME - DEBUG - LogReader Ephemeral [heartbeatCounter=%d, heartbeat_threshold=%d]",
//                            heartbeatCounter, heartbeatThreshold));
                    Instant nowTs = Instant.now();
                    Instant touchTs;
                    Duration livenessDelta;
//                System.out.println(String.format("REMOVE ME - DEBUG - LogReader Ephemeral Check"));
                    // TODO: Add liveness check into the query
//                rs = conn.prepareStatement(String.format("SELECT * FROM ephemeral_owner_table")).executeQuery();
                    ResultSet rs = conn.prepareStatement(String.format("SELECT * FROM ephemeral_owner_table" +
                                    " WHERE touch_timestamp < CURRENT_TIMESTAMP - interval '%d seconds'",
                            manager.ephemeralLivenessThreshold.getSeconds())).executeQuery();
                    while (rs.next()) {
                        // TODO: properly consume log. handle DELETE later
                        touchTs = rs.getTimestamp("touch_timestamp").toInstant();
                        long ownerId = rs.getLong("owner_id");
                        livenessDelta = Duration.between(touchTs, nowTs);
//                    System.out.println(
//                            String.format("LogReader - Ephemeral Check [ownerId=%d, livenessDelta=%s]",
//                                    ownerId, livenessDelta.toString()));
                        // TODO: can add the ephemeral check into the query (and this gets around desync clocks)
                        if (livenessDelta.compareTo(manager.ephemeralLivenessThreshold) >= 0) {
                            System.out.println(
                                    String.format("LogReader - Ephemeral Liveness Threshold Crossed" +
                                                    " [ownerId=%d, livenessDelta=%s]",
                                            ownerId, livenessDelta.toString()));
                            // TODO: delete ephemeral nodes
                            removedOwners.add(ownerId);
                        }
                    }
                    if (removedOwners.isEmpty()) {
                        manager.releaseEphemeralLease();
                    }
//                tx_stmt.execute("COMMIT");
                }
//            } catch (SQLException | InterruptedException e) {
            } catch (SQLException e) {
                // TODO: retry a few times
                // TODO: fix error message
                if (e.getSQLState().equalsIgnoreCase(SERIALIZATION_FAILURE.getState())) {
                    if (DebugConstants.DEBUG_FLAG) {
                        System.out.println(String.format("DEBUG - LOGREADER - Expected exception [SQLState=%s]",
                                e.getSQLState()));
                    }
                } else {
                    System.out.println(String.format("LOGREADER - Exception [SQLState=%s]",
                            e.getSQLState()));
                    System.out.println(String.format("===\nSTACKTRACE\n==="));
                    e.printStackTrace(System.out);
                }
                System.out.println(String.format("===\nLOG READER CONTINUING\n==="));
                return;
            } finally {
                heartbeatCounter++;
            }

            // TODO: Delete ephemerals; Consider helper function. UseWatchexecutor?
            //   select from ephemeral_record_table based on owner_id
            //   generatedeletecmd; execute;
            if (!removedOwners.isEmpty()) {
                manager.tbdDeleteEphemeralHelperFn(removedOwners);
            }
        }
    }
}
