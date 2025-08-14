package dsg.ccsvc.archive;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.caching.CSvcSimpleLRUCache;
import dsg.ccsvc.client.CSvcSandboxParser;
import dsg.ccsvc.command.*;
import dsg.ccsvc.datalayer.*;
import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.util.ConnectionPool;
import dsg.ccsvc.util.DebugConstants;
import dsg.ccsvc.util.TestBase;
import dsg.ccsvc.util.TestImpl;
import dsg.ccsvc.watch.WatchedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static dsg.ccsvc.util.DebugConstants.DEBUG_FLAG;
import static dsg.ccsvc.util.DebugConstants.TESTS_DEBUG_FLAG;

@Slf4j
public class OldManualTests {

    private static Options options = new Options();

    static {
        options.addOption(new Option("df", false, "debug flag"));
        options.addOption(new Option("lf", false, "log flag"));
        options.addOption(new Option("mf", false, "metric flag"));
//        options.addOption(new Option("mnc", true, "max node count"));
//        options.addOption(new Option("bpn", true, "bytes per node"));
//        options.addOption(new Option("ltm", true, "lease threshold multiplier"));
    }

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(OldManualTests.class);
        System.out.println("SLF4J is using: " + log.getClass().getName());
        DefaultParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(String.format("SandboxUserApp - Failed command line options parsing"));
            throw new RuntimeException(e);
        }

//        DebugConstants.LOGREADER_FLAG = cl.hasOption("lf") || DebugConstants.LOGREADER_FLAG;
        DebugConstants.DEBUG_FLAG = cl.hasOption("df") || DebugConstants.DEBUG_FLAG;
        DebugConstants.METRICS_FLAG = cl.hasOption("mf") || DebugConstants.METRICS_FLAG;

        if (DebugConstants.DEBUG_FLAG) {
            log.info("SandboxUserApp - DEBUG FLAG ON");
            Configurator.setRootLevel(Level.DEBUG);
        }
        //TODO: Implement proper logging
//        System.out.println("Testing database connection");
        DebugConstants.LOGREADER_FLAG = true;
        try {
            if (args.length == 0) {
//            sandboxRedesignTest();
//            sandbox_zktest();
//            sandboxRedesignWatcherTest();
//            sandboxRedesignPerformanceTest();
//            sandboxZKPerformanceTest();
                // NOTE: AmazonDB Test
//            sandboxDBTest();
//            sandboxParserTest();
//                sandboxRDBTest();
//            sandboxDBPerformanceTest();
//            sandboxDBTest();
            } else {
                // NOTE: Test rollback mechanisms
                int flag = Integer.parseInt(args[0]);
                if (args.length == 2) {
                    DEBUG_FLAG = true;
                    TESTS_DEBUG_FLAG = true;
                }
                if (flag == 0) {
                    // NOTE: Try to insert and wait
                    // TODO: Currently disabled
//                    insertWaitRDBTest();
//                    insertWaitRDBAssertsTest();
                } else if (flag == 1) {
                    // NOTE: Insert quickly
//                    sandboxRDBTest();
                    rdbBasicCreateAssertsTest();
                } else if (flag == 2) {
                    // NOTE: Insert quickly
                    sandboxDBPerformanceTest();
                } else if (flag == 3) {
                    rdbCleanUp();
                } else if (flag == 4) {
                    rdbDeleteAssertsTest();
                } else if (flag == 5) {
                    rdbEphemeralAssertsTest();
                } else if (flag == 6) {
                    rdbEphemeralChildAssertsTest();
                } else if (flag == 7) {
                    rdbSimpleWatchAssertsTest();
                } else if (flag == 8) {
//                    rdbSetTest();
                } else if (flag == 9) {
                    rdbComplexWatchSetAssertsTest();
                } else if (flag == 10) {
                    rdbWatchSetForcedFailVersionAssertsTest();
                } else if (flag == 11) {
                    rdbWatchDeleteAssertsTest();
                } else if (flag == 12) {
                    rdbWatchDeleteInWatchAssertsTest();
                } else if (flag == 13) {
                    rdbCreateGetNoWaitAssertsTest();
                } else if (flag == 14) {
                    rdbCacheAssertsTest();
                } else if (flag == 15) {
                    rdbLeaseAssertsTest();
                } else if (flag == 16) {
                    rdbLeaseExtendAssertsTest();
                } else if (flag == 17) {
                    rdbListChildrenAssertsTest();
                } else if (flag == 18) {
                    rdbSequentialAssertsTest();
                } else if (flag == 19) {
//                    basicEphemeralParseTest();
                } else if (flag == 20) {
                    basicNetworkTest0();
                } else if (flag == 21) {
                    basicNetworkTest1();
                } else if (flag == 22) {
                    rdbOutstandingChangesAssertsTest();
                } else if (flag == 23) {
                    genericExecutorMicroSecTest();
                } else if (flag == 24) {
                    leaseExtensionExecutionTimeTest();
                } else if (flag == 25) {
                    leaseExtensionNoConnExecutionTimeTest();
                } else if (flag == 26) {
                    fsFileTouchLogicTest("/mnt/efs/fs1/CSVC_TEST");
                } else if (flag == 27) {
                    tbdFSLogRecoveryTest();
                } else if (flag == 94) {
//                    testFutureTryExceptions();
                } else if (flag == 95) {
//                    testSystemTimeDrift();
                } else if (flag == 96) {
                    interfaceTest();
                } else if (flag == 97) {
                    int targetTest = 1;
                    if (args.length > 1) {
                        targetTest = Integer.parseInt(args[1]);
                        if (args.length > 2) {
                            DEBUG_FLAG = true;
                            TESTS_DEBUG_FLAG = true;
                        } else {
                            DEBUG_FLAG = false;
                            TESTS_DEBUG_FLAG = false;
                        }
                    }
                    runTargetTestWithManager(targetTest, 1);
                } else if (flag == 98) {
                    allBaselineTestsWithManager(1);
                } else if (flag == 99) {
                    allBaselineTests();
                }
            }
        } catch (Exception e) {
            System.out.println("===");
            System.out.println("\t\tTest failed");
            System.out.println("===");
            throw new RuntimeException(e);
        }
//
//        String hostname = args[0];
//        int port = Integer.parseInt(args[1]);
//
//        //TODO: handle non-happy path
//        StringBuilder cmd = new StringBuilder();
//
//        for (int i = 2; i < args.length; i++) {
//            cmd.append(args[i]);
//            cmd.append(" ");
//        }
//
//        String[] tokens = cmd.toString().trim().split(" ");
//
//        if (tokens[0].equalsIgnoreCase("benchmark")) {
//            runBenchmark(tokens[1], hostname, port);
////            runBenchmarkSingleConnection(tokens[1], hostname, port);
//            return;
//        }
//
//        try (Socket socket = new Socket(hostname, port)) {
//            OutputStream output = socket.getOutputStream();
//            PrintWriter writer = new PrintWriter(output, true);
//            writer.println(cmd.toString().trim());
//            System.out.println(String.format("[cmd=%s]", cmd.toString().trim()));
//
//            InputStream input = socket.getInputStream();
//            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
//
//            String response = reader.readLine();
//            System.out.println(String.format("[response=%s]", response));
//
//            if (response.equals(CONNECTION_CONFIRMATION_MESSAGE)) {
//                System.out.println(String.format("REMOVE ME: Manage Connection"));
//                manageConnection(reader, writer);
//            }
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
    }

    public static void leaseExtensionNoConnExecutionTimeTest() throws InvalidServiceStateException, IOException {
        System.out.println(String.format("Testing leaseExtensionNoConnExecutionTimeTest:"));
        ArrayList<Long> nanoTimePoints = new ArrayList<>();
        try (DataLayerMgrRDB manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            manager.claimSoftLease();
            int count = 0;
            int maxCount = 50;
            Connection conn;
            try {
                conn = ConnectionPool.getConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            while (count < maxCount) {
                manager.extendSoftLease(conn);
                nanoTimePoints.add(System.nanoTime());
                count++;
            }
            conn.close();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | SQLException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        int idx = 1;
        while (idx < nanoTimePoints.size()) {
            long timeDeltaNS = nanoTimePoints.get(idx) - nanoTimePoints.get(idx-1);
            System.out.println(String.format("\tExtend Lease delta between executions - NANOTIME" +
                            " [calculatedDeltaNS=%d, milliSeconds=%f]",
                    timeDeltaNS, (double)timeDeltaNS/1000000.0));
            idx++;
        }
    }

    public static void leaseExtensionExecutionTimeTest() throws InvalidServiceStateException, IOException {
        System.out.println(String.format("Testing leaseExtensionExecutionTimeTest:"));
        ArrayList<Long> nanoTimePoints = new ArrayList<>();
        try (DataLayerMgrRDB manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            manager.claimSoftLease();
            int count = 0;
            int maxCount = 50;
            while (count < maxCount) {
                manager.extendSoftLease();
                nanoTimePoints.add(System.nanoTime());
                count++;
            }

//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        int idx = 1;
        while (idx < nanoTimePoints.size()) {
            long timeDeltaNS = nanoTimePoints.get(idx) - nanoTimePoints.get(idx-1);
            System.out.println(String.format("\tExtend Lease delta between executions - NANOTIME" +
                            " [calculatedDeltaNS=%d, milliSeconds=%f]",
                            timeDeltaNS, (double)timeDeltaNS/1000000.0));
            idx++;
        }
    }

    public static void genericExecutorMicroSecTest() throws InterruptedException {
//        StopWatch executorWatch = new StopWatch();
        System.out.println(String.format("Testing genericExecutorMicroSecTest:"));

        int periodMicroSec = 750;

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        ArrayList<Instant> executionPoints = new ArrayList<>();
        ArrayList<Long> nanoTimePoints = new ArrayList<>();
        executor.scheduleAtFixedRate(() -> {
            executionPoints.add(Instant.now());
            nanoTimePoints.add(System.nanoTime());
            },0, periodMicroSec,TimeUnit.MICROSECONDS);
        Thread.sleep(30);
        executor.shutdownNow();
        int idx = 1;
        while (idx < executionPoints.size() && idx < nanoTimePoints.size()) {
            long timeDeltaNS = executionPoints.get(idx).getNano() - executionPoints.get(idx-1).getNano();
            System.out.println(String.format("Executor delta between executions - INSTANTS" +
                            " [periodMicroSec=%d, calculatedDelta=%d]",
                            periodMicroSec, timeDeltaNS));
            timeDeltaNS = nanoTimePoints.get(idx) - nanoTimePoints.get(idx-1);
            System.out.println(String.format("\tExecutor delta between executions - NANOTIME" +
                            " [periodMicroSec=%d, calculatedDeltaNS=%d, milliSeconds=%f]",
                            periodMicroSec, timeDeltaNS, (double)timeDeltaNS/1000000.0));
            idx++;
        }
    }

    public static void interfaceTest() {
        String testOutput;
        String expectedOutput = "TestImpl";
        try (TestBase base = new TestImpl()) {
            testOutput = base.interfaceMethodTest();
            System.out.println(testOutput);
        }
    }

    public static void allBaselineTestsWithManager(int managerType)
                                                    throws IOException, ClassNotFoundException {
        int maxTest = 17;
        int idx = 0;
//        rdbCleanUp();
        tbdCleanUpManager(managerType);
        ArrayList<Integer> failedTests = new ArrayList<>();
        while (idx <= maxTest) {
            try {
                log.info("\n===\nNew Test\n===");
                runTargetTestWithManager(idx, managerType);
            } catch (AssertionError e) {
                log.error(String.format("===\n\tFailed Test [idx=%d]\n===", idx), e);
                failedTests.add(idx);
            } catch (InvalidServiceStateException | IOException e) {
                log.error(String.format("===\n\tCritical Error - Unexpected Exception [idx=%d]\n===", idx), e);
                failedTests.add(idx);
            }
            try {
//                System.gc();
                Thread.sleep(750);
                tbdCleanUpManager(managerType);
                if (Files.exists(Paths.get("/mnt/efs/fs1/CSVC_TEST"))) {
                    log.info("\n===\nFAILED TO CLEAN UP\n===");
                } else {
                    log.info("\n===\nCLEAN UP SUCCESSFUL\n===");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            idx++;
        }
        if (failedTests.isEmpty()) {
            log.info("\n===\nAll Tests Passed\n===");
        } else {
            log.error("\n===\nFailed Tests:");
            log.error(failedTests.toString());
            log.error("===");
        }
    }

    public static void allBaselineTests() throws IOException, ClassNotFoundException {
        int maxTest = 17;
        int idx = 0;
        rdbCleanUp();
        ArrayList<Integer> failedTests = new ArrayList<>();
        while (idx <= maxTest) {
            try {
                runTargetTest(idx);
            } catch (AssertionError e) {
                log.error(String.format("===\n\tFailed Test [idx=%d]\n===", idx), e);
                failedTests.add(idx);
            } catch (InvalidServiceStateException | IOException e) {
                log.error(String.format("===\n\tCritical Error - Unexpected Exception [idx=%d]\n===", idx), e);
                failedTests.add(idx);
            } finally {
                rdbCleanUp();
            }
            idx++;
        }
        if (failedTests.isEmpty()) {
            log.info("\n===\nAll Tests Passed\n===");
        } else {
            log.error("\n===\nFailed Tests:");
            log.error(failedTests.toString());
            log.error("===");
        }
    }

    public static DataLayerMgrBase tbdInitializeManagerHelper(int managerType)
            throws InvalidServiceStateException, IOException, ClassNotFoundException {
        switch (managerType) {
            case 0:
                return new DataLayerMgrRDB(null, "/");
            case 1:
                return new DataLayerMgrSharedFS("/mnt/efs/fs1/CSVC_TEST", "/");
            default:
                throw new InvalidServiceStateException("Invalid value");
        }
    }

    public static void runTargetTestWithManager(int testIndex, int managerType)
                                                    throws InvalidServiceStateException, IOException {
        if (testIndex == 0) {
            // NOTE: Try to insert and wait
            // TODO: Currently disabled
//            insertWaitRDBAssertsTest();
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
//                tbdBasicCreateAssertsTestManager(manager);
                ((DataLayerMgrSharedFS)manager).tbdFSCleanup();
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 1) {
            // NOTE: Insert quickly
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdBasicCreateAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 2) {
            // NOTE: Insert quickly
//            sandboxDBPerformanceTest();
        } else if (testIndex == 3) {
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdDeleteAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 4) {
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdEphemeralAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 5) {
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdEphemeralChildAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 6) {
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdSimpleWatchAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 7) {
            // TODO: After watch implement
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdComplexWatchSetAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 8) {
            // TODO: After watch implement
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdWatchSetForcedFailVersionAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 9) {
            // TODO: After watch implement
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdWatchDeleteAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 10) {
            // TODO: After watch implement
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdWatchDeleteInWatchAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 11) {
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdCreateGetNoWaitAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 12) {
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdCacheAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 13) {
            // TODO: After lease implement
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType);
                 DataLayerMgrBase managerLeaseTester = tbdInitializeManagerHelper(managerType)) {
                tbdLeaseAssertsTestManager(manager, managerLeaseTester);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 14) {
            // TODO: After lease implement
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType);
                 DataLayerMgrBase managerLeaseTester = tbdInitializeManagerHelper(managerType)) {
                tdbLeaseExtendAssertsTestManager(manager, managerLeaseTester);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 15) {
            // TODO: After retrieve children implement
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tdbListChildrenAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 16) {
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdSequentialAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 17) {
            // TODO: Consider if outstanding changes fit generally
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdOutstandingChangesAssertsTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 18) {
            // TODO: Ephemeral test
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType);
                 DataLayerMgrBase managerEphemTester = tbdInitializeManagerHelper(managerType)) {
                tbdEphemeralCleanupTestManager(manager, managerEphemTester);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 19) {
            // TODO: Ephemeral test
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdAddRemoveWatchTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else if (testIndex == 99) {
            // TODO: No-Op Test
            try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
                tbdBasicInitStartTestManager(manager);
            } catch (InterruptedException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void runTargetTest(int testIndex) throws InvalidServiceStateException, IOException {
        if (testIndex == 0) {
            // NOTE: Try to insert and wait
            // TODO: Currently disabled
//            insertWaitRDBAssertsTest();
        } else if (testIndex == 1) {
            // NOTE: Insert quickly
            rdbBasicCreateAssertsTest();
        } else if (testIndex == 2) {
            // NOTE: Insert quickly
//            sandboxDBPerformanceTest();
        } else if (testIndex == 3) {
            rdbDeleteAssertsTest();
        } else if (testIndex == 4) {
            rdbEphemeralAssertsTest();
        } else if (testIndex == 5) {
            rdbEphemeralChildAssertsTest();
        } else if (testIndex == 6) {
            rdbSimpleWatchAssertsTest();
        } else if (testIndex == 7) {
            rdbComplexWatchSetAssertsTest();
        } else if (testIndex == 8) {
            rdbWatchSetForcedFailVersionAssertsTest();
        } else if (testIndex == 9) {
            rdbWatchDeleteAssertsTest();
        } else if (testIndex == 10) {
            rdbWatchDeleteInWatchAssertsTest();
        } else if (testIndex == 11) {
            rdbCreateGetNoWaitAssertsTest();
        } else if (testIndex == 12) {
            rdbCacheAssertsTest();
        } else if (testIndex == 13) {
            rdbLeaseAssertsTest();
        } else if (testIndex == 14) {
            rdbLeaseExtendAssertsTest();
        } else if (testIndex == 15) {
            rdbListChildrenAssertsTest();
        } else if (testIndex == 16) {
            rdbSequentialAssertsTest();
        } else if (testIndex == 17) {
            rdbOutstandingChangesAssertsTest();
        }
    }

    public static void tbdOutstandingChangesAssertsTestManager(DataLayerMgrBase manager)
                            throws InvalidServiceStateException, IOException {
        System.out.println("Testing tbdOutstandingChangesAssertsTestManager:");
        ArrayList<String> cleanUpPaths = new ArrayList<>();
        DebugConstants.LOGREADER_FLAG = false;
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        CommandOutput output;
        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);
        cleanUpPaths.add(path);

//            Thread.sleep(2500);
        int childId = 1;

        while (childId <= 5) {
            cmdStr = String.format("create -s %s %s", path+"/node", generatedString);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            output = ((CreateCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(
                        String.format("CmdCreate Output [output=\n%s]",
                                output.toString()));
            }
            cleanUpPaths.add(output.getPath());
            childId++;
        }

        cmdStr = String.format("ls -L %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
//
////            Thread.sleep(1000);
        output = ((LsCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(
                    String.format("CmdLs Output [output=\n%s]",
                            output.getChildrenSet().toString()));
        }
        for (String childPath : output.getChildrenSet()) {
            output = GetCommand.generateGetCommand(testRootPath+"/"+childPath).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(
                        String.format("GetCommand Output [output=\n%s]",
                                output.toString()));
            }
            assert (cleanUpPaths.contains(testRootPath+"/"+childPath));
//                System.out.println(
//                        String.format("GetCommand Output [\ngeneratedString=\n%s,\noutput=\n%s]",
//                                generatedString, new String(output.getData(), StandardCharsets.UTF_8)));
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
//                assert (generatedString.equals("Nonsense"));
//                assert (false);
        }

        // NOTE: Clean up
        int idx = cleanUpPaths.size()-1;
        while (idx >= 0) {
            output = DeleteCommand.generateDeleteCommand(cleanUpPaths.get(idx), true).executeManager(manager);
            try {
                assert (output.getErrorCode() == 0);
            } catch (AssertionError e) {
                log.error(String.format("Test Error - rdbOutstandingChangesAssertsTest " +
                                "- Failed cleanup [path=%s]",
                        cleanUpPaths.get(idx)));
            }
            idx--;
        }
    }

    public static void rdbOutstandingChangesAssertsTest() {
        System.out.println("Testing rdbOutstandingChangesTest:");
        ArrayList<String> cleanUpPaths = new ArrayList<>();
//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            DebugConstants.LOGREADER_FLAG = false;
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            CommandOutput output;
            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);
            cleanUpPaths.add(path);

//            Thread.sleep(2500);
            int childId = 1;

            while (childId <= 5) {
                cmdStr = String.format("create -s %s %s", path+"/node", generatedString);
                if (TESTS_DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
                }
                tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
                parser.resetParser();
                parser.parseOptions(tokens);
                try {
                    cmd = parser.processCommand();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }

                output = ((CreateCommand) cmd).executeManager(manager);
                if (TESTS_DEBUG_FLAG) {
                    System.out.println(
                            String.format("CmdCreate Output [output=\n%s]",
                                    output.toString()));
                }
                cleanUpPaths.add(output.getPath());
                childId++;
            }

            cmdStr = String.format("ls -L %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
//
////            Thread.sleep(1000);
            output = ((LsCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(
                        String.format("CmdLs Output [output=\n%s]",
                                output.getChildrenSet().toString()));
            }
            for (String childPath : output.getChildrenSet()) {
                output = GetCommand.generateGetCommand(testRootPath+"/"+childPath).executeManager(manager);
                if (TESTS_DEBUG_FLAG) {
                    System.out.println(
                            String.format("GetCommand Output [output=\n%s]",
                                    output.toString()));
                }
                assert (cleanUpPaths.contains(testRootPath+"/"+childPath));
//                System.out.println(
//                        String.format("GetCommand Output [\ngeneratedString=\n%s,\noutput=\n%s]",
//                                generatedString, new String(output.getData(), StandardCharsets.UTF_8)));
                assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
//                assert (generatedString.equals("Nonsense"));
//                assert (false);
            }

            // NOTE: Clean up
            int idx = cleanUpPaths.size()-1;
            while (idx >= 0) {
                output = DeleteCommand.generateDeleteCommand(cleanUpPaths.get(idx), true).executeManager(manager);
                try {
                    assert (output.getErrorCode() == 0);
                } catch (AssertionError e) {
                    log.error(String.format("Test Error - rdbOutstandingChangesAssertsTest " +
                                            "- Failed cleanup [path=%s]",
                                            cleanUpPaths.get(idx)));
                }
                idx--;
            }
        } catch (ClassNotFoundException | IOException | InvalidServiceStateException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void basicNetworkTest1() throws IOException, InvalidServiceStateException {
        System.out.println("Testing basicNetworkTest0:");
        try (DataLayerMgrRDB manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();
            int networkSalt = 1;

            CommandOutput output;
            String generatedString = "contents";
            String testRootPath = "/networkTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

            Thread.sleep(2500);

            while (true) {
                int childId = 1;
                while (childId <= 5) {
                    cmdStr = String.format("create -s %s %s", path+"/node"+networkSalt+"_", generatedString);
                    System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
                    tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
                    parser.resetParser();
                    parser.parseOptions(tokens);
                    try {
                        cmd = parser.processCommand();
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }

                    output = ((CreateCommand) cmd).executeManager(manager);
                    System.out.println(
                            String.format("CmdCreate Output [output=\n%s]",
                                    output.toString()));
                    childId++;
                    Thread.sleep(50);
                }
                cmdStr = String.format("ls %s", path);
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
                tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
                parser.resetParser();
                parser.parseOptions(tokens);
                try {
                    cmd = parser.processCommand();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
//
////            Thread.sleep(1000);
                output = ((LsCommand) cmd).executeManager(manager);
                System.out.println(
                        String.format("CmdLs Output [output=\n%s]",
                                output.getChildrenSet().toString()));
                Thread.sleep(1500);
            }
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void basicNetworkTest0() throws IOException, InvalidServiceStateException {
        System.out.println("Testing basicNetworkTest0:");
        try (DataLayerMgrRDB manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();
            int networkSalt = 0;

            CommandOutput output;
            String generatedString = "contents";
            String testRootPath = "/networkTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

            Thread.sleep(2500);

            while (true) {
                int childId = 1;
                while (childId <= 5) {
                    cmdStr = String.format("create -s %s %s", path+"/node"+networkSalt+"_", generatedString);
                    System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
                    tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
                    parser.resetParser();
                    parser.parseOptions(tokens);
                    try {
                        cmd = parser.processCommand();
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }

                    output = ((CreateCommand) cmd).executeManager(manager);
                    System.out.println(
                            String.format("CmdCreate Output [output=\n%s]",
                                    output.toString()));
                    childId++;
                    Thread.sleep(50);
                }
                cmdStr = String.format("ls %s", path);
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
                tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
                parser.resetParser();
                parser.parseOptions(tokens);
                try {
                    cmd = parser.processCommand();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
//
////            Thread.sleep(1000);
                output = ((LsCommand) cmd).executeManager(manager);
                System.out.println(
                        String.format("CmdLs Output [output=\n%s]",
                                output.getChildrenSet().toString()));
                Thread.sleep(1500);
            }
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void basicEphemeralParseAssertsTest() {
        // String test
        String testString = new String(new byte[0], StandardCharsets.UTF_8);
//        System.out.println("===\n"+testString+"\n====");
        String generatedString = "contents";
        CreateCommand cmd = CreateCommand.generateCreateCommand(
                "/TestPath", generatedString.getBytes(StandardCharsets.UTF_8),
                true, true);
//        cmd = CreateCommand.generateCreateCommand("/TestPath", new byte[0], true, true);
        System.out.println("===\n"+cmd.toString());
        CSvcSandboxParser parser = new CSvcSandboxParser();
//
//        String testRootPath = "/redesignTest";
//        String path = String.format("%s", testRootPath);
//        final String[] cmdStr = {String.format("create -e -s %s %s", path, generatedString)};
//        String[] tokens = cmdStr[0].split(" ");
//        parser.parseOptions(tokens);
//        CSvcCommand cmdBasic = null;
//        try {
//            cmdBasic = parser.processCommand();
//        } catch (ParseException e) {
//            throw new RuntimeException(e);
//        }
    }

    public static void tbdWatchSetForcedFailVersionAssertsTestManager(DataLayerMgrBase manager)
                            throws IOException, InvalidServiceStateException, InterruptedException {
        System.out.println("Testing tbdWatchSetForcedFailVersionAssertsTestManager:");
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String generatedStringSet = generatedString+"2";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
        String[] tokens = cmdStr[0].split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);

        cmdStr[0] = String.format("get -w %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

//            Thread.sleep(1000);
        // TODO: Consider how to redesign this so that commands can be changed in watch trigger
        final SetCommand[] postWatchCmd = new SetCommand[1];
        ((GetCommand) cmd).setWatchCallable(((watchManager, commandOutput)
                -> {
            System.out.println("===\n"
                    +"commandOutput.getOutput()"
                    +"\n===\n"
                    +commandOutput.toString()
                    +"\n===");
            // NOTE: Force version check failure
            String postWatchStr = String.format("set -v %d %s %s",
                    commandOutput.getRecordVersion()-1, path, generatedString+"_postWatch");
            CSvcSandboxParser watchParser = new CSvcSandboxParser();
            watchParser.parseOptions(postWatchStr.split(" "));
            try {
                postWatchCmd[0] = (SetCommand) watchParser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }), false);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));


        cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((SetCommand) cmd).executeManager(manager);
//            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        Thread.sleep(500);

        output = postWatchCmd[0].executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println("===\n"
                    + "Post Watch Set Output\n===\n"
                    + output
                    + "\n===");
        }
        assert (output.getErrorCode() == 1);

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        assert (output.getErrorCode() == 0);
    }

    public static void rdbWatchSetForcedFailVersionAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbWatchSetForcedFailVersionTest:");
//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String generatedStringSet = generatedString+"2";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
            String[] tokens = cmdStr[0].split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);

            cmdStr[0] = String.format("get -w %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//            Thread.sleep(1000);
            // TODO: Consider how to redesign this so that commands can be changed in watch trigger
            final SetCommand[] postWatchCmd = new SetCommand[1];
            ((GetCommand) cmd).setWatchCallable(((watchManager, commandOutput)
                    -> {
                System.out.println("===\n"
                        +"commandOutput.getOutput()"
                        +"\n===\n"
                        +commandOutput.toString()
                        +"\n===");
                // NOTE: Force version check failure
                String postWatchStr = String.format("set -v %d %s %s",
                        commandOutput.getRecordVersion()-1, path, generatedString+"_postWatch");
                CSvcSandboxParser watchParser = new CSvcSandboxParser();
                watchParser.parseOptions(postWatchStr.split(" "));
                try {
                    postWatchCmd[0] = (SetCommand) watchParser.processCommand();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }), false);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));


            cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((SetCommand) cmd).executeManager(manager);
//            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            Thread.sleep(500);

            output = postWatchCmd[0].executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println("===\n"
                        + "Post Watch Set Output\n===\n"
                        + output
                        + "\n===");
            }
            assert (output.getErrorCode() == 1);

            // NOTE: Clean up
            output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
            assert (output.getErrorCode() == 0);
//            endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tbdWatchDeleteInWatchAssertsTestManager(DataLayerMgrBase manager)
                            throws IOException, InvalidServiceStateException, InterruptedException {
        System.out.println("Testing tbdWatchDeleteInWatchAssertsTestManager:");

        manager.setLeaseMultiplier(40);
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String generatedStringSet = generatedString + "2";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
        String[] tokens = cmdStr[0].split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);

        cmdStr[0] = String.format("get -w %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

//            Thread.sleep(1000);
        // TODO: Consider how to redesign this so that commands can be changed in watch trigger
        final DeleteCommand[] postWatchCmd = new DeleteCommand[1];
        ((GetCommand) cmd).setWatchCallable((watchManager, commandOutput)
                -> {
            System.out.println("===\n"
                    +"commandOutput.getOutput()"
                    +"\n===\n"
                    +commandOutput.toString()
                    +"\n===");
            String postWatchStr = String.format("delete -v %d %s",
                    commandOutput.getRecordVersion(), path);
            CSvcSandboxParser watchParser = new CSvcSandboxParser();
            watchParser.parseOptions(postWatchStr.split(" "));
            try {
                postWatchCmd[0] = (DeleteCommand) watchParser.processCommand();
                CommandOutput watchOutput = postWatchCmd[0].executeManager(manager);
                System.out.println("===\n"
                        +"Post Watch Delete Output\n===\n"
                        +watchOutput
                        +"\n===");
            } catch (ParseException | InvalidServiceStateException e) {
                throw new RuntimeException(e);
            }
        }, false);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        output = GetCommand.generateGetCommand(path).executeManager(manager);
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((SetCommand) cmd).executeManager(manager);
        assert (output.getErrorCode() == 0);

//            Thread.sleep(2500);
//            Thread.sleep(2500);

        cmdStr[0] = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        Thread.sleep(500);

        output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("Post Delete Get Output [output=\n%s]", output.getOutput()));
        }
        assert (output.getErrorCode() == 1);
    }


    public static void rdbWatchDeleteInWatchAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbWatchDeleteInWatchTest:");
//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String generatedStringSet = generatedString + "2";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
            String[] tokens = cmdStr[0].split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);

            cmdStr[0] = String.format("get -w %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//            Thread.sleep(1000);
            // TODO: Consider how to redesign this so that commands can be changed in watch trigger
            final DeleteCommand[] postWatchCmd = new DeleteCommand[1];
            ((GetCommand) cmd).setWatchCallable((watchManager, commandOutput)
                    -> {
                System.out.println("===\n"
                        +"commandOutput.getOutput()"
                        +"\n===\n"
                        +commandOutput.toString()
                        +"\n===");
                String postWatchStr = String.format("delete -v %d %s",
                        commandOutput.getRecordVersion(), path);
                CSvcSandboxParser watchParser = new CSvcSandboxParser();
                watchParser.parseOptions(postWatchStr.split(" "));
                try {
                    postWatchCmd[0] = (DeleteCommand) watchParser.processCommand();
                    CommandOutput watchOutput = postWatchCmd[0].executeManager(manager);
                    System.out.println("===\n"
                            +"Post Watch Delete Output\n===\n"
                            +watchOutput
                            +"\n===");
                } catch (ParseException | InvalidServiceStateException e) {
                    throw new RuntimeException(e);
                }
            }, false);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            output = GetCommand.generateGetCommand(path).executeManager(manager);
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            output = ((SetCommand) cmd).executeManager(manager);
            assert (output.getErrorCode() == 0);

//            Thread.sleep(2500);
//            Thread.sleep(2500);

            cmdStr[0] = String.format("get %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            Thread.sleep(500);

            output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("Post Delete Get Output [output=\n%s]", output.getOutput()));
            }
            assert (output.getErrorCode() == 1);
//            endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tbdWatchDeleteAssertsTestManager(DataLayerMgrBase manager)
                            throws IOException, InvalidServiceStateException, InterruptedException {
        System.out.println("Testing tbdWatchDeleteAssertsTestManager:");

        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String generatedStringSet = generatedString + "2";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
        String[] tokens = cmdStr[0].split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);

        cmdStr[0] = String.format("get -w %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

//            Thread.sleep(1000);
        // TODO: Consider how to redesign this so that commands can be changed in watch trigger
        final DeleteCommand[] postWatchCmd = new DeleteCommand[1];
        ((GetCommand) cmd).setWatchCallable((watchManager, commandOutput)
                -> {
            System.out.println("===\n"
                    +"commandOutput.getOutput()"
                    +"\n===\n"
                    +commandOutput.toString()
                    +"\n===");
            String postWatchStr = String.format("delete -v %d %s",
                    commandOutput.getRecordVersion(), path);
            CSvcSandboxParser watchParser = new CSvcSandboxParser();
            watchParser.parseOptions(postWatchStr.split(" "));
            try {
                postWatchCmd[0] = (DeleteCommand) watchParser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }, false);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));


        cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }


        ((SetCommand) cmd).executeManager(manager);
        output = GetCommand.generateGetCommand(path).executeManager(manager);
        assert (generatedStringSet.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        Thread.sleep(500);

        output = postWatchCmd[0].executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println("===\n"
                    + "Post Watch Delete Output\n===\n"
                    + output
                    + "\n===");
        }
//            Thread.sleep(2500);

        cmdStr[0] = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("Post Delete Get Output [output=\n%s]", output.getOutput()));
        }
        assert (output.getErrorCode() == 1);
    }

    public static void rdbWatchDeleteAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbWatchDeleteTest:");

//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String generatedStringSet = generatedString + "2";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
            String[] tokens = cmdStr[0].split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);

            cmdStr[0] = String.format("get -w %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//            Thread.sleep(1000);
            // TODO: Consider how to redesign this so that commands can be changed in watch trigger
            final DeleteCommand[] postWatchCmd = new DeleteCommand[1];
            ((GetCommand) cmd).setWatchCallable((watchManager, commandOutput)
                    -> {
                System.out.println("===\n"
                        +"commandOutput.getOutput()"
                        +"\n===\n"
                        +commandOutput.toString()
                        +"\n===");
                String postWatchStr = String.format("delete -v %d %s",
                        commandOutput.getRecordVersion(), path);
                CSvcSandboxParser watchParser = new CSvcSandboxParser();
                watchParser.parseOptions(postWatchStr.split(" "));
                try {
                    postWatchCmd[0] = (DeleteCommand) watchParser.processCommand();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }, false);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));


            cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }


            ((SetCommand) cmd).executeManager(manager);
            output = GetCommand.generateGetCommand(path).executeManager(manager);
            assert (generatedStringSet.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            Thread.sleep(500);

            output = postWatchCmd[0].executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println("===\n"
                        + "Post Watch Delete Output\n===\n"
                        + output
                        + "\n===");
            }
//            Thread.sleep(2500);

            cmdStr[0] = String.format("get %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("Post Delete Get Output [output=\n%s]", output.getOutput()));
            }
            assert (output.getErrorCode() == 1);
//            endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tbdComplexWatchSetAssertsTestManager(DataLayerMgrBase manager)
                        throws IOException, InvalidServiceStateException, InterruptedException {
        System.out.println("Testing tbdComplexWatchSetAssertsTestManager:");
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String generatedStringWatch = generatedString + "_postWatch";
        String generatedStringSet = generatedString + "2";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
        String[] tokens = cmdStr[0].split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);
//            Thread.sleep(250);

        cmdStr[0] = String.format("get -w %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

//            Thread.sleep(1000);
        // TODO: Consider how to redesign this so that commands can be changed in watch trigger
        final SetCommand[] postWatchCmd = new SetCommand[1];
        ((GetCommand) cmd).setWatchCallable((watchManager, commandOutput)
                -> {
            System.out.println("===\n"
                    +"commandOutput.getOutput()"
                    +"\n===\n"
                    +commandOutput.toString()
                    +"\n===");
            String postWatchStr = String.format("set -v %d %s %s",
                    commandOutput.getRecordVersion(), path, generatedStringWatch);
            CSvcSandboxParser watchParser = new CSvcSandboxParser();
            watchParser.parseOptions(postWatchStr.split(" "));
            try {
                postWatchCmd[0] = (SetCommand)watchParser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }, false);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((SetCommand) cmd).executeManager(manager);

        output = GetCommand.generateGetCommand(path).executeManager(manager);
        assert (generatedStringSet.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        Thread.sleep(500);

        output = postWatchCmd[0].executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println("===\n"
                    + "Post Watch Set Output\n===\n"
                    + output
                    + "\n===");
        }
        output = GetCommand.generateGetCommand(path).executeManager(manager);
        assert (generatedStringWatch.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        assert (output.getErrorCode() == 0);
    }

    public static void rdbComplexWatchSetAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbWatchSetTest:");
//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String generatedStringWatch = generatedString + "_postWatch";
            String generatedStringSet = generatedString + "2";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
            String[] tokens = cmdStr[0].split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);
//            Thread.sleep(250);

            cmdStr[0] = String.format("get -w %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//            Thread.sleep(1000);
            // TODO: Consider how to redesign this so that commands can be changed in watch trigger
            final SetCommand[] postWatchCmd = new SetCommand[1];
            ((GetCommand) cmd).setWatchCallable((watchManager, commandOutput)
                    -> {
                System.out.println("===\n"
                    +"commandOutput.getOutput()"
                    +"\n===\n"
                    +commandOutput.toString()
                    +"\n===");
                String postWatchStr = String.format("set -v %d %s %s",
                        commandOutput.getRecordVersion(), path, generatedStringWatch);
                CSvcSandboxParser watchParser = new CSvcSandboxParser();
                watchParser.parseOptions(postWatchStr.split(" "));
                try {
                    postWatchCmd[0] = (SetCommand) watchParser.processCommand();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }, false);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((SetCommand) cmd).executeManager(manager);

            output = GetCommand.generateGetCommand(path).executeManager(manager);
            assert (generatedStringSet.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            Thread.sleep(500);

            output = postWatchCmd[0].executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println("===\n"
                        + "Post Watch Set Output\n===\n"
                        + output
                        + "\n===");
            }
            output = GetCommand.generateGetCommand(path).executeManager(manager);
            assert (generatedStringWatch.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            // NOTE: Clean up
            output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
            assert (output.getErrorCode() == 0);
//            endlessLoop();
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tbdSimpleWatchAssertsTestManager(DataLayerMgrBase manager)
                                                throws IOException, InvalidServiceStateException, InterruptedException {
        System.out.println("Testing tbdSimpleWatchAssertsTest:");

        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);

        cmdStr = String.format("get -w %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        boolean []watchFlag = {false};
//            Thread.sleep(1000);
        ((GetCommand) cmd).setWatchCallable((watchManager, commandOutput)
                -> {
            System.out.println("===\n"
                    +"commandOutput.getOutput()"
                    +"\n===\n"
                    +commandOutput.toString()
                    +"\n===");
            watchFlag[0] = true;
        }, false);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        generatedString = generatedString+"2";
        cmdStr = String.format("set %s %s", path, generatedString);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((SetCommand) cmd).executeManager(manager);

        Thread.sleep(150);

        cmdStr = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

//            Thread.sleep(1000);
        output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
        assert (watchFlag[0]);

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        assert (output.getErrorCode() == 0);
//            endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
    }

    public static void rdbSimpleWatchAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbWatchTest:");

//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);

            cmdStr = String.format("get -w %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            boolean []watchFlag = {false};
//            Thread.sleep(1000);
            ((GetCommand) cmd).setWatchCallable((watchManager, commandOutput)
                    -> {
                System.out.println("===\n"
                    +"commandOutput.getOutput()"
                    +"\n===\n"
                    +commandOutput.toString()
                    +"\n===");
                watchFlag[0] = true;
            }, false);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            generatedString = generatedString+"2";
            cmdStr = String.format("set %s %s", path, generatedString);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            output = ((SetCommand) cmd).executeManager(manager);

            Thread.sleep(150);

            cmdStr = String.format("get %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//            Thread.sleep(1000);
            output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
            assert (watchFlag[0]);

            // NOTE: Clean up
            output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
            assert (output.getErrorCode() == 0);
//            endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public static void tdbLeaseExtendAssertsTestManager(DataLayerMgrBase manager, DataLayerMgrBase managerLeaseTester)
                                                throws IOException, InvalidServiceStateException, InterruptedException {
        System.out.println("Testing tdbLeaseExtendAssertsTestManager:");
        manager.setLeaseMultiplier(40);
        managerLeaseTester.setLeaseMultiplier(40);
        manager.startManager();
        managerLeaseTester.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        managerLeaseTester.claimSoftLease();
        ManagerLeaseHandlerBase leaseHandler = managerLeaseTester.startLeaseHandler();
        boolean leaseClosedFlag = false;
        int sleepMS = 500;
        int count = 1;
        while (!manager.claimSoftLease(false)) {
            if (count < 5) {
                if (TESTS_DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - Extend tester lease [attempt=%d]", count));
                }
            } else {
                leaseHandler.close();
                leaseClosedFlag = true;
            }
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Properly maintained lease [attempt=%d]", count));
            }
            Thread.sleep(sleepMS);
            count += 1;
        }
        if (!leaseClosedFlag) {
            leaseHandler.close();
        }
        assert (count >= 5);

        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("===\n===\nDEBUG - Worker manager claimed lease [attempt=%d]", count));
        }
        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
        String[] tokens = cmdStr[0].split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);
//            Thread.sleep(1500);

        cmdStr[0] = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

//        GetCommand cmdGet = GetCommand.generateGetCommand(path);
//        CommandOutput output = cmdGet.executeManager(manager);
//            Thread.sleep(1000);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        assert (output.getErrorCode() == 0);
    }

    public static void rdbLeaseExtendAssertsTest() throws IOException, InvalidServiceStateException {
        //CSvcManagerBase manager, CSvcManagerBase managerLeaseTester
        System.out.println("Testing rdbLeaseExtendTest:");

//        try (CSvcManagerAmazonDB_CmdAgnostic manager
//                     = new CSvcManagerAmazonDB_CmdAgnostic(null, "/");
//             CSvcManagerAmazonDB_CmdAgnostic managerLeaseTester
//                     = new CSvcManagerAmazonDB_CmdAgnostic(null, "/leaseTester/")) {
        try (DataLayerMgrBase manager
                     = new DataLayerMgrRDB(null, "/");
             DataLayerMgrBase managerLeaseTester
                     = new DataLayerMgrAmazonDB_LeaseTest(null, "/leaseTester/")) {
            manager.startManager();
            managerLeaseTester.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            managerLeaseTester.claimSoftLease();
            ManagerLeaseHandlerBase leaseHandler = managerLeaseTester.startLeaseHandler();
            int sleepMS = 50;
            int count = 1;
            while (!manager.claimSoftLease()) {
                if (count < 5) {
//                    if (managerLeaseTester.extendSoftLease() && TESTS_DEBUG_FLAG) {
                    if (TESTS_DEBUG_FLAG) {
                        System.out.println(String.format("DEBUG - Extend tester lease [attempt=%d]", count));
                    }
                } else {
                    leaseHandler.close();
                }
                if (TESTS_DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - Properly maintained lease [attempt=%d]", count));
                }
                Thread.sleep(sleepMS);
//                sleepMS += 2100;
                count += 1;
            }
            assert (count >= 5);

            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("===\n===\nDEBUG - Worker manager claimed lease [attempt=%d]", count));
            }
            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
            String[] tokens = cmdStr[0].split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);
//            Thread.sleep(1500);

            cmdStr[0] = String.format("get %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//        GetCommand cmdGet = GetCommand.generateGetCommand(path);
//        CommandOutput output = cmdGet.executeManager(manager);
//            Thread.sleep(1000);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            // NOTE: Clean up
            output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
            assert (output.getErrorCode() == 0);
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tbdLeaseAssertsTestManager(DataLayerMgrBase manager, DataLayerMgrBase managerLeaseTester)
                                            throws IOException, InvalidServiceStateException, InterruptedException {
        System.out.println("Testing tbdLeaseAssertsTestManager:");

        manager.setLeaseMultiplier(40);
        managerLeaseTester.setLeaseMultiplier(40);
        manager.startManager();
        managerLeaseTester.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();
//        boolean leaseClaim = false;
//        // TODO: Consider what to do for retries
//        while (!leaseClaim) {
//            try {
//                // TODO: Implement deleting timed out waitlists
//                leaseClaim = managerLeaseTester.submitSoftLeaseClaim().get();
//            } catch (InterruptedException | ExecutionException e) {
//                // TODO: Log
////                leaseClaim = false;
//            }
//        }
        //==
        int sleepMS = 2000;
        int count = 1;
        try (ManagerLeaseHandlerBase ignored = managerLeaseTester.claimSoftLeaseHandler(false).get()) {
//            assert (!manager.claimSoftLease(false));
            while (count < 5) {
                assert (!manager.claimSoftLease(false));
                if (TESTS_DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - Properly maintained lease [attempt=%d]", count));
                }
                Thread.sleep(sleepMS);
                count++;
            }

        } catch (InterruptedException | ExecutionException e) {
            // NOTE: No-Op
            throw new InvalidServiceStateException(e);
        }

//        int sleepMS = 2000;
//        int count = 1;
//        try (ManagerLeaseHandlerBase ignored = managerLeaseTester.startLeaseHandler()) {
//            while (count < 5) {
//                assert (!manager.claimSoftLease(false));
//                if (TESTS_DEBUG_FLAG) {
//                    System.out.println(String.format("DEBUG - Properly maintained lease [attempt=%d]", count));
//                }
//                Thread.sleep(sleepMS);
//                count++;
//            }
//        } catch (Exception e) {
//            throw new InvalidServiceStateException(e);
//        }

        count = 1;
        while (!manager.claimSoftLease(false) && count < 5) {
            Thread.sleep(sleepMS);
            count++;
        }
        assert (count<5);

        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("===\n===\nDEBUG - Worker manager claimed lease [attempt=%d]", count));
        }
        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
        String[] tokens = cmdStr[0].split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);
//            Thread.sleep(1500);

        cmdStr[0] = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
        }
        tokens = cmdStr[0].split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

//        GetCommand cmdGet = GetCommand.generateGetCommand(path);
//        CommandOutput output = cmdGet.executeManager(manager);
//            Thread.sleep(1000);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        assert (output.getErrorCode() == 0);
    }

    public static void fsFileTouchLogicTest(String distributedFSPath) throws IOException, InterruptedException {
        System.out.println("Testing fsFileTouchLogicTests:");
        Path testPath = Paths.get(distributedFSPath.concat("/testFile.temp"));
        long testContent = 12345;
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(testContent);
        Files.write(testPath, buffer.array(), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        long originalTS = Files.getLastModifiedTime(testPath).toMillis();
        Thread.sleep(5000);

        // TODO: get modified time of a file
        //  sleep some time
        //  touch file
        //  confirm contents still valid
        //  confirm modified time changed
        long controlTS = Files.getLastModifiedTime(testPath).toMillis();
        assert (originalTS == controlTS);
        int writeCount;
        try (FileChannel fileChannel = FileChannel.open(testPath, StandardOpenOption.WRITE)) {
            writeCount = fileChannel.write(ByteBuffer.wrap(new byte[2]), fileChannel.size());
        }
        assert (ByteBuffer.wrap(Files.readAllBytes(testPath)).getLong() == testContent);
        try (FileChannel fileChannel = FileChannel.open(testPath, StandardOpenOption.WRITE)) {
            fileChannel.truncate(fileChannel.size()-writeCount);
        }
        long postTouchTS = Files.getLastModifiedTime(testPath).toMillis();
        assert (ByteBuffer.wrap(Files.readAllBytes(testPath)).getLong() == testContent);
        System.out.println(String.format("DEBUG - Testing File Touch Logic [original_ts=%d, post_touch_ts=%d]",
                            originalTS, postTouchTS));
        assert (postTouchTS > originalTS + 3000);
    }

    public static void rdbLeaseAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbLeaseTest:");

//        try (CSvcManagerAmazonDB_CmdAgnostic manager
//                     = new CSvcManagerAmazonDB_CmdAgnostic(null, "/");
//             CSvcManagerAmazonDB_CmdAgnostic managerLeaseTester
//                     = new CSvcManagerAmazonDB_CmdAgnostic(null, "/leaseTester/")) {
        try (DataLayerMgrBase manager
                     = new DataLayerMgrRDB(null, "/");
             DataLayerMgrBase managerLeaseTester
                     = new DataLayerMgrRDB(null, "/leaseTester/")) {
            manager.startManager();
            managerLeaseTester.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            managerLeaseTester.claimSoftLease();
            int sleepMS = 2100;
            int count = 1;
            while (!manager.claimSoftLease()) {
                if (TESTS_DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - Properly maintained lease [attempt=%d]", count));
                }
                Thread.sleep(sleepMS);
//                sleepMS += 2100;
                count += 1;
            }

            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("===\n===\nDEBUG - Worker manager claimed lease [attempt=%d]", count));
            }
            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            final String[] cmdStr = {String.format("create %s %s", path, generatedString)};
            String[] tokens = cmdStr[0].split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);
//            Thread.sleep(1500);

            cmdStr[0] = String.format("get %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
            }
            tokens = cmdStr[0].split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//        GetCommand cmdGet = GetCommand.generateGetCommand(path);
//        CommandOutput output = cmdGet.executeManager(manager);
//            Thread.sleep(1000);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            // NOTE: Clean up
            output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
            assert (output.getErrorCode() == 0);
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public static void tbdSequentialAssertsTestManager(DataLayerMgrBase manager) throws IOException, InvalidServiceStateException {
        System.out.println("Testing tbdSequentialAssertsTestManager:");
        ArrayList<String> cleanUpPaths = new ArrayList<>();
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        CommandOutput output;
        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);
        cleanUpPaths.add(path);

        int childId = 1;

        while (childId <= 5) {
            cmdStr = String.format("create -s %s %s", path+"/node", generatedString);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            output = ((CreateCommand) cmd).executeManager(manager);
            cleanUpPaths.add(output.getPath());
            if (TESTS_DEBUG_FLAG) {
                System.out.println(
                        String.format("CmdCreate Output [output=\n%s]",
                                output.toString()));
            }
            childId++;
        }

        cmdStr = String.format("ls -L %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((LsCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(
                    String.format("CmdLs Output [output=\n%s]",
                            output.getChildrenSet().toString()));
        }
        for (String childPath : output.getChildrenSet()) {
            // TODO: Consider making cleanup Paths a set
            assert (cleanUpPaths.contains(testRootPath+"/"+childPath));
        }

        // NOTE: Clean up
        int idx = cleanUpPaths.size()-1;
        while (idx >= 0) {
            output = DeleteCommand.generateDeleteCommand(cleanUpPaths.get(idx))
                    .executeManager(manager);
            try {
                assert (output.getErrorCode() == 0);
            } catch (AssertionError e) {
                log.error(String.format("Test Error - rdbSequentialAssertsTest - Failed cleanup [path=%s]",
                        cleanUpPaths.get(idx)));
            }
            idx--;
        }
    }

    public static void rdbSequentialAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbSequentialTest:");
        ArrayList<String> cleanUpPaths = new ArrayList<>();
//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            CommandOutput output;
            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);
            cleanUpPaths.add(path);

//            Thread.sleep(2500);
            int childId = 1;

            while (childId <= 5) {
                cmdStr = String.format("create -s %s %s", path+"/node", generatedString);
                if (TESTS_DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
                }
                tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
                parser.resetParser();
                parser.parseOptions(tokens);
                try {
                    cmd = parser.processCommand();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }

                output = ((CreateCommand) cmd).executeManager(manager);
                cleanUpPaths.add(output.getPath());
                if (TESTS_DEBUG_FLAG) {
                    System.out.println(
                            String.format("CmdCreate Output [output=\n%s]",
                                    output.toString()));
                }
                childId++;
            }

            cmdStr = String.format("ls -L %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
//
////            Thread.sleep(1000);
            output = ((LsCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(
                        String.format("CmdLs Output [output=\n%s]",
                                output.getChildrenSet().toString()));
            }
            for (String childPath : output.getChildrenSet()) {
                // TODO: Consider making cleanup Paths a set
                assert (cleanUpPaths.contains(testRootPath+"/"+childPath));
            }

            // NOTE: Clean up
            int idx = cleanUpPaths.size()-1;
            while (idx >= 0) {
                output = DeleteCommand.generateDeleteCommand(cleanUpPaths.get(idx))
                            .executeManager(manager);
                try {
                    assert (output.getErrorCode() == 0);
                } catch (AssertionError e) {
                    log.error(String.format("Test Error - rdbSequentialAssertsTest - Failed cleanup [path=%s]",
                            cleanUpPaths.get(idx)));
                }
                idx--;
            }
//            endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public static void rdbListChildrenAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbListChildrenTest:");
        ArrayList<String> cleanUpPaths = new ArrayList<>();
//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);
            cleanUpPaths.add(path);

//            Thread.sleep(2500);
            int childId = 1;

            while (childId <= 5) {
                cmdStr = String.format("create %s %s", path+"/"+childId, generatedString);
                if (TESTS_DEBUG_FLAG) {
                    System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
                }
                tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
                parser.resetParser();
                parser.parseOptions(tokens);
                try {
                    cmd = parser.processCommand();
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }

                ((CreateCommand) cmd).executeManager(manager);
                cleanUpPaths.add(path+"/"+childId);
                childId++;
            }

            cmdStr = String.format("ls -L %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
//
////            Thread.sleep(1000);
            CommandOutput output = ((LsCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(
                        String.format("CmdLs Output Cache [output=\n%s]",
                                output.getChildrenSet().toString()));
            }
            for (String childPath : output.getChildrenSet()) {
                // TODO: Consider making cleanup Paths a set
                assert (cleanUpPaths.contains(testRootPath+"/"+childPath));
            }

            // NOTE: Clean up
            int idx = cleanUpPaths.size()-1;
            while (idx >= 0) {
                output = DeleteCommand.generateDeleteCommand(cleanUpPaths.get(idx))
                        .executeManager(manager);
                try {
                    assert (output.getErrorCode() == 0);
                } catch (AssertionError e) {
                    log.error(String.format("Test Error - rdbListChildrenAssertsTest - Failed cleanup [path=%s]",
                            cleanUpPaths.get(idx)));
                }
                idx--;
            }
//            endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tdbListChildrenAssertsTestManager(DataLayerMgrBase manager)
                                                            throws IOException, InvalidServiceStateException {
        System.out.println("Testing tdbListChildrenAssertsTestManager:");
        ArrayList<String> cleanUpPaths = new ArrayList<>();
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);
        cleanUpPaths.add(path);

//            Thread.sleep(2500);
        int childId = 1;

        while (childId <= 5) {
            cmdStr = String.format("create %s %s", path+"/"+childId, generatedString);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);
            cleanUpPaths.add(path+"/"+childId);
            childId++;
        }

        cmdStr = String.format("ls -L %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
//
////            Thread.sleep(1000);
        CommandOutput output = ((LsCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(
                    String.format("CmdLs Output Cache [output=\n%s]",
                            output.getChildrenSet().toString()));
        }
        for (String childPath : output.getChildrenSet()) {
            // TODO: Consider making cleanup Paths a set
            assert (cleanUpPaths.contains(testRootPath+"/"+childPath));
        }

        // NOTE: Clean up
        int idx = cleanUpPaths.size()-1;
        while (idx >= 0) {
            output = DeleteCommand.generateDeleteCommand(cleanUpPaths.get(idx))
                    .executeManager(manager);
            try {
                assert (output.getErrorCode() == 0);
            } catch (AssertionError e) {
                log.error(String.format("Test Error - rdbListChildrenAssertsTest - Failed cleanup [path=%s]",
                        cleanUpPaths.get(idx)));
            }
            idx--;
        }
    }

    public static void tbdCacheAssertsTestManager(DataLayerMgrBase manager)
            throws IOException, InvalidServiceStateException {
        System.out.println("Testing tbdCacheAssertsTestManager:");
        CSvcSimpleLRUCache<String, LogRecord> cache = new CSvcSimpleLRUCache<>(15);
        manager.enableCacheLayer(cache);
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String generatedStringSet = generatedString+"2";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

        cmdStr = String.format("get -c %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output Cache [output=\n%s]", output.getOutput()));
        }
        output = GetCommand.generateGetCommand(path).executeManager(manager);
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        cmdStr = String.format("set %s %s", path, generatedStringSet);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((SetCommand) cmd).executeManager(manager);

        cmdStr = String.format("get -c %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output Cache 2 [output=\n%s]", output.getOutput()));
        }
        assert (generatedStringSet.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        assert (output.getErrorCode() == 0);
    }

    public static void rdbCacheAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbCacheTest:");
//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            CSvcSimpleLRUCache<String, LogRecord> cache = new CSvcSimpleLRUCache<>(15);
            manager.enableCacheLayer(cache);
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String generatedStringSet = generatedString+"2";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(2500);

            cmdStr = String.format("get -c %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
//
////            Thread.sleep(1000);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output Cache [output=\n%s]", output.getOutput()));
            }
            output = GetCommand.generateGetCommand(path).executeManager(manager);
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            cmdStr = String.format("set %s %s", path, generatedStringSet);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//            CommandOutput output = ((SetCommand) cmd).executeManager(manager);
            output = ((SetCommand) cmd).executeManager(manager);

            cmdStr = String.format("get -c %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
//            parser = new CSvcSandboxParser();
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
//
////            Thread.sleep(1000);
            output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output Cache 2 [output=\n%s]", output.getOutput()));
            }
            assert (generatedStringSet.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            // NOTE: Clean up
            output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
            assert (output.getErrorCode() == 0);
//            endlessLoop();
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void rdbSetTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbSetTest:");

        try (DataLayerMgrRDB manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

            Thread.sleep(2500);

            cmdStr = String.format("get -w %s", path);
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
//
////            Thread.sleep(1000);
            ((GetCommand) cmd).setWatchCallable((watchManager, commandOutput)
                    -> System.out.println("===\n"+"commandOutput.getOutput()"+"\n==="), false);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));


            cmdStr = String.format("set %s %s", path, generatedString+"2");
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//            CommandOutput output = ((SetCommand) cmd).executeManager(manager);
            output = ((SetCommand) cmd).executeManager(manager);

            endlessLoop();
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void endlessLoop() {
        while (true) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void sandboxDBTest() throws IOException, ClassNotFoundException, InvalidServiceStateException {
        System.out.println("Testing sandboxDBTest:");
        DataLayerMgrRDB manager = new DataLayerMgrRDB(null, "/");
        String generatedString = RandomStringUtils.randomAlphanumeric(1000);

        StopWatch watch = new StopWatch();
        watch.start();
        manager.testWrite(generatedString.getBytes(StandardCharsets.UTF_8));
        manager.testWrite(generatedString.getBytes(StandardCharsets.UTF_8));
//        manager.testParallelCommit();
        watch.stop();
        System.out.println(String.format("sandboxDBTest - Test complete [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
        //===
//        CSvcManagerAmazonDB_Sandbox manager = new CSvcManagerAmazonDB_Sandbox(null);
//        CSvcSandboxParser parser = new CSvcSandboxParser();
//        parser.parseCommand("create /databaseTest contents");
////            CSvcCommand cmd = parser.processCommand();
//        CSvcCommand cmd = CreateCommand.generateCreateCommand("/databaseTest", "contents");
//        System.out.println(cmd.getCmdStr());
//        System.out.println(cmd.getArgCount());
//        cmd.setManager(manager);
//        cmd.executeManager();
//        Thread.sleep(250);
//        parser.parseCommand("get /databaseTest");
////            cmd = parser.processCommand();
//        cmd = GetCommand.generateGetCommand("/databaseTest");
//        System.out.println(cmd.getCmdStr());
//        System.out.println(cmd.getArgCount());
//        cmd.setManager(manager);
//        cmd.executeManager();
//            CreateCommand createcmd = new CreateCommand();
        //====
        // NON-DB BASELINE
//        CSvcManagerFileSystem_CmdAgnostic manager = new CSvcManagerFileSystem_CmdAgnostic("ec2-44-198-61-232.compute-1.amazonaws.com",
//                8001, "./root_path", Paths.get("./root_path/csvc.log"), "/");
//
//        String testId = "idTest";
//        String testKey = "key1";
//        String testValue = "value1";
//
//        manager.createMappingStructure(testId, String.class, String.class);
//        manager.addToMappingStructure(testId, testKey, testValue);
//        String resultValue = manager.getFromMappingStructure(testId, testKey, String.class);
//        System.out.println(String.format("[identifier=%s, key=%s, value=%s]", testId, testKey,resultValue));
//        manager.deleteFromMappingStructure(testKey, testId);
//        resultValue = manager.getFromMappingStructure(testId, testKey, String.class);
//        System.out.println(String.format("[identifier=%s, key=%s, value=%s]", testId, testKey, Objects.isNull(resultValue) ? "null" : resultValue));
    }

    public static void sandboxParserTest() {
        // NOTE: Compare parser output with statically generated command instance
        CSvcSandboxParser parser = new CSvcSandboxParser();
//            parser.parseCommand("create /databaseTest contents");
////            CSvcCommand cmd = parser.processCommand();
//            CSvcCommand cmd = CreateCommand.generateCreateCommand("/databaseTest", "contents");
        String cmdStr = "create -e /databaseTest contents";
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        System.out.println(String.format("cmd.CmdStr=%s", cmd.getCmdStr()));
        System.out.println(String.format("cmd.getArgCount=%d", cmd.getArgCount()));
        System.out.println(String.format("cmd=%s", cmd));
//            System.out.println(cmd.getArgCount());
        // TODO: refactor manager population up to the parser level so
        //  that all parsed command instances have that manager
        CreateCommand crtCmd = CreateCommand.generateCreateCommand("/databaseTest", "contents");
//        cmd.setManager(manager);
        System.out.println(String.format("crtCmd.toStringArgs=%s", crtCmd.toStringArgs()));
    }

    public static void sandboxRDBNoParserTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing sandboxRDBTest:");
        DataLayerMgrRDB manager;
        try {
            manager = new DataLayerMgrRDB(null, "/");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        CreateCommand cmdCrt =
                CreateCommand.generateCreateCommand(path, generatedString);

        cmdCrt.executeManager(manager);

        GetCommand cmdGet = null;
//        try {
        cmdGet = GetCommand.generateGetCommand(path);
//        } catch (ParseException e) {
//            throw new RuntimeException(e);
//        }
        CommandOutput output = cmdGet.executeManager(manager);
        System.out.println(String.format("CmdGet Output [output=%s]", output.getOutput()));

    }

    public static void tbdEphemeralChildAssertsTestManager(DataLayerMgrBase manager)
            throws InvalidServiceStateException, IOException {
        System.out.println("Testing rdbEphemeralChildTest:");

        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create -e %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

        cmdStr = String.format("get -s %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
        assert (output.getRecordStats().getEphemeralOwnerId() != 0);

        path = String.format("%s/%s", path, "badChild");
        cmdStr = String.format("create %s %s", path, generatedString);
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        // NOTE: Expected failed create under ephemeral node
        output = ((CreateCommand) cmd).executeManager(manager);
        assert (output.getErrorCode() == 1);

        cmdStr = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        // NOTE: Expected failed create under ephemeral node
        output = ((GetCommand) cmd).executeManager(manager);
        assert (output.getErrorCode() == 1);

        // NOTE: Clean Up
        output = DeleteCommand.generateDeleteCommand(testRootPath).executeManager(manager);
        assert (output.getErrorCode() == 0);
    }

    public static void rdbEphemeralChildAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbEphemeralChildTest:");

//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create -e %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);
//            Thread.sleep(1000);

            cmdStr = String.format("get -s %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
            assert (output.getRecordStats().getEphemeralOwnerId() != 0);

            path = String.format("%s/%s", path, "badChild");
            cmdStr = String.format("create %s %s", path, generatedString);
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
//            cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            // NOTE: Expected failed create under ephemeral node
            output = ((CreateCommand) cmd).executeManager(manager);
            assert (output.getErrorCode() == 1);

            cmdStr = String.format("get %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            // NOTE: Expected failed create under ephemeral node
            output = ((GetCommand) cmd).executeManager(manager);
            assert (output.getErrorCode() == 1);

            // NOTE: Clean Up
            output = DeleteCommand.generateDeleteCommand(testRootPath).executeManager(manager);
            assert (output.getErrorCode() == 0);
//            Thread.sleep(1000);
//            endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tbdAddRemoveWatchTestManager(DataLayerMgrBase manager)
            throws IOException, InvalidServiceStateException, InterruptedException {
        System.out.println("Testing tbdAddWatchTestManager:");
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

        cmdStr = String.format("get -s %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // TODO: Add addwatch command
        boolean[] watchTriggerFlag = {false};
        AddWatchCommand.generateAddWatchCommand(testRootPath, WatchedEvent.WatchType.CHILDREN,
                (watchManager, commandOutput) -> watchTriggerFlag[0] = true, true).executeManager(manager);

        CreateCommand.generateCreateCommand(testRootPath.concat("/child1"),
                        generatedString.getBytes(StandardCharsets.UTF_8), false, false,
                        true).executeManager(manager);
//        manager.close();
        Thread.sleep(500);
        assert (watchTriggerFlag[0]);

        // TODO: Test persist
        watchTriggerFlag[0] = false;
        CreateCommand.generateCreateCommand(testRootPath.concat("/child2"),
                generatedString.getBytes(StandardCharsets.UTF_8), false, false,
                true).executeManager(manager);
        Thread.sleep(500);
        assert (watchTriggerFlag[0]);

        // TODO: Test remove
        RemoveWatchesCommand.generateRemoveWatchCommand(testRootPath,
                                                        WatchedEvent.WatchType.CHILDREN).executeManager(manager);
        watchTriggerFlag[0] = false;
        CreateCommand.generateCreateCommand(testRootPath.concat("/child3"),
                generatedString.getBytes(StandardCharsets.UTF_8), false, false,
                true).executeManager(manager);
        Thread.sleep(500);
        assert (!watchTriggerFlag[0]);
    }

    public static void tbdFSLogRecoveryTest()
            throws IOException, InvalidServiceStateException, InterruptedException {
        try (DataLayerMgrSharedFS manager =
                     new DataLayerMgrSharedFS("/mnt/efs/fs1/CSVC_TEST", "/")) {
            System.out.println("Testing tbdFSLogRecoveryTest:");
            // TODO: Use pre-made log?
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

            cmdStr = String.format("get -s %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - tbdFSLogRecoveryTestPrep [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            FSTestHelper.tbdFSCopyLogToTemp(manager);
            int retryCount = 0;
            int retryMax = 5;
            path = "/redesignTest/node1";
            CreateCommand cmdCrt = CreateCommand.generateCreateCommand(path, "contents");
            while (retryCount < retryMax) {
                retryCount++;
                try {
                    output = cmdCrt.executeManager(manager);
                    break;
                } catch (Exception e) {
                    if (TESTS_DEBUG_FLAG) {
                        System.out.println("DEBUG - tbdFSLogRecoveryTestPrep - Waiting for log recovery");
                    }
                }
            }
            assert (output.getErrorCode() == 0);

            cmdStr = String.format("get -s %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - tbdFSLogRecoveryTestPrep [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
        }
    }

    public static void tbdEphemeralCleanupTestManager(DataLayerMgrBase manager, DataLayerMgrBase ephemTestManager)
            throws IOException, InvalidServiceStateException, InterruptedException {
        System.out.println("Testing tbdEphemeralCleanupTestManager:");

        ephemTestManager.startManager();
        Thread.sleep(150);
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create -e %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

        cmdStr = String.format("get -s %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
        manager.close();
        Thread.sleep(8000);

        cmdStr = String.format("get -s %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((GetCommand) cmd).executeManager(ephemTestManager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (output.getErrorCode() == 1);
    }

    public static void tbdEphemeralAssertsTestManager(DataLayerMgrBase manager)
                                                        throws IOException, InvalidServiceStateException {
        System.out.println("Testing tbdEphemeralAssertsTestManager:");
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create -e %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

        cmdStr = String.format("get -s %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // NOTE: Attempt to create a child-node under ephemeral node
        cmdStr = String.format("create %s %s", path+"/child", generatedString);
        tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((CreateCommand) cmd).executeManager(manager);
        assert (output.getErrorCode() == 1);

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        assert (output.getErrorCode() == 0);
    }

    public static void rdbEphemeralAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbEphemeralTest:");
//        HashSet<String> testHashSetStr = new HashSet<>();
//        HashSet<Integer> testHashSetInt = new HashSet<>();
//        Class<?> type = testHashSetStr.getClass();
//        System.out.println(String.format("test hashset class: %s", testHashSetStr.getClass()));
//        System.out.println(String.format("hashsetstr instaceof hashsetint: %b", testHashSetInt.getClass().equals(type)));
//        return;
//        CSvcManagerFileSystem_CmdAgnostic manager = new CSvcManagerFileSystem_CmdAgnostic("ec2-44-198-61-232.compute-1.amazonaws.com",
//                8001, "/mnt/efs/fs1/", Paths.get("/mnt/efs/fs1/csvc.log"), "/");
//        CSvcManagerAmazonDB_CmdAgnostic manager;
//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
//            manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/");
            manager.startManager();

//        String testId = "idTest";
//        String testKey = "key1";
//        String testValue = "value1";

//        String generatedString = RandomStringUtils.randomAlphanumeric(1000);
            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
//        CreateCommand cmdCrt =
//                CreateCommand.generateCreateCommand(path, generatedString);
            String cmdStr = String.format("create -e %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
//        System.out.println(String.format("cmd.CmdStr=%s", cmd.getCmdStr()));
//        System.out.println(String.format("cmd.getArgCount=%d", cmd.getArgCount()));
//        System.out.println(String.format("cmd=%s", cmd));

//        cmdCrt.executeManager(manager);
            ((CreateCommand) cmd).executeManager(manager);
//            Thread.sleep(1000);

            cmdStr = String.format("get -s %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//        GetCommand cmdGet = GetCommand.generateGetCommand(path);
//        CommandOutput output = cmdGet.executeManager(manager);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            // NOTE: Attempt to create a child-node under ephemeral node
            cmdStr = String.format("create %s %s", path+"/child", generatedString);
            tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
//        System.out.println(String.format("cmd.CmdStr=%s", cmd.getCmdStr()));
//        System.out.println(String.format("cmd.getArgCount=%d", cmd.getArgCount()));
//        System.out.println(String.format("cmd=%s", cmd));

//        cmdCrt.executeManager(manager);
            output = ((CreateCommand) cmd).executeManager(manager);
            assert (output.getErrorCode() == 1);

            // NOTE: Clean up
            output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
            assert (output.getErrorCode() == 0);
//            endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void insertWaitRDBAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing insertWaitRDBTest:");
        try (DataLayerMgrRDB manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//            ((CreateCommand) cmd).executeManagerSlowTest(manager);
            ((CreateCommand) cmd).executeManagerSlowTest(manager);

            cmdStr = String.format("get %s", path);
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            // NOTE: Clean up
            DeleteCommand.generateDeleteCommand(testRootPath).executeManager(manager);
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void insertWaitRDBTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing insertWaitRDBTest:");
//        HashSet<String> testHashSetStr = new HashSet<>();
//        HashSet<Integer> testHashSetInt = new HashSet<>();
//        Class<?> type = testHashSetStr.getClass();
//        System.out.println(String.format("test hashset class: %s", testHashSetStr.getClass()));
//        System.out.println(String.format("hashsetstr instaceof hashsetint: %b", testHashSetInt.getClass().equals(type)));
//        return;
//        CSvcManagerFileSystem_CmdAgnostic manager = new CSvcManagerFileSystem_CmdAgnostic("ec2-44-198-61-232.compute-1.amazonaws.com",
//                8001, "/mnt/efs/fs1/", Paths.get("/mnt/efs/fs1/csvc.log"), "/");
//        CSvcManagerAmazonDB_CmdAgnostic manager;
        try (DataLayerMgrRDB manager = new DataLayerMgrRDB(null, "/")) {
//            manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/");
            manager.startManager();

//        String testId = "idTest";
//        String testKey = "key1";
//        String testValue = "value1";

//        String generatedString = RandomStringUtils.randomAlphanumeric(1000);
            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
//        CreateCommand cmdCrt =
//                CreateCommand.generateCreateCommand(path, generatedString);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
//        System.out.println(String.format("cmd.CmdStr=%s", cmd.getCmdStr()));
//        System.out.println(String.format("cmd.getArgCount=%d", cmd.getArgCount()));
//        System.out.println(String.format("cmd=%s", cmd));

//        cmdCrt.executeManager(manager);
            ((CreateCommand) cmd).executeManagerSlowTest(manager);

//        cmdStr = String.format("get %s", path);
//        System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
//        tokens = cmdStr.split(" ");
//        parser = new CSvcSandboxParser();
//        parser.parseOptions(tokens);
//        try {
//            cmd = parser.processCommand();
//        } catch (ParseException e) {
//            throw new RuntimeException(e);
//        }
//
////        GetCommand cmdGet = GetCommand.generateGetCommand(path);
////        CommandOutput output = cmdGet.executeManager(manager);
//        CommandOutput output = ((GetCommand)cmd).executeManager(manager);
//        System.out.println(String.format("CmdGet Output [output=%s]", output.getOutput()));
//        endlessLoop();
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tbdCleanUpManager(int managerType) {
        try (DataLayerMgrBase manager = tbdInitializeManagerHelper(managerType)) {
            if (manager instanceof DataLayerMgrSharedFS) {
                ((DataLayerMgrSharedFS)manager).tbdFSCleanup();
            } else if (manager instanceof DataLayerMgrRDB) {
                try {
                    rdbCleanUp();
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new NotImplementedException("Manager Type Not Implemented");
            }
        } catch (InvalidServiceStateException | IOException
                 | ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void rdbCleanUp() throws IOException, ClassNotFoundException {
//        System.out.println("Testing rdbCleanUp:");
        Properties properties;
        Class.forName("org.postgresql.Driver");
        Class.forName("software.amazon.jdbc.Driver");
        properties = new Properties();
        final String USERNAME = "csvcadmin";
        final String PASSWORD = "!123Csvc";

        // Configuring connection properties for the underlying JDBC driver.
        properties.setProperty("user", USERNAME);
        properties.setProperty("password", PASSWORD);
        properties.setProperty("loginTimeout", "100");

        // Configuring connection properties for the JDBC Wrapper.
        properties.setProperty("wrapperPlugins", "failover,efm");
        properties.setProperty("wrapperLogUnclosedConnections", "true");
        DataLayerMgrRDB.testCleanup();
    }

    public static void tbdDeleteAssertsTestManager(DataLayerMgrBase manager) throws IOException, InvalidServiceStateException, InterruptedException {
        System.out.println("Testing tbdDeleteAssertsTestManager:");
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        CommandOutput output;
        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        ((CreateCommand) cmd).executeManager(manager);

        cmdStr = String.format("create %s %s", path+"/node", generatedString);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((CreateCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(
                    String.format("CmdCreate Output [output=\n%s]",
                            output.toString()));
        }
        cmdStr = String.format("delete %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((DeleteCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            // NOTE: Expected Failure
            System.out.println(
                    String.format("DeleteCommand Output [output=\n%s]",
                            output.toString()));
        }
        assert (output.getErrorCode() == 1);

        cmdStr = String.format("delete %s", path+"/node");
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((DeleteCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            // NOTE: Child deleted
            System.out.println(
                    String.format("DeleteCommand Output [output=\n%s]",
                            output.toString()));
        }
        assert (output.getErrorCode() == 0);

        cmdStr = String.format("delete -L %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        output = ((DeleteCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            // NOTE: Properly delete
            System.out.println(
                    String.format("DeleteCommand Output [output=\n%s]",
                            output.toString()));
        }
        assert (output.getErrorCode() == 0);

        Thread.sleep(500);

        cmdStr = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        // NOTE: Expected missing node
        output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("GetCommand Output [output=\n%s]", output.getOutput()));
        }
        assert (output.getErrorCode() == 1);
    }

    public static void rdbDeleteAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing rdbDeleteTest:");
//        try (CSvcManagerAmazonDB_CmdAgnostic manager =
//                     new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager =
                     new DataLayerMgrRDB(null, "/")) {
//            DebugConstants.LOGREADER_FLAG = false;
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            CommandOutput output;
            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            ((CreateCommand) cmd).executeManager(manager);

            cmdStr = String.format("create %s %s", path+"/node", generatedString);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            output = ((CreateCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(
                        String.format("CmdCreate Output [output=\n%s]",
                                output.toString()));
            }
            cmdStr = String.format("delete %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            output = ((DeleteCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                // NOTE: Expected Failure
                System.out.println(
                        String.format("DeleteCommand Output [output=\n%s]",
                                output.toString()));
            }
            assert (output.getErrorCode() == 1);

            cmdStr = String.format("delete %s", path+"/node");
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            output = ((DeleteCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                // NOTE: Child deleted
                System.out.println(
                        String.format("DeleteCommand Output [output=\n%s]",
                                output.toString()));
            }
            assert (output.getErrorCode() == 0);

            cmdStr = String.format("delete -L %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            output = ((DeleteCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                // NOTE: Properly delete
                System.out.println(
                        String.format("DeleteCommand Output [output=\n%s]",
                                output.toString()));
            }
            assert (output.getErrorCode() == 0);

            Thread.sleep(500);

            cmdStr = String.format("get %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            // NOTE: Expected missing node
            output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("GetCommand Output [output=\n%s]", output.getOutput()));
            }
            assert (output.getErrorCode() == 1);
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tbdCreateGetNoWaitAssertsTestManager(DataLayerMgrBase manager)
                                                                    throws IOException, InvalidServiceStateException {
        System.out.println("Testing tbdCreateGetNoWaitAssertsTestManager:");
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(3500);

        cmdStr = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

//        GetCommand cmdGet = GetCommand.generateGetCommand(path);
//        CommandOutput output = cmdGet.executeManager(manager);
//            Thread.sleep(1000);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        output = GetCommand.generateGetCommand(path).executeManager(manager);
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        assert (output.getErrorCode() == 0);
    }

    public static void rdbCreateGetNoWaitAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing sandboxRDBTest:");
//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(3500);

            cmdStr = String.format("get %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//        GetCommand cmdGet = GetCommand.generateGetCommand(path);
//        CommandOutput output = cmdGet.executeManager(manager);
//            Thread.sleep(1000);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            output = GetCommand.generateGetCommand(path).executeManager(manager);
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            // NOTE: Clean up
            output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
            assert (output.getErrorCode() == 0);
//        endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tbdBasicInitStartTestManager(DataLayerMgrBase manager)
                                                        throws IOException, InvalidServiceStateException {
        System.out.println("Testing tbdBasicStartAssertsTestManager:");
        manager.startManager();
        endlessLoop();
    }

    public static void tbdBasicCreateAssertsTestManager(DataLayerMgrBase manager)
                                                            throws IOException, InvalidServiceStateException {
        System.out.println("Testing tbdBasicCreateAssertsTestManager:");
        manager.startManager();

        CSvcSandboxParser parser = new CSvcSandboxParser();

        String generatedString = "contents";
        String testRootPath = "/redesignTest";
        String path = String.format("%s", testRootPath);
        String cmdStr = String.format("create %s %s", path, generatedString);
        String[] tokens = cmdStr.split(" ");
        parser.parseOptions(tokens);
        CSvcCommand cmd = null;
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);

        }
        ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(3500);

        cmdStr = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
        }
        tokens = cmdStr.split(" ");
        parser.resetParser();
        parser.parseOptions(tokens);
        try {
            cmd = parser.processCommand();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

//            Thread.sleep(1000);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
    }

    public static void rdbBasicCreateAssertsTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing sandboxRDBTest:");
//        try (CSvcManagerAmazonDB_CmdAgnostic manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/")) {
        try (DataLayerMgrBase manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);

            }
            ((CreateCommand) cmd).executeManager(manager);

//            Thread.sleep(3500);

            cmdStr = String.format("get %s", path);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            }
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

//            Thread.sleep(1000);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            assert (generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
//        endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public static void basicRDBTest() throws IOException, InvalidServiceStateException {
        log.info("Testing sandboxRDBTest:");
        try (DataLayerMgrRDB manager = new DataLayerMgrRDB(null, "/")) {
            manager.startManager();

            CSvcSandboxParser parser = new CSvcSandboxParser();

            String generatedString = "contents";
            String testRootPath = "/redesignTest";
            String path = String.format("%s", testRootPath);
            String cmdStr = String.format("create %s %s", path, generatedString);
            String[] tokens = cmdStr.split(" ");
            parser.parseOptions(tokens);
            CSvcCommand cmd = null;
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            ((CreateCommand) cmd).executeManager(manager);

            Thread.sleep(3500);

            cmdStr = String.format("get %s", path);
            System.out.println(String.format("DEBUG - Test [cmd=%s]", cmdStr));
            tokens = cmdStr.split(" ");
            parser.resetParser();
            parser.parseOptions(tokens);
            try {
                cmd = parser.processCommand();
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }

            Thread.sleep(1000);
            CommandOutput output = ((GetCommand) cmd).executeManager(manager);
            System.out.println(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void sandboxDBPerformanceTest() throws IOException, InvalidServiceStateException {
        System.out.println("Testing sandboxDBPerformanceTest:");
        String generatedString = RandomStringUtils.randomAlphanumeric(1000);

//        HashSet<String> testHashSetStr = new HashSet<>();
//        HashSet<Integer> testHashSetInt = new HashSet<>();
//        Class<?> type = testHashSetStr.getClass();
//        System.out.println(String.format("test hashset class: %s", testHashSetStr.getClass()));
//        System.out.println(String.format("hashsetstr instaceof hashsetint: %b", testHashSetInt.getClass().equals(type)));
//        return;
//        CSvcManagerAmazonDB_CmdAgnostic manager = null;
        try (DataLayerMgrRDB manager = new DataLayerMgrRDB(null, "/")) {
//            manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/");
            //        CSvcManagerFileSystem_CmdAgnostic manager = new CSvcManagerFileSystem_CmdAgnostic("ec2-44-198-61-232.compute-1.amazonaws.com",
//                8001, "./root_path", Paths.get("./root_path/csvc.log"), "/");
            manager.startManager();

//        String testId;
//        String testKey;
//        String testValue;

//        testId = "idTest";
//        testKey = "key1";
//        testValue = "value1";
            String testRootPath = "/dbPerformanceTest";
            String path = String.format("%s", testRootPath);
            GetCommand cmdGet;
            CommandOutput output;

            StopWatch watch = new StopWatch();
//        StopWatch watchSection = new StopWatch();
            watch.start();
            CreateCommand cmdCrt =
                    CreateCommand.generateCreateCommand(path, generatedString);
//        cmdCrt.executeManager(manager);
            cmdCrt.executeManager(manager);
//        executeManagerOneConnection

            // Create 20 subnodes with 20 children each
            int i;
            int j;
            for (i = 0; i < 20; i++) {
                path = String.format("%s/%d", testRootPath, i);
                cmdCrt = CreateCommand.generateCreateCommand(path, generatedString + i + "_");
//            cmdCrt.executeManager(manager);
                cmdCrt.executeManager(manager);
//                    Thread.sleep(50);
                for (j = 0; j < 20; j++) {
                    path = String.format("%s/%d/%d", testRootPath, i, j);
                    cmdCrt = CreateCommand.generateCreateCommand(path, generatedString + i + "_" + j);
//                cmdCrt.executeManager(manager);
                    cmdCrt.executeManager(manager);
                }
            }
            watch.stop();
            System.out.println(String.format("sandboxDBPerformanceTest - Test complete [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
            // TODO: add a validation step that reads back the contents.
            //  Add to (salt) each generated string with per node suffix
            for (i = 0; i < 20; i++) {
                path = String.format("%s/%d", testRootPath, i);
//            cmdCrt = dsg.tbd.modular.messaging.CreateCommand.generateCreateCommand(path, generatedString+i+"_");
                cmdGet = GetCommand.generateGetCommand(path);
//            cmdCrt.executeManager(manager);
                output = cmdGet.executeManager(manager);
//                    Thread.sleep(50);
                if (!(new String(output.getData(), StandardCharsets.UTF_8).equals(generatedString + i + "_"))) {
                    System.out.println(String.format("sandboxDBPerformanceTest - Data validation failed [" +
                                    "output=%s, expectedOutput=%s]",
                            new String(output.getData(), StandardCharsets.UTF_8), generatedString + i + "_"));
                    throw new AssertionError("Failed Test");
                }
                for (j = 0; j < 20; j++) {
                    path = String.format("%s/%d/%d", testRootPath, i, j);
                    cmdGet = GetCommand.generateGetCommand(path);
//                cmdCrt = dsg.tbd.modular.messaging.CreateCommand.generateCreateCommand(path, generatedString+i+"_"+j);
//                cmdCrt.executeManager(manager);
                    output = cmdGet.executeManager(manager);
//                    Thread.sleep(50);
                    if (!output.getOutput().equals(generatedString + i + "_" + j)) {
                        System.out.println(String.format("sandboxDBPerformanceTest - Data validation failed [" +
                                        "output=%s, expectedOutput=%s]",
                                output.getOutput(), generatedString + i + "_" + j));
                        break;
                    }
                }
            }
//        manager.disableRunningFlag();
//        System.out.println(String.format("sandboxDBPerformanceTest - disable running flag [isRunning=%s]",
//                manager.isRunning()));
        } catch (ClassNotFoundException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
}