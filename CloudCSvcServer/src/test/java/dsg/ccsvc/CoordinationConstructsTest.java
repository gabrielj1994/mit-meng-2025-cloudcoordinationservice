package dsg.ccsvc;

import dsg.ccsvc.caching.CSvcSimpleLRUCache;
import dsg.ccsvc.client.CSvcSandboxParser;
import dsg.ccsvc.command.*;
import dsg.ccsvc.datalayer.*;
import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.util.DebugConstants;
import dsg.ccsvc.watch.WatchedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.*;

import static dsg.ccsvc.util.DebugConstants.TESTS_DEBUG_FLAG;

@Slf4j
public class CoordinationConstructsTest {

    private static String TEST_FLAG_DEBUG = "test.debug.enable";
    private static String TEST_FLAG_METRICS = "test.metrics.enable";
    private static String TEST_PARAM_MANAGER_TYPE = "test.manager.type";

    private static int managerType = -1;

    private static DataLayerMgrBase manager;

    private static DataLayerMgrBase tbdInitializeManagerHelper(int managerType)
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

    private static void rdbCleanUp() throws IOException, ClassNotFoundException {
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

    private static void tbdCleanUpManager(int managerType) {
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


    @BeforeAll
    static void setUpAll() {
        Logger log = LoggerFactory.getLogger(CoordinationConstructsTest.class);
        log.info("SLF4J is using: " + log.getClass().getName());

        DebugConstants.DEBUG_FLAG = Boolean.getBoolean(TEST_FLAG_DEBUG) || DebugConstants.DEBUG_FLAG;
        DebugConstants.METRICS_FLAG = Boolean.getBoolean(TEST_FLAG_METRICS) || DebugConstants.METRICS_FLAG;

        if (DebugConstants.DEBUG_FLAG) {
            log.info("SandboxUserApp - DEBUG FLAG ON");
            Configurator.setRootLevel(Level.DEBUG);
        }

        DebugConstants.LOGREADER_FLAG = true;

        Integer managerTypeParam = Integer.getInteger(TEST_PARAM_MANAGER_TYPE);
        if (!Objects.isNull(managerTypeParam)) {
            managerType = managerTypeParam;
        }
    }

    @BeforeEach
    void setUpEach(TestInfo testInfo) {
        log.info("==============\nRunning test: "
                + testInfo.getDisplayName() + "\n==============");
        if (managerType >= 0) {
            tbdCleanUpManager(managerType);
            try {
                manager = tbdInitializeManagerHelper(managerType);
            } catch (InvalidServiceStateException | IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @AfterEach
    void cleanUpEach(TestInfo testInfo) {
        log.info("==============\nFinished test: "
                + testInfo.getDisplayName() + "\n==============");
        if (!Objects.isNull(manager)) {
            try {
                manager.close();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    public void basicCreateAssertsTest()
            throws IOException, InvalidServiceStateException {
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

        cmdStr = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertEquals(generatedString, new String(output.getData(), StandardCharsets.UTF_8));
    }

    @Test
    public void deleteAssertsTest() throws IOException, InvalidServiceStateException, InterruptedException {
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(
                    String.format("CmdCreate Output [output=\n%s]",
                            output.toString()));
        }
        cmdStr = String.format("delete %s", path);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(
                    String.format("DeleteCommand Output [output=\n%s]",
                            output.toString()));
        }
        Assertions.assertTrue(output.getErrorCode() == 1);

        cmdStr = String.format("delete %s", path+"/node");
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(
                    String.format("DeleteCommand Output [output=\n%s]",
                            output.toString()));
        }
        Assertions.assertTrue(output.getErrorCode() == 0);

        cmdStr = String.format("delete -L %s", path);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(
                    String.format("DeleteCommand Output [output=\n%s]",
                            output.toString()));
        }
        Assertions.assertTrue(output.getErrorCode() == 0);

        Thread.sleep(500);

        cmdStr = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("GetCommand Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(output.getErrorCode() == 1);
    }

    @Test
    public void ephemeralAssertsTest()
            throws IOException, InvalidServiceStateException {
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

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
        Assertions.assertTrue(output.getErrorCode() == 1);

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        Assertions.assertTrue(output.getErrorCode() == 0);
    }

    @Test
    public void ephemeralChildAssertsTest()
            throws InvalidServiceStateException, IOException {
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
        Assertions.assertTrue(output.getRecordStats().getEphemeralOwnerId() != 0);

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
        Assertions.assertTrue(output.getErrorCode() == 1);

        cmdStr = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
        Assertions.assertTrue(output.getErrorCode() == 1);

        // NOTE: Clean Up
        output = DeleteCommand.generateDeleteCommand(testRootPath).executeManager(manager);
        Assertions.assertTrue(output.getErrorCode() == 0);
    }
    // NOTE: COME HERE
    @Test
    public void simpleWatchAssertsTest()
            throws IOException, InvalidServiceStateException, InterruptedException {

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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.info("===\n"
                    +"commandOutput.getOutput()"
                    +"\n===\n"
                    +commandOutput.toString()
                    +"\n===");
            watchFlag[0] = true;
        }, false);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        generatedString = generatedString+"2";
        cmdStr = String.format("set %s %s", path, generatedString);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
        Assertions.assertTrue(watchFlag[0]);

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        Assertions.assertTrue(output.getErrorCode() == 0);
//            endlessLoop();
//        } catch (ClassNotFoundException | InterruptedException e) {
    }

    @Test
    public void complexWatchSetAssertsTest()
            throws IOException, InvalidServiceStateException, InterruptedException {
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
            log.info("===\n"
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
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
        Assertions.assertTrue(generatedStringSet.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        Thread.sleep(500);

        output = postWatchCmd[0].executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            log.debug("===\n"
                    + "Post Watch Set Output\n===\n"
                    + output
                    + "\n===");
        }
        output = GetCommand.generateGetCommand(path).executeManager(manager);
        Assertions.assertTrue(generatedStringWatch.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        Assertions.assertTrue(output.getErrorCode() == 0);
    }

    @Test
    public void watchSetForcedFailVersionAssertsTest()
            throws IOException, InvalidServiceStateException, InterruptedException {
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
            log.info("===\n"
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
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));


        cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
//            Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        Thread.sleep(500);

        output = postWatchCmd[0].executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            log.debug("===\n"
                    + "Post Watch Set Output\n===\n"
                    + output
                    + "\n===");
        }
        Assertions.assertTrue(output.getErrorCode() == 1);

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        Assertions.assertTrue(output.getErrorCode() == 0);
    }

    @Test
    public void watchDeleteAssertsTest()
            throws IOException, InvalidServiceStateException, InterruptedException {
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
            log.info("===\n"
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
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));


        cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
        Assertions.assertTrue(generatedStringSet.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        Thread.sleep(500);

        output = postWatchCmd[0].executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            log.debug("===\n"
                    + "Post Watch Delete Output\n===\n"
                    + output
                    + "\n===");
        }
//            Thread.sleep(2500);

        cmdStr[0] = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
            log.debug(String.format("Post Delete Get Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(output.getErrorCode() == 1);
    }

    @Test
    public void watchDeleteInWatchAssertsTest()
            throws IOException, InvalidServiceStateException, InterruptedException {
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
            log.info("===\n"
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
                log.info("===\n"
                        +"Post Watch Delete Output\n===\n"
                        +watchOutput
                        +"\n===");
            } catch (ParseException | InvalidServiceStateException e) {
                throw new RuntimeException(e);
            }
        }, false);
        CommandOutput output = ((GetCommand) cmd).executeManager(manager);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        output = GetCommand.generateGetCommand(path).executeManager(manager);
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        cmdStr[0] = String.format("set %s %s", path, generatedStringSet);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
        Assertions.assertTrue(output.getErrorCode() == 0);

//            Thread.sleep(2500);
//            Thread.sleep(2500);

        cmdStr[0] = String.format("get %s", path);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
            log.debug(String.format("Post Delete Get Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(output.getErrorCode() == 1);
    }

    @Test
    public void createGetNoWaitAssertsTest()
            throws IOException, InvalidServiceStateException {
        log.debug("Testing tbdCreateGetNoWaitAssertsTestManager:");
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        output = GetCommand.generateGetCommand(path).executeManager(manager);
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        Assertions.assertTrue(output.getErrorCode() == 0);
    }

    @Test
    public void cacheAssertsTest()
            throws IOException, InvalidServiceStateException {
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("CmdGet Output Cache [output=\n%s]", output.getOutput()));
        }
        output = GetCommand.generateGetCommand(path).executeManager(manager);
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        cmdStr = String.format("set %s %s", path, generatedStringSet);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("CmdGet Output Cache 2 [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(generatedStringSet.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // NOTE: Clean up
        output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
        Assertions.assertTrue(output.getErrorCode() == 0);
    }

    @Test
    public void leaseAssertsTest()
            throws IOException, InvalidServiceStateException, InterruptedException {
        try (DataLayerMgrBase managerLeaseTester = tbdInitializeManagerHelper(managerType)) {
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
//            Assertions.assertTrue(!manager.claimSoftLease(false));
                while (count < 5) {
                    Assertions.assertTrue(!manager.claimSoftLease(false));
                    if (TESTS_DEBUG_FLAG) {
                        log.debug(String.format("DEBUG - Properly maintained lease [attempt=%d]", count));
                    }
                    Thread.sleep(sleepMS);
                    count++;
                }

            } catch (InterruptedException | ExecutionException e) {
                // NOTE: No-Op
                throw new InvalidServiceStateException(e);
            }

            count = 1;
            while (!manager.claimSoftLease(false) && count < 5) {
                Thread.sleep(sleepMS);
                count++;
            }
            Assertions.assertTrue(count<5);

            if (TESTS_DEBUG_FLAG) {
                log.debug(String.format("===\n===\nDEBUG - Worker manager claimed lease [attempt=%d]", count));
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
                log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
                log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            // NOTE: Clean up
            output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
            Assertions.assertTrue(output.getErrorCode() == 0);
        } catch (ClassNotFoundException e) {
            Assertions.fail();
        }
    }

    @Test
    public void leaseExtendAssertsTestManager()
            throws IOException, InvalidServiceStateException, InterruptedException {
        try (DataLayerMgrBase managerLeaseTester = tbdInitializeManagerHelper(managerType)) {
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
                        log.debug(String.format("DEBUG - Extend tester lease [attempt=%d]", count));
                    }
                } else {
                    leaseHandler.close();
                    leaseClosedFlag = true;
                }
                if (TESTS_DEBUG_FLAG) {
                    log.debug(String.format("DEBUG - Properly maintained lease [attempt=%d]", count));
                }
                Thread.sleep(sleepMS);
                count += 1;
            }
            if (!leaseClosedFlag) {
                leaseHandler.close();
            }
            Assertions.assertTrue(count >= 5);

            if (TESTS_DEBUG_FLAG) {
                log.debug(String.format("===\n===\nDEBUG - Worker manager claimed lease [attempt=%d]", count));
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
                log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr[0]));
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
                log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

            // NOTE: Clean up
            output = DeleteCommand.generateDeleteCommand(path).executeManager(manager);
            Assertions.assertTrue(output.getErrorCode() == 0);
        } catch (ClassNotFoundException e) {
            Assertions.fail();
        }
    }

    @Test
    public void listChildrenAssertsTest()
            throws IOException, InvalidServiceStateException {
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
                log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(
                    String.format("CmdLs Output Cache [output=\n%s]",
                            output.getChildrenSet().toString()));
        }
        for (String childPath : output.getChildrenSet()) {
            // TODO: Consider making cleanup Paths a set
            Assertions.assertTrue(cleanUpPaths.contains(testRootPath+"/"+childPath));
        }

        // NOTE: Clean up
        int idx = cleanUpPaths.size()-1;
        while (idx >= 0) {
            output = DeleteCommand.generateDeleteCommand(cleanUpPaths.get(idx))
                    .executeManager(manager);
            try {
                Assertions.assertTrue(output.getErrorCode() == 0);
            } catch (AssertionError e) {
                log.error(String.format("Test Error - rdbListChildrenAssertsTest - Failed cleanup [path=%s]",
                        cleanUpPaths.get(idx)));
            }
            idx--;
        }
    }

    @Test
    public void sequentialAssertsTest() throws IOException, InvalidServiceStateException {
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
                log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
                log.debug(
                        String.format("CmdCreate Output [output=\n%s]",
                                output.toString()));
            }
            childId++;
        }

        cmdStr = String.format("ls -L %s", path);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(
                    String.format("CmdLs Output [output=\n%s]",
                            output.getChildrenSet().toString()));
        }
        for (String childPath : output.getChildrenSet()) {
            // TODO: Consider making cleanup Paths a set
            Assertions.assertTrue(cleanUpPaths.contains(testRootPath+"/"+childPath));
        }

        // NOTE: Clean up
        int idx = cleanUpPaths.size()-1;
        while (idx >= 0) {
            output = DeleteCommand.generateDeleteCommand(cleanUpPaths.get(idx))
                    .executeManager(manager);
            try {
                Assertions.assertTrue(output.getErrorCode() == 0);
            } catch (AssertionError e) {
                log.error(String.format("Test Error - rdbSequentialAssertsTest - Failed cleanup [path=%s]",
                        cleanUpPaths.get(idx)));
            }
            idx--;
        }
    }

    @Test
    public void outstandingChangesAssertsTest()
            throws InvalidServiceStateException, IOException {
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
                log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
                log.debug(
                        String.format("CmdCreate Output [output=\n%s]",
                                output.toString()));
            }
            cleanUpPaths.add(output.getPath());
            childId++;
        }

        cmdStr = String.format("ls -L %s", path);
        if (TESTS_DEBUG_FLAG) {
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(
                    String.format("CmdLs Output [output=\n%s]",
                            output.getChildrenSet().toString()));
        }
        for (String childPath : output.getChildrenSet()) {
            output = GetCommand.generateGetCommand(testRootPath+"/"+childPath).executeManager(manager);
            if (TESTS_DEBUG_FLAG) {
                log.debug(
                        String.format("GetCommand Output [output=\n%s]",
                                output.toString()));
            }
            Assertions.assertTrue(cleanUpPaths.contains(testRootPath+"/"+childPath));
//                log.debug(
//                        String.format("GetCommand Output [\ngeneratedString=\n%s,\noutput=\n%s]",
//                                generatedString, new String(output.getData(), StandardCharsets.UTF_8)));
            Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
//                Assertions.assertTrue(generatedString.equals("Nonsense"));
//                Assertions.assertTrue(false);
        }

        // NOTE: Clean up
        int idx = cleanUpPaths.size()-1;
        while (idx >= 0) {
            output = DeleteCommand.generateDeleteCommand(cleanUpPaths.get(idx), true).executeManager(manager);
            try {
                Assertions.assertTrue(output.getErrorCode() == 0);
            } catch (AssertionError e) {
                log.error(String.format("Test Error - rdbOutstandingChangesAssertsTest " +
                                "- Failed cleanup [path=%s]",
                        cleanUpPaths.get(idx)));
            }
            idx--;
        }
    }

    @Test
    public void ephemeralCleanupTest()
            throws IOException, InvalidServiceStateException, InterruptedException {
        try (DataLayerMgrBase ephemTestManager = tbdInitializeManagerHelper(managerType)) {
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
                log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
                log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));
            manager.close();
            Thread.sleep(8000);

            cmdStr = String.format("get -s %s", path);
            if (TESTS_DEBUG_FLAG) {
                log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
                log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
            }
            Assertions.assertTrue(output.getErrorCode() == 1);
        } catch (ClassNotFoundException e) {
            Assertions.fail();
        }
    }

    @Test
    public void tbdAddRemoveWatchTestManager()
            throws IOException, InvalidServiceStateException, InterruptedException {
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
            log.debug(String.format("DEBUG - Test [cmd=%s]", cmdStr));
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
            log.debug(String.format("CmdGet Output [output=\n%s]", output.getOutput()));
        }
        Assertions.assertTrue(generatedString.equals(new String(output.getData(), StandardCharsets.UTF_8)));

        // TODO: Add addwatch command
        boolean[] watchTriggerFlag = {false};
        AddWatchCommand.generateAddWatchCommand(testRootPath, WatchedEvent.WatchType.CHILDREN,
                (watchManager, commandOutput) -> watchTriggerFlag[0] = true, true).executeManager(manager);

        CreateCommand.generateCreateCommand(testRootPath.concat("/child1"),
                generatedString.getBytes(StandardCharsets.UTF_8), false, false,
                true).executeManager(manager);
//        manager.close();
        Thread.sleep(500);
        Assertions.assertTrue(watchTriggerFlag[0]);

        // TODO: Test persist
        watchTriggerFlag[0] = false;
        CreateCommand.generateCreateCommand(testRootPath.concat("/child2"),
                generatedString.getBytes(StandardCharsets.UTF_8), false, false,
                true).executeManager(manager);
        Thread.sleep(500);
        Assertions.assertTrue(watchTriggerFlag[0]);

        // TODO: Test remove
        RemoveWatchesCommand.generateRemoveWatchCommand(testRootPath,
                WatchedEvent.WatchType.CHILDREN).executeManager(manager);
        watchTriggerFlag[0] = false;
        CreateCommand.generateCreateCommand(testRootPath.concat("/child3"),
                generatedString.getBytes(StandardCharsets.UTF_8), false, false,
                true).executeManager(manager);
        Thread.sleep(500);
        Assertions.assertTrue(!watchTriggerFlag[0]);
    }
}