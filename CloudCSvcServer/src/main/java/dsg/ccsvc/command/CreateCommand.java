package dsg.ccsvc.command;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.datalayer.DataLayerMgrRDB;
import dsg.ccsvc.datalayer.ManagerLeaseHandlerBase;
import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.util.ConnectionPool;
import dsg.ccsvc.util.DebugConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static dsg.ccsvc.log.LogRecord.LogRecordType.CREATE;
import static dsg.ccsvc.log.LogRecord.LogRecordType.CREATE_EPHEMERAL;

/**
 * create command for cli
 */
@Slf4j
public class CreateCommand extends CSvcCommand {

    // TODO: consider not making this static
//    private static Options options = new Options();
//    private String[] args;
//    private CommandLine cl;

    private String stringCommand;

    private static Options options = new Options();

    static {
        options.addOption(new Option("e", false, "ephemeral"));
        options.addOption(new Option("s", false, "sequential"));
        options.addOption(new Option("c", false, "container"));
        options.addOption(new Option("retry", false, "enable retries"));
//        options.addOption(new Option("t", true, "ttl"));
    }

    public CreateCommand() {
//        super("create", "[-s] [-e] [-c] [-t ttl] path [data] [acl]");
        super("create", "[-s] [-e] [-c] [-retry] path [data]");
    }

    @Override
    public CSvcCommand parse(String[] cmdArgs) throws ParseException {
        DefaultParser parser = new DefaultParser();
        if (DebugConstants.DEBUG_FLAG) {
            System.out.println(String.format("[cmd_parse_cmdArgs=%s]",
                    String.join(",", cmdArgs)));
        }

//        try {
        cl = parser.parse(options, cmdArgs);
//        } catch (ParseException ex) {
//            throw new CliParseException(ex);
//        }

        args = Arrays.copyOfRange(cl.getArgs(), 1, cl.getArgs().length);
        if (DebugConstants.DEBUG_FLAG) {
            System.out.println(String.format("[ephemeral_flag=%b, sequential_flag=%b]",
                    cl.hasOption("e"), cl.hasOption("s")));
            System.out.println(String.format("[cmd_parse_args=%s]", String.join(",", args)));
        }
        if (args.length < 1) {
            throw new ParseException(getUsageStr());
        }
        return this;
    }

    public CommandOutput executeManager(DataLayerMgrBase manager) throws InvalidServiceStateException {
        CommandOutput result = new CommandOutput();
        String output = "";
        CommitOptions.CommitOptionsBuilder optionsBldr = CommitOptions.builder();
        // TODO: consider how to better design setup
//        this.prepareManager(manager);

        // TODO: synchronize on manager
//        long logValidationValue = manager.getLogValidationToken();
//        Instant logInstant = manager.getLogInstant();
        int pathIndex = 0;
        // TODO: Clean up flags
        boolean sequentialFlag = cl.hasOption("s");
        optionsBldr.sequentialFlag(sequentialFlag);
//        pathIndex += sequentialFlag ? 1 : 0;

        boolean retryFlag = cl.hasOption("retry");

        boolean ephemeralFlag = cl.hasOption("e");
        LogRecord.LogRecordType type = ephemeralFlag ? CREATE_EPHEMERAL : CREATE;
//        pathIndex += ephemeralFlag ? 1 : 0;

//        Path filepath = Paths.get(this.getArgIndex(0));
        Path filepath = Paths.get(this.getArgIndex(pathIndex));
        // TODO: there was an of by 1 error here. Confirm not the same in other places
        String data = this.getArgCount() == pathIndex+1 ? "null" : this.getArgIndex(pathIndex+1);
        String filename = filepath.getFileName().toString();
        String parentPath = filepath.getParent().toString();
//        System.out.println(String.format("REMOVE ME! [operation=%s, filename=%s, parentPath=%s, data=%s]",
//                "create", filename, parentPath, data));
        // TODO: add support for ephemeral nodes and watches
        // TODO: Consider if there is anything better than these soft validations.
        //  Nothing other than checking table guarantees no issues
//        Set<String> parentChildren = manager.getChildrenNodes(parentPath);
        boolean parentExistsFlag = manager.validatePath(parentPath);
        boolean nodeExistsFlag = manager.validatePath(filepath.toString());
//        System.out.println(String.format("REMOVE ME - DEBUG child mapping []"));

//        HashSet<String> parentChildren = manager.getFromMappingStructure("pathChildrenMap", parentPath);
//        boolean parentMissing = Objects.isNull(manager.getFromMappingStructure("pathChildrenMap", parentPath, HashSet.class));
//        if (parentMissing) {
        // ===
        // VALIDATIONS
//        if (!parentExistsFlag) {
//            // TODO: consider refactoring for cleaner implementation - more granular error message
//            output = String.format("Create Operation Failed - Parent Missing [parentPath=%s]", parentPath);
//            result.populateError(1, output);
//            System.out.println(output);
//            return result;
//        }
//
//        if (nodeExistsFlag) {
//            // TODO: consider refactoring for cleaner implementation - more granular error message
//            output = String.format("Create Operation Failed - Duplicate Path [path=%s]", filepath.toString());
//            result.populateError(1, output);
//            System.out.println(output);
//            return result;
//        }
//
//        // NOTE: Ephemeral node validations
//        if (manager.containsEphemeralNode(parentPath)) {
//            // TODO: consider refactoring for cleaner implementation
//            output = String.format("Create Operation Failed - Parent path EPHEMERAL [parentPath=%s]", parentPath);
//            result.populateError(1, output);
//            System.out.println(output);
//            return result;
//        }
        // === SOFT VALIDATIONS END

//        if (parentChildren.contains(filename)) {
//            // TODO: consider refactoring for cleaner implementation
//            output = String.format("Create Operation Failed - Node already exists [path=%s]", filepath.toString());
//            result.populateError(1, output);
//            System.out.println(output);
//            return result;
//        }
//        boolean sequentialFlag = cl.hasOption("s");
//
////        boolean commitFlag = manager.commitToLog(logInstant, logRecord);
//        boolean ephemeralFlag = cl.hasOption("e");
//        if (ephemeralFlag) {
////            System.out.println(
////                    String.format("CreateCommand - Ephemeral Node [path=%s, manager_id=%d]",
////                            filepath.toString(), manager.getManagerId()));
//            type = LogRecord.LogRecordType.CREATE_EPHEMERAL;
//        }
        // TODO: consider that the manager that commits knows its own ID
        LogRecord logRecord = new LogRecord(filepath.toString(),
                data.getBytes(StandardCharsets.UTF_8),
                type,
                manager.getManagerId());
//        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
        int retryMax = retryFlag ? RETRY_THRESHOLD : 1;
//        boolean leaseFlag = false;
//        boolean leaseFlag = true; // FOR TESTING
        boolean commitFlag = false;
        int retryCount = 0;
        // TODO: Consider always attempting to gain lease multiple times
//        while (!leaseFlag && retryCount < RETRY_THRESHOLD) {
//            retryCount++;
//            leaseFlag = manager.claimSoftLease();
//            if (retryFlag && !leaseFlag) {
//                // TODO: Consider what happens if it fails for non SQL reasons
//                // NOTE: Log retry
//                System.out.println(String.format(
//                        "DEBUG - CreateCommand - Retrying failed lease claim [path=%s, retry_count=%d]",
//                        filepath.toString(), retryCount));
//                // TODO: Evaluate how much to wait + bakery algorithm
//            }
//        }

        long logValidationValue = manager.getLogValidationToken();
//        manager.claimSoftLease();
//        System.out.printf("\t\tREMOVE ME - CreateCommand - Submit lease claim [path=%s]\n", filepath);
//        boolean leaseClaim = false;
//        while (!leaseClaim) {
//            try {
//                leaseClaim = manager.submitSoftLeaseClaim().get();
//            } catch (Exception e) {
//                System.out.println("ERROR - CreateCommand - An error occurred.");
//                e.printStackTrace(System.out);
//                output = "Create Operation Failed.";
//                result.populateError(1, output);
//                return result;
//            }
//        }

//        System.out.printf("\t\tREMOVE ME - CreateCommand - Start Lease Handler [path=%s]\n", filepath);
        try (ManagerLeaseHandlerBase ignored = manager.claimSoftLeaseHandler(false).get()) {
//        try (ManagerLeaseHandlerBase ignored = manager.startLeaseHandler()) {
            while (!commitFlag && retryCount < retryMax) {
                retryCount++;
                commitFlag = manager.commitToLog(logValidationValue, logRecord, optionsBldr.build());
                if (retryFlag && !commitFlag) {
                    // TODO: Consider what happens if it fails for non SQL reasons
                    // NOTE: Log retry
                    System.out.println(String.format(
                            "DEBUG - CreateCommand - Retrying failed commit [path=%s, retry_count=%d]",
                            filepath.toString(), retryCount));
                }
            }
        } catch (ExecutionException e) {
            log.error(String.format("CreateCommand - An error occurred [path=%s]", filepath), e);
            throw new InvalidServiceStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag
            log.error("CreateCommand - Thread was interrupted", e);
        } catch (Exception e) {
            log.error(String.format("CreateCommand - Unexpected Exception [path=%s]", filepath), e);
            throw new InvalidServiceStateException(e);
        }
//        if (leaseFlag) {
//            manager.releaseSoftLease();
//        }
        // TODO: Enable ephemeralFlag again
//        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, ephemeralFlag, false);
//        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
//        boolean commitFlag = true;
        if (commitFlag) {
            // TODO: refactor this, can be handled by manager in commit to log
//            output = "Create Operation Succeeded.";
            result.setOutput(logRecord.getPath());
            result.setPath(logRecord.getPath());
//            manager.updateMetadataStore(logRecord);
//            return result;
        } else {
            // TODO: RETRY with VERSION VALIDATION
            System.out.println("WARN - CreateCommand - An error occurred.");
//            e.printStackTrace();
            output = "Create Operation Failed.";
            result.populateError(1, output);
//            return result;
        }
        return result;
    }

//    public CommandOutput executeManager(CSvcManagerAmazonDB_CmdAgnostic manager) throws InvalidServiceStateException {
//        CommandOutput result = new CommandOutput();
//        String output = "";
//        // TODO: consider how to better design setup
////        this.prepareManager(manager);
//
//        // TODO: synchronize on manager
////        long logValidationValue = manager.getLogValidationToken();
////        Instant logInstant = manager.getLogInstant();
//        int pathIndex = 0;
//        boolean sequentialFlag = cl.hasOption("s");
////        pathIndex += sequentialFlag ? 1 : 0;
//
//        boolean ephemeralFlag = cl.hasOption("e");
////        pathIndex += ephemeralFlag ? 1 : 0;
//
//        boolean retryFlag = cl.hasOption("retry");
//
////        Path filepath = Paths.get(this.getArgIndex(0));
//        Path filepath = Paths.get(this.getArgIndex(pathIndex));
//        // TODO: there was an of by 1 error here. Confirm not the same in other places
//        String data = this.getArgCount() == pathIndex+1 ? "null" : this.getArgIndex(pathIndex+1);
//        String filename = filepath.getFileName().toString();
//        String parentPath = filepath.getParent().toString();
////        System.out.println(String.format("REMOVE ME! [operation=%s, filename=%s, parentPath=%s, data=%s]",
////                "create", filename, parentPath, data));
//        // TODO: add support for ephemeral nodes and watches
//        // TODO: Consider if there is anything better than these soft validations.
//        //  Nothing other than checking table guarantees no issues
////        Set<String> parentChildren = manager.getChildrenNodes(parentPath);
//        boolean parentExistsFlag = manager.validatePath(parentPath);
//        boolean nodeExistsFlag = manager.validatePath(filepath.toString());
////        System.out.println(String.format("REMOVE ME - DEBUG child mapping []"));
//
////        HashSet<String> parentChildren = manager.getFromMappingStructure("pathChildrenMap", parentPath);
////        boolean parentMissing = Objects.isNull(manager.getFromMappingStructure("pathChildrenMap", parentPath, HashSet.class));
////        if (parentMissing) {
//        // ===
//        // VALIDATIONS
////        if (!parentExistsFlag) {
////            // TODO: consider refactoring for cleaner implementation - more granular error message
////            output = String.format("Create Operation Failed - Parent Missing [parentPath=%s]", parentPath);
////            result.populateError(1, output);
////            System.out.println(output);
////            return result;
////        }
////
////        if (nodeExistsFlag) {
////            // TODO: consider refactoring for cleaner implementation - more granular error message
////            output = String.format("Create Operation Failed - Duplicate Path [path=%s]", filepath.toString());
////            result.populateError(1, output);
////            System.out.println(output);
////            return result;
////        }
////
////        // NOTE: Ephemeral node validations
////        if (manager.containsEphemeralNode(parentPath)) {
////            // TODO: consider refactoring for cleaner implementation
////            output = String.format("Create Operation Failed - Parent path EPHEMERAL [parentPath=%s]", parentPath);
////            result.populateError(1, output);
////            System.out.println(output);
////            return result;
////        }
//        // === SOFT VALIDATIONS END
//
////        if (parentChildren.contains(filename)) {
////            // TODO: consider refactoring for cleaner implementation
////            output = String.format("Create Operation Failed - Node already exists [path=%s]", filepath.toString());
////            result.populateError(1, output);
////            System.out.println(output);
////            return result;
////        }
////        boolean sequentialFlag = cl.hasOption("s");
////
//////        boolean commitFlag = manager.commitToLog(logInstant, logRecord);
////        boolean ephemeralFlag = cl.hasOption("e");
//        LogRecord.LogRecordType type = CREATE;
//        if (ephemeralFlag) {
//            System.out.println(
//                    String.format("CreateCommand - Ephemeral Node [path=%s, manager_id=%d]",
//                            filepath.toString(), manager.getManagerId()));
//            type = CREATE_EPHEMERAL;
//        }
//        // TODO: consider that the manager that commits knows its own ID
//        LogRecord logRecord = new LogRecord(filepath.toString(),
//                data.getBytes(StandardCharsets.UTF_8),
//                type,
//                manager.getManagerId());
////        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
//        int retryMax = retryFlag ? RETRY_THRESHOLD : 1;
////        boolean leaseFlag = false;
////        boolean leaseFlag = true; // FOR TESTING
//        boolean commitFlag = false;
//        int retryCount = 0;
//        // TODO: Consider always attempting to gain lease multiple times
////        while (!leaseFlag && retryCount < RETRY_THRESHOLD) {
////            retryCount++;
////            leaseFlag = manager.claimSoftLease();
////            if (retryFlag && !leaseFlag) {
////                // TODO: Consider what happens if it fails for non SQL reasons
////                // NOTE: Log retry
////                System.out.println(String.format(
////                        "DEBUG - CreateCommand - Retrying failed lease claim [path=%s, retry_count=%d]",
////                        filepath.toString(), retryCount));
////                // TODO: Evaluate how much to wait + bakery algorithm
////            }
////        }
//
//        // TODO: Consider how to extend lease
//        long logValidationValue = manager.getLogValidationToken();
////        manager.claimSoftLease();
//        boolean leaseClaim = false;
//        // TODO: Consider what to do for retries
//        while (!leaseClaim) {
//            try {
//                // TODO: Implement deleting timed out waitlists
//                leaseClaim = manager.submitSoftLeaseClaim().get();
//            } catch (Exception e) {
//                System.out.println("ERROR - CreateCommand - An error occurred.");
//                e.printStackTrace(System.out);
//                output = "Create Operation Failed.";
//                result.populateError(1, output);
//                return result;
//            }
//        }
//
//        try (ManagerLeaseHandlerBase ignored = manager.startLeaseHandler()) {
//            while (!commitFlag && retryCount < retryMax) {
//                retryCount++;
//                commitFlag = manager.commitToLog(logValidationValue, logRecord, -1,
//                        false, sequentialFlag, false);
//                if (retryFlag && !commitFlag) {
//                    // TODO: Consider what happens if it fails for non SQL reasons
//                    // NOTE: Log retry
//                    System.out.println(String.format(
//                            "DEBUG - CreateCommand - Retrying failed commit [path=%s, retry_count=%d]",
//                            filepath.toString(), retryCount));
//                }
//            }
//        } catch (Exception e) {
//            throw new InvalidServiceStateException(e);
//        }
////        if (leaseFlag) {
////            manager.releaseSoftLease();
////        }
//        // TODO: Enable ephemeralFlag again
////        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, ephemeralFlag, false);
////        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
////        boolean commitFlag = true;
//        if (commitFlag) {
//            // TODO: refactor this, can be handled by manager in commit to log
////            output = "Create Operation Succeeded.";
//            result.setOutput(logRecord.getPath());
//            result.setPath(logRecord.getPath());
////            manager.updateMetadataStore(logRecord);
////            return result;
//        } else {
//            // TODO: RETRY with VERSION VALIDATION
//            System.out.println("An error occurred.");
////            e.printStackTrace();
//            output = "Create Operation Failed.";
//            result.populateError(1, output);
////            return result;
//        }
//        return result;
//    }

    public CommandOutput executeManagerOriginal(DataLayerMgrRDB manager) throws InvalidServiceStateException {
        CommandOutput result = new CommandOutput();
        String output = "";
        // TODO: consider how to better design setup
//        this.prepareManager(manager);

        // TODO: synchronize on manager
//        long logValidationValue = manager.getLogValidationToken();
//        Instant logInstant = manager.getLogInstant();
        int pathIndex = 0;
        boolean sequentialFlag = cl.hasOption("s");
//        pathIndex += sequentialFlag ? 1 : 0;

        boolean ephemeralFlag = cl.hasOption("e");
//        pathIndex += ephemeralFlag ? 1 : 0;

        boolean retryFlag = cl.hasOption("retry");

//        Path filepath = Paths.get(this.getArgIndex(0));
        Path filepath = Paths.get(this.getArgIndex(pathIndex));
        // TODO: there was an of by 1 error here. Confirm not the same in other places
        String data = this.getArgCount() == pathIndex+1 ? "null" : this.getArgIndex(pathIndex+1);
        String filename = filepath.getFileName().toString();
        String parentPath = filepath.getParent().toString();
//        System.out.println(String.format("REMOVE ME! [operation=%s, filename=%s, parentPath=%s, data=%s]",
//                "create", filename, parentPath, data));
        // TODO: add support for ephemeral nodes and watches
        // TODO: Consider if there is anything better than these soft validations.
        //  Nothing other than checking table guarantees no issues
        Set<String> parentChildren = manager.getChildrenNodes(parentPath);
//        boolean isValid = manager.validateNewPath(filepath.toString());
//        System.out.println(String.format("REMOVE ME - DEBUG child mapping []"));

//        HashSet<String> parentChildren = manager.getFromMappingStructure("pathChildrenMap", parentPath);
//        boolean parentMissing = Objects.isNull(manager.getFromMappingStructure("pathChildrenMap", parentPath, HashSet.class));
//        if (parentMissing) {
        // VALIDATIONS
        if (Objects.isNull(parentChildren)) {
            // TODO: consider refactoring for cleaner implementation
            output = String.format("Create Operation Failed - Parent path not found [parentPath=%s]", parentPath);
            result.populateError(1, output);
            System.out.println(output);
            return result;
        }

        // NOTE: Ephemeral node validations
        if (manager.containsEphemeralNode(parentPath)) {
            // TODO: consider refactoring for cleaner implementation
            output = String.format("Create Operation Failed - Parent path EPHEMERAL [parentPath=%s]", parentPath);
            result.populateError(1, output);
            System.out.println(output);
            return result;
        }

        if (parentChildren.contains(filename)) {
            // TODO: consider refactoring for cleaner implementation
            output = String.format("Create Operation Failed - Node already exists [path=%s]", filepath.toString());
            result.populateError(1, output);
            System.out.println(output);
            return result;
        }
//        boolean sequentialFlag = cl.hasOption("s");
//
////        boolean commitFlag = manager.commitToLog(logInstant, logRecord);
//        boolean ephemeralFlag = cl.hasOption("e");
        LogRecord.LogRecordType type = CREATE;
        if (ephemeralFlag) {
            System.out.println(
                    String.format("CreateCommand - Ephemeral Node [manager_id=%d]",
                            manager.getManagerId()));
            type = CREATE_EPHEMERAL;
        }
        // TODO: consider that the manager that commits knows its own ID
        LogRecord logRecord = new LogRecord(filepath.toString(),
                data.getBytes(StandardCharsets.UTF_8),
                type,
                manager.getManagerId());
//        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
        int retryMax = retryFlag ? RETRY_THRESHOLD : 1;
//        boolean leaseFlag = false;
//        boolean leaseFlag = true; // FOR TESTING
        boolean commitFlag = false;
        int retryCount = 0;
        // TODO: Consider always attempting to gain lease multiple times
//        while (!leaseFlag && retryCount < RETRY_THRESHOLD) {
//            retryCount++;
//            leaseFlag = manager.claimSoftLease();
//            if (retryFlag && !leaseFlag) {
//                // TODO: Consider what happens if it fails for non SQL reasons
//                // NOTE: Log retry
//                System.out.println(String.format(
//                        "DEBUG - CreateCommand - Retrying failed lease claim [path=%s, retry_count=%d]",
//                        filepath.toString(), retryCount));
//                // TODO: Evaluate how much to wait + bakery algorithm
//            }
//        }

        // TODO: Consider how to extend lease
        long logValidationValue = manager.getLogValidationToken();
        while (!commitFlag && retryCount < retryMax) {
            retryCount++;
            commitFlag = manager.commitToLog(logValidationValue, logRecord, -1,
                    false, sequentialFlag, false);
            if (retryFlag && !commitFlag) {
                // TODO: Consider what happens if it fails for non SQL reasons
                // NOTE: Log retry
                System.out.println(String.format(
                        "DEBUG - CreateCommand - Retrying failed commit [path=%s, retry_count=%d]",
                        filepath.toString(), retryCount));
            }
        }

//        if (leaseFlag) {
//            manager.releaseSoftLease();
//        }
        // TODO: Enable ephemeralFlag again
//        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, ephemeralFlag, false);
//        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
//        boolean commitFlag = true;
        if (commitFlag) {
            // TODO: refactor this, can be handled by manager in commit to log
//            output = "Create Operation Succeeded.";
            result.setOutput(logRecord.getPath());
//            manager.updateMetadataStore(logRecord);
//            return result;
        } else {
            // TODO: RETRY with VERSION VALIDATION
            System.out.println("An error occurred.");
//            e.printStackTrace();
            output = "Create Operation Failed.";
            result.populateError(1, output);
//            return result;
        }
        return result;
    }

    public CommandOutput executeManagerSlowTest(DataLayerMgrRDB manager) {
        CommandOutput result = new CommandOutput();
        String output = "";
        // TODO: consider how to better design setup
//        this.prepareManager(manager);

        // TODO: synchronize on manager
        long logValidationValue = manager.getLogValidationToken();
//        Instant logInstant = manager.getLogInstant();
        Path filepath = Paths.get(this.getArgIndex(0));
        // TODO: there was an of by 1 error here. Confirm not the same in other places
        String data = this.getArgCount() == 1 ? "null" : this.getArgIndex(1);
        String filename = filepath.getFileName().toString();
        String parentPath = filepath.getParent().toString();
//        System.out.println(String.format("REMOVE ME! [operation=%s, filename=%s, parentPath=%s, data=%s]",
//                "create", filename, parentPath, data));
        // TODO: add support for ephemeral nodes and watches
        Set<String> parentChildren = manager.getChildrenNodes(parentPath);
//        System.out.println(String.format("REMOVE ME - DEBUG child mapping []"));

//        HashSet<String> parentChildren = manager.getFromMappingStructure("pathChildrenMap", parentPath);
//        boolean parentMissing = Objects.isNull(manager.getFromMappingStructure("pathChildrenMap", parentPath, HashSet.class));
//        if (parentMissing) {
        // VALIDATIONS
        if (Objects.isNull(parentChildren)) {
            // TODO: consider refactoring for cleaner implementation
            output = "Create Operation Failed - Parent path not found.";
            result.populateError(1, output);
            System.out.println(String.format("Create Operation Failed - Parent path not found."));
            return result;
        }

        if (parentChildren.contains(filename)) {
            // TODO: consider refactoring for cleaner implementation
            output = String.format("Create Operation Failed - Node already exists [path=%s]", filepath.toString());
            result.populateError(1, output);
            System.out.println(output);
            return result;
        }

        LogRecord logRecord = new LogRecord(filepath.toString(),
                data.getBytes(StandardCharsets.UTF_8),
                CREATE);
//        boolean commitFlag = manager.commitToLog(logInstant, logRecord);
        boolean commitFlag = false;
        try (Connection conn = ConnectionPool.getConnection()) {
            commitFlag = manager.testSlowCommit(logValidationValue, logRecord, -1,
                    false, conn, false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
//        boolean commitFlag = true;
        if (commitFlag) {
            // TODO: refactor this, can be handled by manager in commit to log
            output = "Create Operation Succeeded.";
            result.setOutput(output);
//            manager.updateMetadataStore(logRecord);
            return result;
        } else {
            // TODO: RETRY with VERSION VALIDATION
            System.out.println("An error occurred.");
//            e.printStackTrace();
            output = "Create Operation Failed.";
            result.populateError(1, output);
            return result;
        }
    }

    // TODO: Remove this, just tesing one connection performance
//    public CommandOutput executeManagerOneConnection(CSvcManagerAmazonDB_CmdAgnostic manager, Connection conn) {
//        CommandOutput result = new CommandOutput();
//        String output = "";
//        // TODO: consider how to better design setup
////        this.prepareManager(manager);
//
//        // TODO: synchronize on manager
//        long logValidationValue = 0;
//        try {
//            logValidationValue = manager.getLogValidationToken(conn);
//        } catch (SQLException e) {
//            System.out.println(String.format("Error - getLogValidationToken - Failed to connect."));
//            throw new RuntimeException(e);
//        }
////        Instant logInstant = manager.getLogInstant();
//        Path filepath = Paths.get(this.getArgIndex(0));
//        // TODO: there was an of by 1 error here. Confirm not the same in other places
//        String data = this.getArgCount() == 1 ? "null" : this.getArgIndex(1);
//        String filename = filepath.getFileName().toString();
//        String parentPath = filepath.getParent().toString();
////        System.out.println(String.format("REMOVE ME! [operation=%s, filename=%s, parentPath=%s, data=%s]",
////                "create", filename, parentPath, data));
//        // TODO: add support for ephemeral nodes and watches
//        Set<String> parentChildren = manager.getChildrenNodes(parentPath);
////        HashSet<String> parentChildren = manager.getFromMappingStructure("pathChildrenMap", parentPath);
////        boolean parentMissing = Objects.isNull(manager.getFromMappingStructure("pathChildrenMap", parentPath, HashSet.class));
////        if (parentMissing) {
//        if (Objects.isNull(parentChildren)) {
//            // TODO: consider refactoring for cleaner implementation
//            output = "Create Operation Failed - Parent path not found.";
//            result.populateError(1, output);
//            System.out.println(String.format("Create Operation Failed - Parent path not found."));
//            return result;
//        }
//
//        LogRecord logRecord = new LogRecord(filepath.toString(),
//                data.getBytes(StandardCharsets.UTF_8),
//                LogRecord.LogRecordType.CREATE);
////        boolean commitFlag = manager.commitToLog(logInstant, logRecord);
//        boolean commitFlag = false;
//        try {
//            commitFlag = manager.commitToLog(logValidationValue, logRecord, false, conn);
//        } catch (SQLException e) {
//            System.out.println(String.format("Error - commitToLog - Failed to commit."));
//            throw new RuntimeException(e);
//        }
////        boolean commitFlag = true;
//        if (commitFlag) {
//            // TODO: refactor this, can be handled by manager in commit to log
//            output = "Create Operation Succeeded.";
//            result.setOutput(output);
////            manager.updateMetadataStore(logRecord);
//            return result;
//        } else {
//            // TODO: RETRY with VERSION VALIDATION
//            System.out.println("An error occurred.");
////            e.printStackTrace();
//            output = "Create Operation Failed.";
//            result.populateError(1, output);
//            return result;
//        }
//    }

    @Override
    public String toString() {
        return String.format("[cmd=create, cl_args=%s, cl_options=%s, args=%s]",
                cl.getArgList().toString(), Arrays.asList(cl.getOptions()), Arrays.asList(args));
    }

    // TODO: remove this, for testing only
    public String toStringArgs() {
        return String.format("[cmd=create, args=%s]", Arrays.asList(args));
    }

//    // TODO: Consider a different approach. Standardize the required structures
//    //  and have each data layer populate it properly
//    private void prepareManager(CSvcManagerAmazonDB_CmdAgnostic manager) {
//        manager.addToMappingStructure("pathChildrenMap", "/", new HashSet<String>());
//    }

    @Override
    public CommandOutput executeManager() {
        return manager.create(this);
    }

    //TODO: remove testing helper
    public static CreateCommand generateCreateCommand(String path, String content) {
//        CreateCommand cmd = new CreateCommand();
////        cmd.args = new String[]{path, content};
//        cmd.setArgs(new String[]{path, content});
        return generateCreateCommand(path, content.getBytes(StandardCharsets.UTF_8),
                false, false);
    }

    public static CreateCommand generateCreateCommand(
            String path, byte[] content,
            boolean ephemeralFlag, boolean sequentialFLag) {
        return generateCreateCommand(path, content, ephemeralFlag, sequentialFLag, false);
    }

    public static CreateCommand generateCreateCommand(
            String path, byte[] content,
            boolean ephemeralFlag, boolean sequentialFLag, boolean retryFlag) {
        CreateCommand cmd = new CreateCommand();
        ArrayList<String> tokens = new ArrayList<>();
        tokens.add("create");
        if (ephemeralFlag) {
            tokens.add("-e");
        }

        if (sequentialFLag) {
            tokens.add("-s");
        }

        if (retryFlag) {
            tokens.add("-retry");
        }

        tokens.add(path);
        if (content.length > 0) {
            // TODO: Consider how to generate a create a command with arbitrary byte content
            tokens.add(new String(content, StandardCharsets.UTF_8));
        }

        // TODO: Handle exceptions; Either remove dependency on ZK CLI
        try {
            cmd.parse(tokens.toArray(new String[0]));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        if (DebugConstants.DEBUG_FLAG) {
            System.out.println(String.format("cmd=%s", cmd));
        }
        return cmd;
    }

    /*
    GetCommand cmd = new GetCommand();
        cmd.watchCallable = watchCallable;
        ArrayList<String> tokens = new ArrayList<>();
        tokens.add("get ");
        if (statsFlag) {
            tokens.add("-s ");
        }

        if (cacheFlag) {
            tokens.add("-c ");
        }

        if (watchFlag) {
            tokens.add("-w ");
        }

        tokens.add(path);
        cmd.parse(tokens.toArray(new String[0]));
//        StringBuilder strBldr = new StringBuilder();
//        if (watchFlag) {
//            strBldr.append("-w ");
//        }
//        strBldr.append(path);
////        cmd.args = new String[]{path};
//        cmd.setArgs(strBldr.toString().split(" "));
        return cmd;
     */
}
