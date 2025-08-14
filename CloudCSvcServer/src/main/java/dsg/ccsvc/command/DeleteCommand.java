package dsg.ccsvc.command;

import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.datalayer.ManagerLeaseHandlerBase;
import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.util.DebugConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
public class DeleteCommand extends CSvcCommand {

    private static Options options = new Options();
    private CommandLine cl;
    // TODO: Consider making path a base field
    private String path;
    private final boolean highPriorityFlag;

//    boolean versionFlag;

//    int expectedVersion;

    static {
        options.addOption("v", true, "version");
        options.addOption("L", false, "log sync");
        options.addOption(new Option("retry", false, "enable retries"));
    }

    public DeleteCommand() {
        this(false);
    }

    public DeleteCommand(boolean highPriorityFlag) {
        super("delete", "[-retry] [-v version] path");
        this.highPriorityFlag = highPriorityFlag;
    }

    @Override
    public CSvcCommand parse(String[] cmdArgs) throws ParseException {
        DefaultParser parser = new DefaultParser();
//        try {
            cl = parser.parse(options, cmdArgs);
//        } catch (ParseException ex) {
//            throw new CliParseException(ex);
//        }
//        args = cl.getArgs();
        args = Arrays.copyOfRange(cl.getArgs(), 1, cl.getArgs().length);
        if (args.length < 1) {
            throw new ParseException(getUsageStr());
        }

        path = this.getArgIndex(0);

//        retainCompatibility(cmdArgs);

        return this;
    }

    private void retainCompatibility(String[] cmdArgs) throws ParseException {
        if (args.length > 2) {
            err.println("'delete path [version]' has been deprecated. "
                        + "Please use 'delete [-v version] path' instead.");
            DefaultParser parser = new DefaultParser();
//            try {
                cl = parser.parse(options, cmdArgs);
//            } catch (ParseException ex) {
//                throw new CliParseException(ex);
//            }
            args = cl.getArgs();
        }
    }

    public CommandOutput executeManager(DataLayerMgrBase manager) throws InvalidServiceStateException {
        CommandOutput result = new CommandOutput();
        String output = "";
        CommitOptions.CommitOptionsBuilder optionsBldr = CommitOptions.builder();

        // TODO: consider how to better design setup
//        this.prepareManager(manager);

        // TODO: synchronize on manager
        long logValidationValue = manager.getLogValidationToken();
//        Instant logInstant = manager.getLogInstant();
        Path filepath = Paths.get(this.getArgIndex(0));
        log.error(String.format("REMOVE ME REMOVEME - Started delete [path=%s, priorityFlag=%b]",
                filepath, highPriorityFlag));
        // TODO: there was an of by 1 error here. Confirm not the same in other places
        String data = this.getArgCount() == 1 ? "null" : this.getArgIndex(1);
        String filename = filepath.getFileName().toString();
        String parentPath = filepath.getParent().toString();
//        System.out.println(String.format("REMOVE ME! [operation=%s, filename=%s, parentPath=%s, data=%s]",
//                "create", filename, parentPath, data));
        if (manager.getRootNode().equals(filepath.toString())) {
                output = String.format("Delete Operation Failed - Invalid Path " +
                                        "- Cannot delete root node [path=%s]", filepath.toString());
                result.populateError(1, output);
                System.out.println(output);
                return result;
        }
        // TODO: implement version validation
//        boolean versionFlag = false;
//        int expectedVersion = -1;
//        if(cl.hasOption("v")) {
//            versionFlag = true;
//            expectedVersion = Integer.parseInt(cl.getOptionValue("v"));
//        }

        boolean retryFlag = cl.hasOption("retry");

        boolean versionFlag = cl.hasOption("v");
        int expectedVersion = versionFlag ? Integer.parseInt(cl.getOptionValue("v")) : -1;
        optionsBldr.validateVersionFlag(versionFlag);
        optionsBldr.expectedVersion(expectedVersion);

        boolean syncLogFlag = cl.hasOption("L");
        // TODO: add support for ephemeral nodes and watches
        // NOTE: Preliminary Validations
//        System.out.println(String.format("REMOVE ME - DEBUG child mapping []"));

//        HashSet<String> parentChildren = manager.getFromMappingStructure("pathChildrenMap", parentPath);
//        boolean parentMissing = Objects.isNull(manager.getFromMappingStructure("pathChildrenMap", parentPath, HashSet.class));
//        if (parentMissing) {
        // NOTE: Skip redundant validations if syncing to log
        if (!syncLogFlag) {
            // === SOFT VALIDATIONS START
//            boolean nodeExistsFlag = manager.validatePath(filepath.toString());
//
//            // SOFT PRE VALIDATIONS
//            // NOTE: Check node exists
//            if (!nodeExistsFlag) {
//                // TODO: consider refactoring for cleaner implementation
//                output = String.format("Delete Operation Failed - Path not found [path=%s]", filepath.toString());
//                result.populateError(1, output);
//                System.out.println(output);
//                return result;
//            }
//
//            Set<String> children = manager.getChildrenNodes(filepath.toString());
//            // TODO: Clean up this logic, or wrap it in helper function
//            // NOTE: If children null, then target path exists in outstanding changes
//            if (!Objects.isNull(children) && !children.isEmpty()) {
//                // TODO: consider refactoring for cleaner implementation
//                output = String.format("Delete Operation Failed - Node not empty [path=%s]", filepath.toString());
//                result.populateError(1, output);
//                System.out.println(output);
//                return result;
//            }
            // === SOFT VALIDATIONS END
        }

        LogRecord logRecord = new LogRecord(filepath.toString(),
                new byte[0],
                LogRecord.LogRecordType.DELETE);
        int retryMax = retryFlag ? RETRY_THRESHOLD : 1;
//        // TODO: REMOVEME REMOVE ME
//        log.warn(String.format("DeleteCommand - REMOVEME REMOVE ME - retry flag [retry_flag=%b, retry_max=%d]",
//                retryFlag, retryMax));
        boolean commitFlag = false;
        int retryCount = 0;

//        boolean leaseClaim = false;
//        while (!leaseClaim) {
//            try {
//                leaseClaim = manager.submitSoftLeaseClaim().get();
//            } catch (Exception e) {
//                log.error("ERROR - DeleteCommand - An error occurred.", e);
////                e.printStackTrace(System.out);
//                output = "Delete Operation Failed.";
//                result.populateError(1, output);
//                return result;
//            }
//        }

//        try (ManagerLeaseHandlerBase ignored = manager.startLeaseHandler()) {
        try (ManagerLeaseHandlerBase ignored = manager.claimSoftLeaseHandler(highPriorityFlag).get()) {
            //        boolean commitFlag = manager.commitToLog(logInstant, logRecord);
            // TODO: add version validation
            // TODO: Combine the two into general catch all calls once confirmed to work
            while (!commitFlag && retryCount < retryMax) {
                retryCount++;
                commitFlag = manager.commitToLog(logValidationValue, logRecord, optionsBldr.build());
//                commitFlag = manager.commitToLog(logValidationValue, logRecord,
//                        expectedVersion, versionFlag, false);
                if (retryFlag && !commitFlag) {
                    // TODO: Consider what happens if it fails for non SQL reasons
                    // NOTE: Log retry
                    log.warn(String.format(
                            "DeleteCommand - Retrying failed commit [path=%s, retry_count=%d]",
                            filepath, retryCount));
                }
            }
        } catch (ExecutionException e) {
            log.error(String.format("DeleteCommand - An error occurred [path=%s]", filepath), e);
            throw new InvalidServiceStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag
            log.error("DeleteCommand - Thread was interrupted", e);
        } catch (Exception e) {
            log.error(String.format("DeleteCommand - Unexpected Exception [path=%s]", filepath), e);
            throw new InvalidServiceStateException(e);
        }


//        if (versionFlag) {
//            commitFlag = manager.commitToLog(logValidationValue, logRecord, expectedVersion, versionFlag, false);
//        } else {
//            commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
//        }
//        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
//        boolean commitFlag = true;
        if (commitFlag) {
            // TODO: refactor this, can be handled by manager in commit to log
            output = String.format("Delete Operation Succeeded [path=%s]", filepath.toString());
            result.setOutput(output);
//            manager.updateMetadataStore(logRecord);
//            return result;
        } else {
            // TODO: RETRY with VERSION VALIDATION
            output = String.format("Delete Operation Failed [path=%s]", filepath.toString());
            result.setOutput(output);
//            e.printStackTrace();
            result.populateError(1, output);
//            System.out.println(output);
//            return result;
        }
        if (DebugConstants.DEBUG_FLAG) {
            log.debug(output);
        }
        return result;
    }

//    public CommandOutput executeManager(CSvcManagerAmazonDB_CmdAgnostic manager) throws InvalidServiceStateException {
//        CommandOutput result = new CommandOutput();
//        String output = "";
//        CommitOptions.CommitOptionsBuilder optionsBldr = CommitOptions.builder();
//        // TODO: consider how to better design setup
////        this.prepareManager(manager);
//
//        // TODO: synchronize on manager
//        long logValidationValue = manager.getLogValidationToken();
////        Instant logInstant = manager.getLogInstant();
//        Path filepath = Paths.get(this.getArgIndex(0));
//        // TODO: there was an of by 1 error here. Confirm not the same in other places
//        String data = this.getArgCount() == 1 ? "null" : this.getArgIndex(1);
//        String filename = filepath.getFileName().toString();
//        String parentPath = filepath.getParent().toString();
////        System.out.println(String.format("REMOVE ME! [operation=%s, filename=%s, parentPath=%s, data=%s]",
////                "create", filename, parentPath, data));
//
//        // TODO: implement version validation
////        boolean versionFlag = false;
////        int expectedVersion = -1;
////        if(cl.hasOption("v")) {
////            versionFlag = true;
////            expectedVersion = Integer.parseInt(cl.getOptionValue("v"));
////        }
//
//        boolean retryFlag = cl.hasOption("retry");
//
//        boolean versionFlag = cl.hasOption("v");
//        int expectedVersion = versionFlag ? Integer.parseInt(cl.getOptionValue("v")) : -1;
//        optionsBldr.validateVersionFlag(versionFlag);
//        optionsBldr.expectedVersion(expectedVersion);
//
//        boolean syncLogFlag = cl.hasOption("L");
//        // TODO: add support for ephemeral nodes and watches
//        // NOTE: Preliminary Validations
////        System.out.println(String.format("REMOVE ME - DEBUG child mapping []"));
//
////        HashSet<String> parentChildren = manager.getFromMappingStructure("pathChildrenMap", parentPath);
////        boolean parentMissing = Objects.isNull(manager.getFromMappingStructure("pathChildrenMap", parentPath, HashSet.class));
////        if (parentMissing) {
//        // NOTE: Skip redundant validations if syncing to log
//        if (!syncLogFlag) {
//            // === SOFT VALIDATIONS START
////            boolean nodeExistsFlag = manager.validatePath(filepath.toString());
////
////            // SOFT PRE VALIDATIONS
////            // NOTE: Check node exists
////            if (!nodeExistsFlag) {
////                // TODO: consider refactoring for cleaner implementation
////                output = String.format("Delete Operation Failed - Path not found [path=%s]", filepath.toString());
////                result.populateError(1, output);
////                System.out.println(output);
////                return result;
////            }
////
////            Set<String> children = manager.getChildrenNodes(filepath.toString());
////            // TODO: Clean up this logic, or wrap it in helper function
////            // NOTE: If children null, then target path exists in outstanding changes
////            if (!Objects.isNull(children) && !children.isEmpty()) {
////                // TODO: consider refactoring for cleaner implementation
////                output = String.format("Delete Operation Failed - Node not empty [path=%s]", filepath.toString());
////                result.populateError(1, output);
////                System.out.println(output);
////                return result;
////            }
//            // === SOFT VALIDATIONS END
//        }
//
//        LogRecord logRecord = new LogRecord(filepath.toString(),
//                new byte[0],
//                LogRecord.LogRecordType.DELETE);
//        int retryMax = retryFlag ? RETRY_THRESHOLD : 1;
//        boolean commitFlag = false;
//        int retryCount = 0;
//
//        boolean leaseClaim = false;
//        // TODO: Consider what to do for retries
//        while (!leaseClaim) {
//            try {
//                // TODO: Implement deleting timed out waitlists
//                leaseClaim = manager.submitSoftLeaseClaim().get();
//            } catch (Exception e) {
//                System.out.println("ERROR - DeleteCommand - An error occurred.");
//                e.printStackTrace(System.out);
//                output = "Delete Operation Failed.";
//                result.populateError(1, output);
//                return result;
//            }
//        }
//
//        try (ManagerLeaseHandlerBase ignored = manager.startLeaseHandler()) {
//            //        boolean commitFlag = manager.commitToLog(logInstant, logRecord);
//            // TODO: add version validation
//            // TODO: Combine the two into general catch all calls once confirmed to work
//            while (!commitFlag && retryCount < retryMax) {
//                retryCount++;
//                commitFlag = manager.commitToLog(logValidationValue, logRecord, optionsBldr.build());
////                commitFlag = manager.commitToLog(logValidationValue, logRecord,
////                        expectedVersion, versionFlag, false);
//                if (retryFlag && !commitFlag) {
//                    // TODO: Consider what happens if it fails for non SQL reasons
//                    // NOTE: Log retry
//                    System.out.println(String.format(
//                            "DEBUG - DeleteCommand - Retrying failed commit [path=%s, retry_count=%d]",
//                            filepath.toString(), retryCount));
//                }
//            }
//        } catch (Exception e) {
//            throw new InvalidServiceStateException(e);
//        }
//
//
////        if (versionFlag) {
////            commitFlag = manager.commitToLog(logValidationValue, logRecord, expectedVersion, versionFlag, false);
////        } else {
////            commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
////        }
////        boolean commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
////        boolean commitFlag = true;
//        if (commitFlag) {
//            // TODO: refactor this, can be handled by manager in commit to log
//            output = String.format("Delete Operation Succeeded [path=%s]", filepath.toString());
//            result.setOutput(output);
////            manager.updateMetadataStore(logRecord);
////            return result;
//        } else {
//            // TODO: RETRY with VERSION VALIDATION
//            output = String.format("Delete Operation Failed [path=%s]", filepath.toString());
//            result.setOutput(output);
////            e.printStackTrace();
//            result.populateError(1, output);
////            System.out.println(output);
////            return result;
//        }
//        if (DebugConstants.DEBUG_FLAG) {
//            System.out.println(output);
//        }
//        return result;
//    }

//    public static DeleteCommand generateDeleteCommand(String path) {
//        DeleteCommand cmd = new DeleteCommand();
//        StringBuilder strBldr = new StringBuilder();
////        if (watchFlag) {
////            strBldr.append("-w ");
////        }
//        strBldr.append(path);
//        cmd.setArgs(strBldr.toString().split(" "));
//        return cmd;
//    }
//    public static DeleteCommand generateDeleteCommand(String path) {
//        return generateDeleteCommand(path, false, -1, false);
//    }

    public String getPath() {
        return path;
    }

    public static DeleteCommand generateDeleteCommand(String path) {
        return generateDeleteCommand(path, false, -1);
    }

    public static DeleteCommand generateDeleteCommand(String path, boolean syncLogFlag) {
        return generateDeleteCommand(path, false, -1, false, syncLogFlag);
    }

    public static DeleteCommand generateDeleteCommand(String path, boolean versionFlag, int version) {
        return generateDeleteCommand(path, versionFlag, version, false);
    }

    public static DeleteCommand generateDeleteCommand(
            String path, boolean versionFlag, int version, boolean retryFlag) {
        return generateDeleteCommand(path, versionFlag, version, retryFlag, false);
    }

    public static DeleteCommand generateDeleteCommand(
            String path, boolean versionFlag, int version, boolean retryFlag, boolean syncLogFlag) {
        return generateDeleteCommand(path, versionFlag, version, retryFlag, syncLogFlag, false);
    }
    public static DeleteCommand generateDeleteCommand(
            String path, boolean versionFlag, int version, boolean retryFlag,
            boolean syncLogFlag, boolean highPriorityFlag) {
        DeleteCommand cmd = new DeleteCommand(highPriorityFlag);
        ArrayList<String> tokens = new ArrayList<>();
        tokens.add("delete");

        if (versionFlag) {
            tokens.add("-v");
            tokens.add(Integer.toString(version));
        }

        if (syncLogFlag) {
            tokens.add("-L");
        }

        if (retryFlag) {
            tokens.add("-retry");
        }

        tokens.add(path);
        // TODO: Figure out how to handle exceptions
        try {
            cmd.parse(tokens.toArray(new String[0]));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        return cmd;
    }

//    @Override
//    public boolean exec() throws CliException {
//        String path = args[1];
//        int version;
//        if (cl.hasOption("v")) {
//            version = Integer.parseInt(cl.getOptionValue("v"));
//        } else {
//            version = -1;
//        }
//
//        try {
//            zk.delete(path, version);
//        } catch (IllegalArgumentException ex) {
//            throw new MalformedPathException(ex.getMessage());
//        } catch (KeeperException | InterruptedException ex) {
//            throw new CliWrapperException(ex);
//        }
//        return false;
//    }

}
