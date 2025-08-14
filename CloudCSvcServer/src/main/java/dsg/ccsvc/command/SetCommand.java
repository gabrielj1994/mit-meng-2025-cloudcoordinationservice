/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dsg.ccsvc.command;

import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.datalayer.ManagerLeaseHandlerBase;
import dsg.ccsvc.util.DebugConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
//import org.apache.zookeeper.KeeperException;
//import org.apache.zookeeper.cli.CliCommand;
//import org.apache.zookeeper.cli.*;
//import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

@Slf4j
public class SetCommand extends CSvcCommand {

    private static Options options = new Options();
//    private String[] args;
    private CommandLine cl;

    static {
        options.addOption("s", false, "stats");
        options.addOption("v", true, "version");
        options.addOption(new Option("retry", false, "enable retries"));
    }

    public SetCommand() {
        super("set", "[-s] [-v version] [-retry] path data");
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
        if (args.length < 2) {
            throw new ParseException(getUsageStr());
        }

        return this;
    }

    public CommandOutput executeManager(DataLayerMgrBase manager) throws InvalidServiceStateException {
        // TODO: consider the tension between command abstraction and database transaction wrapping
        CommandOutput result = new CommandOutput();
        String output = "";
        CommitOptions.CommitOptionsBuilder optionsBldr = CommitOptions.builder();
        // TODO: consider how to better design setup

        // TODO: synchronize on manager
        long logValidationValue = manager.getLogValidationToken();
        Path filepath = Paths.get(this.getArgIndex(0));
        // TODO: there was an off by 1 error here. Confirm not the same in other places
        String data = this.getArgCount() == 1 ? "null" : this.getArgIndex(1);
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
//        cl.getOptionValue("version");
        String filename = filepath.getFileName().toString();
        String parentPath = filepath.getParent().toString();
        if (DebugConstants.DEBUG_FLAG) {
            System.out.println(String.format("REMOVE ME! [operation=%s, filename=%s, parentPath=%s, data=%s," +
                            " versionFlag=%b, version=%d]",
                    "set", filename, parentPath, data, versionFlag, expectedVersion));
        }
        // TODO: add support for ephemeral nodes and watches
        // VALIDATION: Node must exist
        // TODO: Soft Preliminary Validations should just be from inmemory structs
//        LogRecord targetLogRecord = manager.retrieveFromLog(filepath.toString());
//        if (Objects.isNull(targetLogRecord)) {
//            // TODO: consider refactoring for cleaner implementation
//            output = "Set Operation Failed - Target node not found.";
//            result.populateError(1, output);
//            System.out.println(String.format(output));
//            return result;
//        }
        LogRecord logRecord = new LogRecord(filepath.toString(),
                data.getBytes(StandardCharsets.UTF_8),
                LogRecord.LogRecordType.SET);
        // TODO: validate version
//        boolean commitFlag = manager.commitToLog(logInstant, logRecord);
        int retryMax = retryFlag ? RETRY_THRESHOLD : 1;
        boolean commitFlag = false;
        int retryCount = 0;

//        boolean leaseClaim = false;
//        // TODO: Consider what to do for retries
//        while (!leaseClaim) {
//            try {
//                // TODO: Implement deleting timed out waitlists
//                leaseClaim = manager.submitSoftLeaseClaim().get();
//            } catch (Exception e) {
//                System.out.println("ERROR - SetCommand - An error occurred.");
//                e.printStackTrace(System.out);
//                output = "Set Operation Failed.";
//                result.populateError(1, output);
//                return result;
//            }
//        }

        try (ManagerLeaseHandlerBase ignored = manager.claimSoftLeaseHandler(false).get()) {
//        try (ManagerLeaseHandlerBase ignored = manager.startLeaseHandler()) {
            // TODO: Combine the two into general catch all calls once confirmed to work
            while (!commitFlag && retryCount < retryMax) {
                retryCount++;
//                commitFlag = manager.commitToLog(logValidationValue, logRecord, expectedVersion, versionFlag, false);
                commitFlag = manager.commitToLog(logValidationValue, logRecord, optionsBldr.build());
                if (retryFlag && !commitFlag) {
                    // TODO: Consider what happens if it fails for non SQL reasons
                    // NOTE: Log retry
                    System.out.println(String.format(
                            "DEBUG - SetCommand - Retrying failed commit [path=%s, retry_count=%d]",
                            filepath.toString(), retryCount));
                }
            }
        } catch (ExecutionException e) {
            log.error(String.format("SetCommand - An error occurred [path=%s]", filepath), e);
            throw new InvalidServiceStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag
            log.error("SetCommand - Thread was interrupted", e);
        } catch (Exception e) {
            log.error(String.format("SetCommand - Unexpected Exception [path=%s]", filepath), e);
            throw new InvalidServiceStateException(e);
        }
//        commitFlag = manager.commitToLog(logValidationValue, logRecord, expectedVersion, versionFlag, false);
//        if (versionFlag) {
//            commitFlag = manager.commitToLog(logValidationValue, logRecord, expectedVersion, versionFlag, false);
//        } else {
//            commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
//        }
//        boolean commitFlag = true;
        if (commitFlag) {
            // TODO: refactor this, can `be handled by manager in commit to log?
            //  Or should metadatastore management be handled by command abstraction?
            output = "Set Operation Succeeded.";
            result.setOutput(output);
//            manager.updateMetadataStore(logRecord);
        } else {
            // TODO: RETRY with VERSION VALIDATION
            System.out.println("WARN - SetCommand - An error occurred.");
//            e.printStackTrace();
            output = "Set Operation Failed.";
            result.populateError(1, output);
        }
        if (DebugConstants.DEBUG_FLAG) {
            System.out.println(String.format(output));
        }
        return result;
    }

//    public CommandOutput executeManager(CSvcManagerAmazonDB_CmdAgnostic manager) throws InvalidServiceStateException {
//        // TODO: consider the tension between command abstraction and database transaction wrapping
//        CommandOutput result = new CommandOutput();
//        String output = "";
//        CommitOptions.CommitOptionsBuilder optionsBldr = CommitOptions.builder();
//        // TODO: consider how to better design setup
//
//        // TODO: synchronize on manager
//        long logValidationValue = manager.getLogValidationToken();
//        Path filepath = Paths.get(this.getArgIndex(0));
//        // TODO: there was an off by 1 error here. Confirm not the same in other places
//        String data = this.getArgCount() == 1 ? "null" : this.getArgIndex(1);
//        // TODO: implement version validation
////        boolean versionFlag = false;
////        int expectedVersion = -1;
////        if(cl.hasOption("v")) {
////            versionFlag = true;
////            expectedVersion = Integer.parseInt(cl.getOptionValue("v"));
////        }
//        boolean retryFlag = cl.hasOption("retry");
//
//        boolean versionFlag = cl.hasOption("v");
//        int expectedVersion = versionFlag ? Integer.parseInt(cl.getOptionValue("v")) : -1;
//        optionsBldr.validateVersionFlag(versionFlag);
//        optionsBldr.expectedVersion(expectedVersion);
////        cl.getOptionValue("version");
//        String filename = filepath.getFileName().toString();
//        String parentPath = filepath.getParent().toString();
//        if (DebugConstants.DEBUG_FLAG) {
//            System.out.println(String.format("REMOVE ME! [operation=%s, filename=%s, parentPath=%s, data=%s," +
//                            " versionFlag=%b, version=%d]",
//                    "set", filename, parentPath, data, versionFlag, expectedVersion));
//        }
//        // TODO: add support for ephemeral nodes and watches
//        // VALIDATION: Node must exist
//        // TODO: Soft Preliminary Validations should just be from inmemory structs
////        LogRecord targetLogRecord = manager.retrieveFromLog(filepath.toString());
////        if (Objects.isNull(targetLogRecord)) {
////            // TODO: consider refactoring for cleaner implementation
////            output = "Set Operation Failed - Target node not found.";
////            result.populateError(1, output);
////            System.out.println(String.format(output));
////            return result;
////        }
//        LogRecord logRecord = new LogRecord(filepath.toString(),
//                data.getBytes(StandardCharsets.UTF_8),
//                LogRecord.LogRecordType.SET);
//        // TODO: validate version
////        boolean commitFlag = manager.commitToLog(logInstant, logRecord);
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
//                System.out.println("ERROR - SetCommand - An error occurred.");
//                e.printStackTrace(System.out);
//                output = "Set Operation Failed.";
//                result.populateError(1, output);
//                return result;
//            }
//        }
//
//        try (ManagerLeaseHandlerBase ignored = manager.startLeaseHandler()) {
//            // TODO: Combine the two into general catch all calls once confirmed to work
//            while (!commitFlag && retryCount < retryMax) {
//                retryCount++;
////                commitFlag = manager.commitToLog(logValidationValue, logRecord, expectedVersion, versionFlag, false);
//                commitFlag = manager.commitToLog(logValidationValue, logRecord, optionsBldr.build());
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
////        commitFlag = manager.commitToLog(logValidationValue, logRecord, expectedVersion, versionFlag, false);
////        if (versionFlag) {
////            commitFlag = manager.commitToLog(logValidationValue, logRecord, expectedVersion, versionFlag, false);
////        } else {
////            commitFlag = manager.commitToLog(logValidationValue, logRecord, false);
////        }
////        boolean commitFlag = true;
//        if (commitFlag) {
//            // TODO: refactor this, can `be handled by manager in commit to log?
//            //  Or should metadatastore management be handled by command abstraction?
//            output = "Set Operation Succeeded.";
//            result.setOutput(output);
////            manager.updateMetadataStore(logRecord);
//        } else {
//            // TODO: RETRY with VERSION VALIDATION
//            System.out.println("An error occurred.");
////            e.printStackTrace();
//            output = "Set Operation Failed.";
//            result.populateError(1, output);
//        }
//        if (DebugConstants.DEBUG_FLAG) {
//            System.out.println(String.format(output));
//        }
//        return result;
//    }

    //TODO: remove testing helper
    public static SetCommand generateSetCommand(String path, String content) {
        SetCommand cmd = new SetCommand();
//        cmd.args = new String[]{path, content};
        cmd.setArgs(new String[]{path, content});
        return cmd;
    }

    public static SetCommand generateSetCommand(String path, byte[] content, boolean statsFlag,
                                                boolean versionFlag, int version, boolean retryFlag) {
        SetCommand cmd = new SetCommand();
//        cmd.watchCallable = watchCallable;
        ArrayList<String> tokens = new ArrayList<>();
        tokens.add("set");
        if (statsFlag) {
            tokens.add("-s");
        }

        if (versionFlag) {
            tokens.add("-v");
            tokens.add(Integer.toString(version));
        }

        if (retryFlag) {
            tokens.add("-retry");
        }

        tokens.add(path);
        if (content.length > 0) {
            // TODO: Consider how to generate a create a command with arbitrary byte content
            tokens.add(new String(content, StandardCharsets.UTF_8));
        }

        // TODO: Figure out how to handle exceptions
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

    @Override
    public String toString() {
        return String.format("[cmd=set, cl_args=%s, cl_options=%s, args=%s]",
                cl.getArgList().toString(), Arrays.asList(cl.getOptions()), Arrays.asList(args));
    }

//
//    @Override
//    public boolean exec() throws CliException {
//        String path = args[1];
//        byte[] data = args[2].getBytes(UTF_8);
//        int version;
//        if (cl.hasOption("v")) {
//            version = Integer.parseInt(cl.getOptionValue("v"));
//        } else {
//            version = -1;
//        }
//
//        try {
//            Stat stat = zk.setData(path, data, version);
//            if (cl.hasOption("s")) {
//                new StatPrinter(out).print(stat);
//            }
//        } catch (IllegalArgumentException ex) {
//            throw new MalformedPathException(ex.getMessage());
//        } catch (KeeperException | InterruptedException ex) {
//            throw new CliWrapperException(ex);
//        }
//        return false;
//    }

}
