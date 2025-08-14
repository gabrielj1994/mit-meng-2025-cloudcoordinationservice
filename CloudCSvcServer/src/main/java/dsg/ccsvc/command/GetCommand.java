package dsg.ccsvc.command;

import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.datalayer.DataLayerMgrRDB;
import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.InvalidServiceStateException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiConsumer;

import dsg.ccsvc.watch.WatchedEvent.WatchType;

@Slf4j
public class GetCommand extends CSvcCommand {

    private static Options options = new Options();
//    private String[] args;
    private CommandLine cl;

    // TODO: Consider if this is proper design for watches
//    private Consumer<CommandOutput> watchCallable = null;

    // TODO: Consider if this biconsumer<> design is better
//    private BiConsumer<CommandOutput, CSvcManagerAmazonDB_CmdAgnostic> watchCallableTwo = null;
    private WatchConsumerWrapper<WatchInput> watchCallable = null;


    static {
        options.addOption("s", false, "stats");
        options.addOption("w", false, "watch");
        options.addOption("c", false, "cache");
    }

    public GetCommand() {
        super("get", "[-s] [-w] [-c] path");
    }

    @Override
    public CSvcCommand parse(String[] cmdArgs) throws ParseException {
        DefaultParser parser = new DefaultParser();
//        try {
        cl = parser.parse(options, cmdArgs);
//        } catch (ParseException ex) {
//            throw new ParseException(ex);
//        }
        args = Arrays.copyOfRange(cl.getArgs(), 1, cl.getArgs().length);
//        args = cl.getArgs();
        if (args.length < 1) {
            throw new ParseException(getUsageStr());
        }

//        retainCompatibility(cmdArgs);

        return this;
    }

    // TODO: Consider other implementations
//    public void setWatchCallable(Consumer<CommandOutput> watchCallable) {
//        this.watchCallable = watchCallable;
//    }

    // TODO: add a way to register a callable without a command (or its own command)
    public void setWatchCallable(BiConsumer<DataLayerMgrBase, WatchInput> watchCallable, boolean persistentWatchFlag) {
        this.watchCallable = new WatchConsumerWrapper<>(watchCallable, persistentWatchFlag, WatchType.DATA);
    }

    // TODO: Consider if this is needed. It's logic from ZK for backward compatibility
//    private void retainCompatibility(String[] cmdArgs) throws ParseException {
//        // get path [watch]
//        if (args.length > 2) {
//            // rewrite to option
//            cmdArgs[2] = "-w";
//            err.println("'get path [watch]' has been deprecated. " + "Please use 'get [-s] [-w] path' instead.");
//            DefaultParser parser = new DefaultParser();
//            try {
//                cl = parser.parse(options, cmdArgs);
//            } catch (ParseException ex) {
//                throw new CliParseException(ex);
//            }
//            args = cl.getArgs();
//        }
//    }

    public CommandOutput executeManager(DataLayerMgrBase manager) throws InvalidServiceStateException {
        CommandOutput result = new CommandOutput();
        String output = "";
        // TODO: synchronize on manager
        // TODO: consider how to leverage log validation
//        long logValidationValue = manager.getLogValidationToken();
        // TODO: redesign argument handling
        // TODO: check that watchflag is properly moved to options and not args
        int i = 0;
        // TODO: confirm watchflag properly moved to options
//        boolean watchFlag = false;
//        if (this.getArgCount() > 1) {
//            watchFlag = true;
//            i += 1;
//        }
        Path filepath = Paths.get(this.getArgIndex(i));
//        String data = this.getArgCount() == 2 ? "null" : this.getArgIndex(1);
//        String filename = filepath.getFileName().toString();
//        String parentPath = filepath.getParent().toString();
//        System.out.println(String.format("REMOVE ME! [operation=%s, filepath=%s]",
//                "get", filepath));
//        boolean parentMissing = Objects.isNull(manager.getFromMappingStructure(parentPath, "pathChildrenMap"));
//        if (parentMissing) {
//            // TODO: consider refactoring for cleaner implementation
////            output = "Create Operation Failed - Parent path not found.";
////            result.populateError(1, output);
////            return result;
//            System.out.println(String.format("Create Operation Failed - Parent path not found."));
//        }

        // TODO: Properly finish watches
        boolean watchFlag = cl.hasOption("w");

        if (watchFlag && Objects.isNull(watchCallable)) {
            throw new IllegalArgumentException("Illegal command configuration " +
                    "- enabled watch flag must set watchCallable");
        }
//        if (cl.hasOption("w")) {
//            watchFlag = true;
//        }

        boolean statsFlag = cl.hasOption("s");
//        if (cl.hasOption("s")) {
//            statsFlag = true;
//        }

        // TODO: Consider how to communicate cache is not compatible with -w. Stats (-s) can
        //  be made to pull from the inmemory data structures as well
        boolean cacheFlag = cl.hasOption("c");
//        if (cl.hasOption("c")) {
//            cacheFlag = true;
//        }

        if (cacheFlag && watchFlag) {
            // TODO: consider how to best communicate unsupported arguments
            throw new IllegalArgumentException("Unsupported parameter combination " +
                    "- cannot register watch and read from cache");
            // TODO: Consider ZK exceptions
//            throw new MalformedCommandException("-c cannot be combined with -s or -e. Containers cannot be ephemeral or sequential.");
        }

        // TODO: redesign this to get error trace from data layer about what went wrong in case of errors
//        LogRecord logRecord = manager.retrieveFromLog(filepath.toString(), statsFlag);
        // TODO: Figure out how to set up readthrough option to be configurable for GETCMD
        boolean readThroughFlag = true;
        LogRecord logRecord = cacheFlag ?
                manager.retrieveFromCache(filepath.toString(), statsFlag, readThroughFlag) :
                manager.retrieveFromLog(filepath.toString(), statsFlag, watchFlag ? watchCallable : null);
        if (!Objects.isNull(logRecord)) {
//            result.setOutput(new String(logRecord.getData(), StandardCharsets.UTF_8));
            if (logRecord.getType().equals(LogRecord.LogRecordType.DELETE)) {
                System.out.println(String.format("GetCmd - Missing node path [path=%s]", filepath));
                output = String.format("Get Command Failed - Missing Record [path=%s]", filepath);
                result.setOutput(output);
                result.populateError(1, output);
                result.setExistFlag(false);
            } else {
                result.setOutput(logRecord.toString());
                result.setData(logRecord.getData());
                result.setExistFlag(true);
                if (statsFlag) {
                    result.setRecordStats(logRecord.getStats());
                }
            }
            // TODO: add watch support
//            if (watchFlag) {
//                manager.addWatch(filepath.toString(), new WatchedEvent(filepath.toString(), watchCallable));
//            }
        } else {
            // TODO: RETRY?
//            result.setOutput(String.format("Missing record [path=%s]", filepath));
            output = String.format("Get Command Failed - Missing Record [path=%s]", filepath);
            result.setOutput(output);
            result.populateError(1, output);
            result.setExistFlag(false);
        }
        return result;
    }

    public CommandOutput executeManager(DataLayerMgrRDB manager) {
        CommandOutput result = new CommandOutput();
        String output = "";
        // TODO: synchronize on manager
        // TODO: consider how to leverage log validation
//        long logValidationValue = manager.getLogValidationToken();
        // TODO: redesign argument handling
        // TODO: check that watchflag is properly moved to options and not args
        int i = 0;
        // TODO: confirm watchflag properly moved to options
//        boolean watchFlag = false;
//        if (this.getArgCount() > 1) {
//            watchFlag = true;
//            i += 1;
//        }
        Path filepath = Paths.get(this.getArgIndex(i));
//        String data = this.getArgCount() == 2 ? "null" : this.getArgIndex(1);
//        String filename = filepath.getFileName().toString();
//        String parentPath = filepath.getParent().toString();
//        System.out.println(String.format("REMOVE ME! [operation=%s, filepath=%s]",
//                "get", filepath));
//        boolean parentMissing = Objects.isNull(manager.getFromMappingStructure(parentPath, "pathChildrenMap"));
//        if (parentMissing) {
//            // TODO: consider refactoring for cleaner implementation
////            output = "Create Operation Failed - Parent path not found.";
////            result.populateError(1, output);
////            return result;
//            System.out.println(String.format("Create Operation Failed - Parent path not found."));
//        }

        // TODO: Properly finish watches
        boolean watchFlag = cl.hasOption("w");

        if (watchFlag && Objects.isNull(watchCallable)) {
            throw new IllegalArgumentException("Illegal command configuration " +
                    "- enabled watch flag must set watchCallable");
        }
//        if (cl.hasOption("w")) {
//            watchFlag = true;
//        }

        boolean statsFlag = cl.hasOption("s");
//        if (cl.hasOption("s")) {
//            statsFlag = true;
//        }

        // TODO: Consider how to communicate cache is not compatible with -w. Stats (-s) can
        //  be made to pull from the inmemory data structures as well
        boolean cacheFlag = cl.hasOption("c");
//        if (cl.hasOption("c")) {
//            cacheFlag = true;
//        }

        if (cacheFlag && watchFlag) {
            // TODO: consider how to best communicate unsupported arguments
            throw new IllegalArgumentException("Unsupported parameter combination " +
                    "- cannot register watch and read from cache");
            // TODO: Consider ZK exceptions
//            throw new MalformedCommandException("-c cannot be combined with -s or -e. Containers cannot be ephemeral or sequential.");
        }

        // TODO: redesign this to get error trace from data layer about what went wrong in case of errors
//        LogRecord logRecord = manager.retrieveFromLog(filepath.toString(), statsFlag);
        // TODO: Figure out how to set up readthrough option to be configurable for GETCMD
        boolean readThroughFlag = true;
        LogRecord logRecord = cacheFlag ?
                manager.retrieveFromCache(filepath.toString(), statsFlag, readThroughFlag) :
                manager.retrieveFromLog(filepath.toString(), statsFlag, watchFlag ? watchCallable : null);
        if (!Objects.isNull(logRecord)) {
//            result.setOutput(new String(logRecord.getData(), StandardCharsets.UTF_8));
            if (logRecord.getType().equals(LogRecord.LogRecordType.DELETE)) {
                System.out.println(String.format("GetCmd - Missing node path [path=%s]", filepath));
                output = String.format("Get Command Failed - Missing Record [path=%s]", filepath);
                result.setOutput(output);
                result.populateError(1, output);
                result.setExistFlag(false);
            } else {
                result.setOutput(logRecord.toString());
                result.setData(logRecord.getData());
                result.setExistFlag(true);
                if (statsFlag) {
                    result.setRecordStats(logRecord.getStats());
                }
            }
            // TODO: add watch support
//            if (watchFlag) {
//                manager.addWatch(filepath.toString(), new WatchedEvent(filepath.toString(), watchCallable));
//            }
        } else {
            // TODO: RETRY?
//            result.setOutput(String.format("Missing record [path=%s]", filepath));
            output = String.format("Get Command Failed - Missing Record [path=%s]", filepath);
            result.setOutput(output);
            result.populateError(1, output);
            result.setExistFlag(false);
        }
        return result;
    }

    @Override
    public CommandOutput executeManager() {
        return manager.get(this);
    }

    //TODO: remove testing helper;
    // Consider keeping this in a command factory
    public static GetCommand generateGetCommand(String path) {
        return generateGetCommand(path, false, false, false, null, false);
    }

    //TODO: remove testing helper;
    // Consider keeping this in a command factory
    public static GetCommand generateSyncGetCommand(String path) {
        return generateGetCommand(path, false, false, false, null, false);
    }

    // TODO: clean up functions
    public static GetCommand generateGetCommand(
            String path, boolean statsFlag, boolean cacheFlag,
            boolean watchFlag, BiConsumer<DataLayerMgrBase, WatchInput> watchCallable) {
        return generateGetCommand(path, statsFlag, cacheFlag, watchFlag, watchCallable, false);
    }


    public static GetCommand generateGetCommand(
            String path, boolean statsFlag, boolean cacheFlag, boolean watchFlag,
            BiConsumer<DataLayerMgrBase, WatchInput> watchCallable, boolean persistentWatchFlag) {
        GetCommand cmd = new GetCommand();
//        cmd.watchCallable = watchCallable;
        cmd.watchCallable = new WatchConsumerWrapper<>(watchCallable, persistentWatchFlag, WatchType.DATA);

        ArrayList<String> tokens = new ArrayList<>();
        tokens.add("get");
        if (statsFlag) {
            tokens.add("-s");
        }

        if (cacheFlag) {
            tokens.add("-c");
        }

        if (watchFlag) {
            tokens.add("-w");
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

//    public int getArgCount() {
//        return args.length;
//    }

//
//    @Override
//    public boolean exec() throws CliException {
//        boolean watch = cl.hasOption("w");
//        String path = args[1];
//        Stat stat = new Stat();
//        byte[] data;
//        try {
//            data = zk.getData(path, watch, stat);
//        } catch (IllegalArgumentException ex) {
//            throw new MalformedPathException(ex.getMessage());
//        } catch (KeeperException | InterruptedException ex) {
//            throw new CliWrapperException(ex);
//        }
//        data = (data == null) ? "null".getBytes() : data;
//        out.println(new String(data, UTF_8));
//        if (cl.hasOption("s")) {
//            new StatPrinter(out).print(stat);
//        }
//        return watch;
//    }

}
