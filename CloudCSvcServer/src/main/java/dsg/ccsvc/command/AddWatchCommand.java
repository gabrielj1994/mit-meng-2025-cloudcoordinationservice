package dsg.ccsvc.command;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent;
import org.apache.commons.cli.*;
//import org.apache.zookeeper.AddWatchMode;
//import org.apache.zookeeper.KeeperException;
//import org.apache.zookeeper.cli.CliCommand;
//import org.apache.zookeeper.cli.CliException;
//import org.apache.zookeeper.cli.CliParseException;
//import org.apache.zookeeper.cli.CliWrapperException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * addWatch command for cli.
 * Matches the ZooKeeper API addWatch()
 */
public class AddWatchCommand extends CSvcCommand {
    // TODO: Fix; This is broken from remove ZK dependency

    private static final Options options = new Options();
//    private static final AddWatchMode defaultMode = AddWatchMode.PERSISTENT_RECURSIVE;

    private CommandLine cl;
//    private AddWatchMode mode = defaultMode;

    private WatchConsumerWrapper<WatchInput> watchCallable = null;

    static {
//        options.addOption("m", true, "");
        OptionGroup watchType = new OptionGroup();
        watchType.addOption(new Option("exist", false, "Trigger on node create or delete"));
        watchType.addOption(new Option("children", false, "Trigger on node children change"));
        watchType.addOption(new Option("data", false, "Trigger on node data change"));
        watchType.setRequired(true);
        options.addOptionGroup(watchType);
        options.addOption("p", false, "Maintain watch after trigger");
    }

    public AddWatchCommand() {
        super("addWatch", "[-exist | -children | -data] [-p] path");
    }

    @Override
    public CSvcCommand parse(String[] cmdArgs) throws ParseException {
        DefaultParser parser = new DefaultParser();
//        try {
        cl = parser.parse(options, cmdArgs);
//        } catch (ParseException ex) {
//            throw new CliParseException(ex);
//        }

        args = Arrays.copyOfRange(cl.getArgs(), 1, cl.getArgs().length);
        if (args.length < 1) {
            throw new ParseException(getUsageStr());
        }

//        if (cl.hasOption("m")) {
//            try {
////                mode = AddWatchMode.valueOf(cl.getOptionValue("m").toUpperCase());
//            } catch (IllegalArgumentException e) {
//                throw new ParseException(getUsageStr());
//            }
//        }

        return this;
    }

    public CommandOutput executeManager(DataLayerMgrBase manager) throws InvalidServiceStateException {
        CommandOutput result = new CommandOutput();
        String output = "";
        WatchedEvent.WatchType type;
        if (cl.hasOption("exist")) {
            type = WatchedEvent.WatchType.EXIST;
        } else if (cl.hasOption("children")) {
            type = WatchedEvent.WatchType.CHILDREN;
        } else if (cl.hasOption("data")) {
            type = WatchedEvent.WatchType.DATA;
        } else {
            throw new InvalidServiceStateException(String.format("CRITICAL ERROR - Unsupported watch type"));
        }

        boolean persistentFlag = cl.hasOption("p");
        Path filepath = Paths.get(this.getArgIndex(0));
        result.setPath(filepath.toString());


        if (Objects.isNull(watchCallable)) {
            watchCallable = new WatchConsumerWrapper<>(
                                    generateDefaultCallable(filepath.toString(), type),
                                    persistentFlag, type);
        }

        // TODO: Consider if addWatch needs lease
        int retryCount = 0;
        int retryMax = 10;
        boolean commandResultFlag = false;
        while (!commandResultFlag && retryCount < retryMax) {
            retryCount++;
            commandResultFlag = manager.addWatch(filepath.toString(), watchCallable);
        }

        result.setErrorCode(commandResultFlag ? 0 : 1);
        return result;
    }

    public void setWatchCallable(BiConsumer<DataLayerMgrBase, WatchInput> watchCallable, boolean persistentWatchFlag) {
        this.watchCallable = new WatchConsumerWrapper<>(watchCallable, persistentWatchFlag, WatchedEvent.WatchType.DATA);
    }

    public static BiConsumer<DataLayerMgrBase, WatchInput> generateDefaultCallable(String path, WatchedEvent.WatchType type) {
        return (manager, watchInput) -> {
            System.out.println(String.format("Watch triggered [path=%s, type=%s]", path, type));
        };
    }

    public static AddWatchCommand generateAddWatchCommand(
            String path, WatchedEvent.WatchType type, BiConsumer<DataLayerMgrBase, WatchInput> watchCallable,
            boolean persistentWatchFlag) throws InvalidServiceStateException {
        AddWatchCommand cmd = new AddWatchCommand();
        if (Objects.isNull(type)) {
            throw new InvalidServiceStateException("ERROR - generateAddWatchCommand " +
                    "- Required 'type' parameter is null");
        }
        watchCallable = Objects.isNull(watchCallable) ? generateDefaultCallable(path, type) : watchCallable;
        cmd.watchCallable = new WatchConsumerWrapper<>(watchCallable, persistentWatchFlag, type);

        ArrayList<String> tokens = new ArrayList<>();
        tokens.add("addWatch");

        tokens.add("-"+type.getCmdStr());

        if (persistentWatchFlag) {
            tokens.add("-p");
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
}
