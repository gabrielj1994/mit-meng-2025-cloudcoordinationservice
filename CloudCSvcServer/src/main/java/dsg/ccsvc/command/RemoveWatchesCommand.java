package dsg.ccsvc.command;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchedEvent;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.NotImplementedException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

public class RemoveWatchesCommand extends CSvcCommand {
    // TODO: Fix; This is broken from remove ZK dependency

    private static final Options options = new Options();
//    private static final AddWatchMode defaultMode = AddWatchMode.PERSISTENT_RECURSIVE;

    private CommandLine cl;
//    private AddWatchMode mode = defaultMode;

    static {
//        options.addOption("m", true, "");
        OptionGroup watchType = new OptionGroup();
        watchType.addOption(new Option("exist", false, "Delete exist watch on node"));
        watchType.addOption(new Option("children", false, "Delete children watch on node"));
        watchType.addOption(new Option("data", false, "Delete data watch on node"));
        watchType.addOption(new Option("all", false, "Delete all watch on node"));
        watchType.setRequired(true);
        options.addOptionGroup(watchType);
    }

    public RemoveWatchesCommand() {
        super("removewatches", "[-exist | -children | -data | -all] path");
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

        return this;
    }

    @Override
    public void execute() {
        throw new NotImplementedException();
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
        } else if (cl.hasOption("all")) {
            type = null;
        } else {
            throw new InvalidServiceStateException(String.format("CRITICAL ERROR - Unsupported watch type"));
        }

        Path filepath = Paths.get(this.getArgIndex(0));
        result.setPath(filepath.toString());

        int retryCount = 0;
        int retryMax = 10;
        boolean commandResultFlag = false;
        while (!commandResultFlag && retryCount < retryMax) {
            retryCount++;
            commandResultFlag = manager.removeWatch(filepath.toString(), type);
        }

        result.setErrorCode(commandResultFlag ? 0 : 1);
        return result;
    }

    public static RemoveWatchesCommand generateRemoveWatchCommand(String path, WatchedEvent.WatchType type) {
        RemoveWatchesCommand cmd = new RemoveWatchesCommand();

        ArrayList<String> tokens = new ArrayList<>();
        tokens.add("removewatches");

        if (Objects.isNull(type)) {
            tokens.add("-all");
        } else {
            tokens.add("-"+type.getCmdStr());
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
