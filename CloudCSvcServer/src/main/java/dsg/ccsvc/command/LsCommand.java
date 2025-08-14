package dsg.ccsvc.command;

import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.datalayer.DataLayerMgrRDB;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.InvalidServiceStateException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
//import org.apache.zookeeper.KeeperException;
//import org.apache.zookeeper.ZKUtil;
//import org.apache.zookeeper.cli.CliCommand;
//import org.apache.zookeeper.cli.*;
//import org.apache.zookeeper.data.Stat;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiConsumer;

import dsg.ccsvc.watch.WatchedEvent.WatchType;

@Slf4j
public class LsCommand extends CSvcCommand {

    private static Options options = new Options();
//    private String[] args;
    private CommandLine cl;

    // TODO: Consider if this is proper design for watches
//    private Consumer<CommandOutput> watchCallable = null;

    private WatchConsumerWrapper<WatchInput> watchCallable = null;

    static {
        options.addOption("?", false, "help");
        options.addOption("s", false, "stat");
        options.addOption("w", false, "watch");
        options.addOption("R", false, "recurse");
        options.addOption("L", false, "log sync");
        options.addOption(new Option("retry", false, "enable retries"));

    }

    public LsCommand() {
//        super("ls", "[-s] [-w] [-R] [-L sync_row] path");
        super("ls", "[-s] [-w] [-R] [-L] [-retry] path");
    }

    private void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ls [options] path", options);
    }

//    @Override
    public CSvcCommand parse(String[] cmdArgs) throws ParseException {
        DefaultParser parser = new DefaultParser();
//        try {
        cl = parser.parse(options, cmdArgs);
//        } catch (ParseException ex) {
//            throw new CliParseException(ex);
//        }

        args = Arrays.copyOfRange(cl.getArgs(), 1, cl.getArgs().length);
        if (cl.hasOption("?")) {
            printHelp();
        }
        if (args.length < 1) {
            throw new ParseException(getUsageStr());
        }

//        retainCompatibility(cmdArgs);

        return this;
    }

    // TODO: Consider if this is needed. It's logic from ZK for backward compatibility
//    private void retainCompatibility(String[] cmdArgs) throws ParseException {
//        // get path [watch]
//        if (args.length > 2) {
//            // rewrite to option
//            cmdArgs[2] = "-w";
//            err.println("'ls path [watch]' has been deprecated. " + "Please use 'ls [-w] path' instead.");
//            DefaultParser parser = new DefaultParser();
////            try {
//            cl = parser.parse(options, cmdArgs);
////            } catch (ParseException ex) {
////                throw new CliParseException(ex);
////            }
//            args = cl.getArgs();
//        }
//    }

    public CommandOutput executeManager(DataLayerMgrBase manager) throws InvalidServiceStateException {
        CommandOutput result = new CommandOutput();
        String output = "";
        Path filepath = Paths.get(this.getArgIndex(0));
        // TODO: Properly finish watches
        boolean watchFlag = cl.hasOption("w");
        boolean statsFlag = cl.hasOption("s");
        boolean syncLogFlag = cl.hasOption("L");
//        int syncRow = syncLogFlag ? Integer.parseInt(cl.getOptionValue("L")) : 0;
        // TODO: Implement recursive flag
        boolean recursiveFlag = cl.hasOption("R");
        boolean retryFlag = cl.hasOption("retry");

        int retryMax = retryFlag ? RETRY_THRESHOLD : 1;
        int retryCount = 0;

//        if (statsFlag && recursiveFlag) {
//            // TODO: consider how to best communicate unsupported arguments
//            throw new IllegalArgumentException("Unsupported parameter combination " +
//                    "- cannot fetch stats and recurse subdirectories");
////                    "- -s & -R");
//        }
        // TODO: Consider how to handle blocking flag; if it should be exposed or simply evaluated
        boolean blockingFlag = watchFlag || statsFlag || syncLogFlag;
//        result.setChildrenList(manager.retrieveChildren(filepath.toString(), statsFlag, watchCallable, blockingFlag));
        while (result.getErrorCode() != 0 && retryCount < retryMax) {
            retryCount++;
            // TODO: Consider a better design; current feels adhoc and messy
            result = manager.retrieveChildren(filepath.toString(), statsFlag,
                    watchFlag ? watchCallable : null, blockingFlag,
                    syncLogFlag);
            if (retryFlag && result.getErrorCode() != 0) {
                // TODO: Consider different logic depending on exact error code
                // TODO: Consider what happens if it fails for non SQL reasons
                // NOTE: Log retry
                System.out.println(String.format(
                        "DEBUG - LsCommand - Retrying failed retrieve [path=%s, retry_count=%d]",
                        filepath.toString(), retryCount));
            }
        }
        return result;
//        return manager.retrieveChildren(filepath.toString(), statsFlag,
//                watchFlag ? watchCallable : null, blockingFlag,
//                syncLogFlag, syncRow);
        // ===== ORIGINAL GET CODE
    }

    public CommandOutput executeManager(DataLayerMgrRDB manager) {
        CommandOutput result = new CommandOutput();
        String output = "";
        Path filepath = Paths.get(this.getArgIndex(0));
        // TODO: Properly finish watches
        boolean watchFlag = cl.hasOption("w");
        boolean statsFlag = cl.hasOption("s");
        boolean syncLogFlag = cl.hasOption("L");
//        int syncRow = syncLogFlag ? Integer.parseInt(cl.getOptionValue("L")) : 0;
        // TODO: Implement recursive flag
        boolean recursiveFlag = cl.hasOption("R");
        boolean retryFlag = cl.hasOption("retry");

        int retryMax = retryFlag ? RETRY_THRESHOLD : 1;
        int retryCount = 0;

//        if (statsFlag && recursiveFlag) {
//            // TODO: consider how to best communicate unsupported arguments
//            throw new IllegalArgumentException("Unsupported parameter combination " +
//                    "- cannot fetch stats and recurse subdirectories");
////                    "- -s & -R");
//        }
        // TODO: Consider how to handle blocking flag; if it should be exposed or simply evaluated
        boolean blockingFlag = watchFlag || statsFlag || syncLogFlag;
//        result.setChildrenList(manager.retrieveChildren(filepath.toString(), statsFlag, watchCallable, blockingFlag));
        while (result.getErrorCode() != 0 && retryCount < retryMax) {
            retryCount++;
            // TODO: Consider a better design; current feels adhoc and messy
            result = manager.retrieveChildren(filepath.toString(), statsFlag,
                    watchFlag ? watchCallable : null, blockingFlag,
                    syncLogFlag);
            if (retryFlag && result.getErrorCode() != 0) {
                // TODO: Consider different logic depending on exact error code
                // TODO: Consider what happens if it fails for non SQL reasons
                // NOTE: Log retry
                System.out.println(String.format(
                        "DEBUG - LsCommand - Retrying failed retrieve [path=%s, retry_count=%d]",
                        filepath.toString(), retryCount));
            }
        }
        return result;
//        return manager.retrieveChildren(filepath.toString(), statsFlag,
//                watchFlag ? watchCallable : null, blockingFlag,
//                syncLogFlag, syncRow);
        // ===== ORIGINAL GET CODE
    }

//
//    @Override
//    public boolean exec() throws CliException {
//        if (args.length < 2) {
//            throw new MalformedCommandException(getUsageStr());
//        }
//
//        String path = args[1];
//        boolean watch = cl.hasOption("w");
//        boolean withStat = cl.hasOption("s");
//        boolean recursive = cl.hasOption("R");
//        try {
//            if (recursive) {
//                ZKUtil.visitSubTreeDFS(zk, path, watch, (rc, path1, ctx, name) -> out.println(path1));
//            } else {
//                Stat stat = withStat ? new Stat() : null;
//                List<String> children = zk.getChildren(path, watch, stat);
//                printChildren(children, stat);
//            }
//        } catch (IllegalArgumentException ex) {
//            throw new MalformedPathException(ex.getMessage());
//        } catch (KeeperException | InterruptedException ex) {
//            throw new CliWrapperException(ex);
//        }
//        return watch;
//    }

//    public LsCommand() {
//        super("ls", "[-s] [-w] [-R] path");
//    }

    // TODO: Clean up API calls
    public static LsCommand generateLsCommand(String path) {
        return generateLsCommand(path, false,
                false, false, null, false);
    }

    public static LsCommand generateLsCommand(
            String path, boolean statsFlag, boolean recursiveFlag, boolean watchFlag,
            BiConsumer<DataLayerMgrBase, WatchInput> watchCallable, boolean persistentWatchFlag) {
        return generateLsCommand(path, statsFlag,
                recursiveFlag, watchFlag, watchCallable,
                persistentWatchFlag, false);
    }

    public static LsCommand generateLsCommand(
            String path, boolean statsFlag, boolean recursiveFlag, boolean watchFlag,
            BiConsumer<DataLayerMgrBase, WatchInput> watchCallable, boolean persistentWatchFlag, boolean syncLogFlag) {
        return generateLsCommand(path, statsFlag, recursiveFlag, watchFlag, watchCallable,
                                 persistentWatchFlag, syncLogFlag, false);
    }

    public static LsCommand generateLsCommand(
            String path, boolean statsFlag, boolean recursiveFlag, boolean watchFlag,
            BiConsumer<DataLayerMgrBase, WatchInput> watchCallable, boolean persistentWatchFlag, boolean syncLogFlag,
            boolean retryFlag) {
        LsCommand cmd = new LsCommand();
        ArrayList<String> tokens = new ArrayList<>();
        tokens.add("ls");
        if (statsFlag) {
            tokens.add("-s");
        }

        if (watchFlag) {
            tokens.add("-w");
            cmd.watchCallable = new WatchConsumerWrapper<>(
                    watchCallable,
                    persistentWatchFlag,
                    WatchType.CHILDREN);
        }

        // TODO: Implement recursiveFlag
        if (recursiveFlag) {
            tokens.add("-R");
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

//    private void printChildren(List<String> children, Stat stat) {
//        Collections.sort(children);
//        out.append("[");
//        boolean first = true;
//        for (String child : children) {
//            if (!first) {
//                out.append(", ");
//            } else {
//                first = false;
//            }
//            out.append(child);
//        }
//        out.append("]\n");
//        if (stat != null) {
//            new StatPrinter(out).print(stat);
//        }
//    }

    public void setWatchCallable(BiConsumer<DataLayerMgrBase, WatchInput> watchCallable, boolean persistentWatchFlag) {
        this.watchCallable = new WatchConsumerWrapper<>(watchCallable, persistentWatchFlag, WatchType.CHILDREN);
    }
}
