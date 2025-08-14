package dsg.ccsvc.command;

import dsg.ccsvc.datalayer.DataLayerManager;
import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchWrapperBase;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.NotImplementedException;

import java.io.PrintStream;
import java.util.Map;

public abstract class CSvcCommand {

    protected static final int RETRY_THRESHOLD = 5;

    protected DataLayerManager manager;
    protected PrintStream out;
    protected PrintStream err;
    protected String cmdStr;
    protected String optionStr;

    // TODO: Consider different implementations
//    protected static Options options = new Options();
    protected String[] args;
    protected CommandLine cl;

    protected WatchWrapperBase watcher;

    /**
     * a CLI command with command string and options.
     * Using System.out and System.err for printing
     * @param cmdStr the string used to call this command
     * @param optionStr the string used to call this command
     */
    public CSvcCommand(String cmdStr, String optionStr) {
        this.out = System.out;
        this.err = System.err;
        this.cmdStr = cmdStr;
        this.optionStr = optionStr;
    }

    public void setManager(DataLayerManager manager) {
        this.manager = manager;
    }

    /**
     * get the string used to call this command
     */
    public String getCmdStr() {
        return cmdStr;
    }

    /**
     * get the option string
     */
    public String getOptionStr() {
        return optionStr;
    }

    /**
     * get a usage string, contains the command and the options
     */
    public String getUsageStr() {
        return cmdStr + " " + optionStr;
    }

    protected String[] getArgs() {
        return args;
    }

    protected void setArgs(String[] args) {
        this.args = args;
    }

    /**
     * add this command to a map. Use the command string as key.
     * @param cmdMap
     */
    public void addToMap(Map<String, CSvcCommand> cmdMap) {
        cmdMap.put(cmdStr, this);
    }

    public int getArgCount() {
        return args.length;
    }

    public String getArgIndex(int index) {
        // TODO: Out of bounds handle
        return index >= 0 && index < args.length ? args[index] : null;
    }

    public boolean hasOption(String option) {
        return cl.hasOption(option);
    }

    public String getOptionValue(String option) {
        return cl.getOptionValue(option);
    }

    public void execute() {
        System.out.println("unsupported");
    }

    public CommandOutput executeManager() {
        System.out.println("unsupported");
        return null;
    }

    public CommandOutput executeManager(DataLayerMgrBase manager) throws InvalidServiceStateException {
        throw new NotImplementedException();
    }

    public WatchWrapperBase getWatcher() {
        return watcher;
    }

    public void setWatcher(WatchWrapperBase watcher) {
        this.watcher = watcher;
    }

    /**
     * parse the command arguments
     * @param cmdArgs
     * @return this CSvcCommand
//     * @throws CliParseException
     */
    public abstract CSvcCommand parse(String[] cmdArgs) throws ParseException;
//    public abstract CSvcCommand parse(String[] cmdArgs) throws CliParseException;

    @Override
    public String toString() {
        return String.format("[args=%s, cmd=%s]", String.join(",", args));
    }
}
