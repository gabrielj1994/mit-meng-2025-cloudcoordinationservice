package dsg.ccsvc.client;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.command.*;
import dsg.ccsvc.datalayer.DataLayerMgrRDB;
import dsg.ccsvc.datalayer.DataLayerMgrSharedFS;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchedEvent;
import lombok.extern.slf4j.Slf4j;
//import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiConsumer;

//import static org.example.util.ZKConfigs.*;

// TODO: Move to ZKServer Module
@Slf4j
public class CSvcClient implements AutoCloseable {

    // TODO: Consider Rename
    public enum ManagerType {
        DB_VARIANT,
        FS_VARIANT;

        public static ManagerType fromInteger(int value) {

            for (ManagerType type : ManagerType.values()) {
                if (value == type.ordinal()) {
                    return type;
                }
            }
            return null;
        }
    }
//    CSvcManagerAmazonDB_CmdAgnostic manager;
    DataLayerMgrBase manager;

    private CSvcClient(DataLayerMgrBase manager) throws InvalidServiceStateException {
        // TODO: Deal with these exceptions elsewhere
//        while (Objects.isNull(manager)) {
//            try {
//                manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/");
//            } catch (IOException | ClassNotFoundException e) {
//                throw new RuntimeException(e);
//            } catch (InvalidServiceStateException e) {
//                manager = null;
//            }
//        }
//        try {
//            manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/");
            // TODO: Consider adding start/stop/autocloseable
            // TODO: Validations
        if (manager.isInitialized()) {
            throw new InvalidServiceStateException("CSvcClient Error - CSvc manager must not be initialized");
        }
        this.manager = manager;
//            manager.startManager();
//        }
    }

    public void startClient() throws InvalidServiceStateException {
        try {
            manager.startManager();
        } catch (IOException e) {
            throw new InvalidServiceStateException(e);
        }
    }

    public boolean isRunning() {
        return manager.isRunning();
    }

    // TODO: Properly handle exceptions
    public boolean exists(String path) throws InvalidServiceStateException {
        CommandOutput output = GetCommand.generateGetCommand(path).executeManager(manager);
        return output.getExistFlag();
    }

    public CommandOutput create(String path, byte[] contents,
                                boolean ephemeralFlag, boolean sequentialFlag, boolean retryFlag)
                            throws InvalidServiceStateException {
        CommandOutput result = null;
        CreateCommand cmd = CreateCommand
                .generateCreateCommand(path, contents,
                        ephemeralFlag, sequentialFlag, retryFlag);
//        System.out.println(String.format("REMOVEME - [cmd=%s]", cmd.toString()));
//        return CreateCommand
//                .generateCreateCommand(path, contents, ephemeralFlag, sequentialFlag)
//                .executeManager(manager);
        // TODO: Properly set up retries in the library
        boolean clientRetryFlag = true;
        int retryCount = 0;
        int retryThreshold = 10;
        while (clientRetryFlag && retryCount < retryThreshold) {
//            System.out.printf("\t\tREMOVE ME - CSvcClient create [retry=%d]\n", retryCount);
            try {
                retryCount++;
                result = cmd.executeManager(manager);
//                clientRetryFlag = false;
                clientRetryFlag = result.getErrorCode() != 0;
            } catch (Exception e) {
                // NOTE: Do nothing; print exception
////                System.out.println("REMOVEME");
                log.error("===\nCSvcClient - create - Retry\n===");
                log.error(e.toString());
                log.error("===\nCSvcClient - create - Retry\n===");
            }
        }

        if (clientRetryFlag) {
            throw new InvalidServiceStateException("Failed create");
        }
        return result;
    }

    public String readData(String path, boolean returnNullIfPathNotExists) throws InvalidServiceStateException {
        String result = "null";
        CommandOutput output = GetCommand.generateGetCommand(path).executeManager(manager);
        if (!output.getExistFlag() && !returnNullIfPathNotExists) {
            // TODO: handle exceptions that were initially from ZK
            throw new InvalidServiceStateException(String.format("Failed read [path=%s]", path));
        } else if (output.getExistFlag()) {
            // TODO: Consider how to support arbitrary byte data in nodes
            result = new String(output.getData(), StandardCharsets.UTF_8);
        }
        return result;
    }

//    public String readDataSync(String path, boolean returnNullIfPathNotExists) throws InvalidServiceStateException {
//        String result = "null";
////        CommandOutput output = GetCommand.generateGetCommand(path).executeManager(manager);
//        CommandOutput output = GetCommand.generateGetCommand(path, ).executeManager(manager);
//        if (!output.getExistFlag() && !returnNullIfPathNotExists) {
//            // TODO: handle exceptions that were initially from ZK
//            throw new InvalidServiceStateException(String.format("Failed read [path=%s]", path));
//        } else if (output.getExistFlag()) {
//            // TODO: Consider how to support arbitrary byte data in nodes
//            result = new String(output.getData(), StandardCharsets.UTF_8);
//        }
//        return result;
//    }

    public void writeData(String path, byte[] contents,
                          boolean retryFlag) throws InvalidServiceStateException {
        CommandOutput result = null;
        //generateSetCommand
        SetCommand cmd = SetCommand.generateSetCommand(path, contents, false,
                                            false, -1, retryFlag);
        //generateSetCommand(String path, boolean statsFlag, byte[] content,
        //                                                boolean versionFlag, int version, boolean retryFlag)
//        System.out.println(String.format("REMOVEME - [cmd=%s]", cmd.toString()));
//        return CreateCommand
//                .generateCreateCommand(path, contents, ephemeralFlag, sequentialFlag)
//                .executeManager(manager);
        // TODO: Properly set up retries in the library
        boolean clientRetryFlag = true;
        int retryCount = 0;
        int retryThreshold = 10;
        while (clientRetryFlag && retryCount < retryThreshold) {
            try {
                retryCount++;
                result = cmd.executeManager(manager);
//                clientRetryFlag = false;
                clientRetryFlag = result.getErrorCode() != 0;
            } catch (Exception e) {
                // NOTE: Do nothing; print exception
////                System.out.println("REMOVEME");
                log.error("===\nCSvcClient - set - Retry\n===");
                log.error(e.toString());
                log.error("===\nCSvcClient - set - Retry\n===");
            }
        }

        if (clientRetryFlag) {
            throw new InvalidServiceStateException("Failed set");
        }
//        return result;
    }

    public Set<String> getChildren(String path) throws InvalidServiceStateException {
        CommandOutput output = LsCommand.generateLsCommand(path, false, false, false,
                                                            null, false, true,
                                                            true).executeManager(manager);
        return Collections.unmodifiableSet(output.getChildrenSet());
    }

    // TODO: Consider if this needs return boolean for output state
    public void delete(String path) throws InvalidServiceStateException {
        DeleteCommand.generateDeleteCommand(path, false, -1, true, true).executeManager(manager);
    }

    public void recursiveDelete(String path) throws InvalidServiceStateException {
        Set<String> children = getChildren(path);
//        if (children.isEmpty()) {
//            delete(path);
//        } else {
        for (String childNodeName : children) {
            recursiveDelete(path.concat("/").concat(childNodeName));
        }
//        }
        delete(path);
    }

    public boolean unsubscribeChildrenChanges(String path) throws InvalidServiceStateException {
        RemoveWatchesCommand cmd = RemoveWatchesCommand
                                    .generateRemoveWatchCommand(path, WatchedEvent.WatchType.CHILDREN);
        boolean retryFlag = true;
        int retryCount = 0;
        int retryMax = 10;
        CommandOutput output = null;
        while (retryFlag && retryCount < retryMax) {
            output = cmd.executeManager(manager);
            retryFlag = output.getErrorCode() != 0;
            retryCount++;
        }

        if (retryFlag) {
            log.error(String.format("CRITICAL ERROR - unsubscribeChildrenChanges " +
                    "- Failed to remove watch [path=%s]", path));
            throw new InvalidServiceStateException(String.format("CRITICAL ERROR - unsubscribeChildrenChanges " +
                    "- Failed to remove watch [path=%s]", path));
        }
        return true;
    }

    public String subscribeNodeChange(String path, BiConsumer<DataLayerMgrBase, WatchInput> watchCallable,
                                                boolean persistentWatchFlag) throws InvalidServiceStateException {
        GetCommand cmd = GetCommand.generateGetCommand(path);
        cmd.setWatchCallable(watchCallable, persistentWatchFlag);
        // TODO: Handle retries more elegantly
        boolean retryFlag = true;
        int retryCount = 0;
        int retryMax = 10;
        CommandOutput output = null;
        while (retryFlag && retryCount < retryMax) {
            output = cmd.executeManager(manager);
            retryFlag = output.getErrorCode() != 0;
            retryCount++;
        }

        if (retryFlag) {
            log.error(String.format("CRITICAL ERROR - subscribeNodeChange - Failed to add watch [path=%s]", path));
            throw new InvalidServiceStateException(String.format("CRITICAL ERROR - subscribeNodeChange " +
                                                    "- Failed to add watch [path=%s]", path));
        }

        return new String(output.getData(), StandardCharsets.UTF_8);
    }

    // TODO: Update watchCallables to biconsumers (won't always have access to manager)
    public Optional<Set<String>> subscribeChildChanges(
            String path,
            BiConsumer<DataLayerMgrBase, WatchInput> watchCallable) throws InvalidServiceStateException {
//        BiConsumer<CommandOutput, CSvcManagerAmazonDB_CmdAgnostic> watchCallable) {
        CommandOutput output = LsCommand.generateLsCommand(
                                path, false, false, true,
                                watchCallable, true, true, true)
                                .executeManager(manager);
        return Optional.ofNullable(output.getExistFlag() ? output.getChildrenSet() : null);
    }

    public void close() throws InterruptedException {
        manager.close();
    }

    // TODO: remove this after redesign to biconsumer
    public DataLayerMgrBase getManager() {
        return manager;
    }

    public static CSvcClient generateFSManagerClient(String workDirPath, String cSvcNodeRoot)
                                            throws InvalidServiceStateException, IOException, ClassNotFoundException {
        return new CSvcClient(new DataLayerMgrSharedFS(workDirPath, cSvcNodeRoot));
    }

    // TODO: Implement DB variant credentials wrapper
    public static CSvcClient generateDBManagerClient(Object credentials, String cSvcNodeRoot)
                                            throws InvalidServiceStateException, IOException, ClassNotFoundException {
        return new CSvcClient(new DataLayerMgrRDB(credentials, cSvcNodeRoot));
    }

//    public boolean subscribeStateChanges(
//            String path,
//            Consumer<CommandOutput> watchCallable) {
////        BiConsumer<CommandOutput, CSvcManagerAmazonDB_CmdAgnostic> watchCallable) {
//        CommandOutput output = GetCommand.generateGetCommand(
//                        path, false, false, true, watchCallable, true)
//                .executeManager(manager);
//        return output.getExistFlag();
//    }
}
