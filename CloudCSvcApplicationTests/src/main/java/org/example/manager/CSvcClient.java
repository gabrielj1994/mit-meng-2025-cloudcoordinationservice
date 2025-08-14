//package org.example.manager;
//
//import dsg.tbd.modular.messaging.*;
//import dsg.tbd.modular.tbd.CSvcManagerAmazonDB_CmdAgnostic;
//import lombok.extern.slf4j.Slf4j;
//import org.I0Itec.zkclient.IZkChildListener;
//import org.I0Itec.zkclient.IZkStateListener;
//import org.I0Itec.zkclient.exception.ZkNodeExistsException;
//import org.apache.zookeeper.CreateMode;
//import org.apache.zookeeper.ZooDefs;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.Set;
//import java.util.function.BiConsumer;
//import java.util.function.Consumer;
//
//import static org.example.util.ZKConfigs.*;
//import static org.example.util.ZKConfigs.ELECTION_NODE;
//
//// TODO: Move to ZKServer Module
//@Slf4j
//public class CSvcClient {
////    CSvcManagerAmazonDB_CmdAgnostic manager;
////
////    public CSvcClient() {
////        // TODO: Deal with these exceptions elsewhere
////        try {
////            manager = new CSvcManagerAmazonDB_CmdAgnostic(null, "/");
////        } catch (IOException | ClassNotFoundException e) {
////            throw new RuntimeException(e);
////        }
////
////        // TODO: Consider adding start/stop/autocloseable
////        manager.startManager();
////    }
////
////    // TODO: Properly handle exceptions
////    public boolean exists(String path) {
////        CommandOutput output = GetCommand.generateGetCommand(path).executeManager(manager);
////        return output.getExistFlag();
////    }
////
////    public CommandOutput create(String path, byte[] contents,
////                                boolean ephemeralFlag, boolean sequentialFlag) {
////        return CreateCommand
////                .generateCreateCommand(path, contents, ephemeralFlag, sequentialFlag)
////                .executeManager(manager);
////    }
////
////    public String readData(String path, boolean returnNullIfPathNotExists) {
////        String result = "null";
////        CommandOutput output = GetCommand.generateGetCommand(path).executeManager(manager);
////        if (!output.getExistFlag() && !returnNullIfPathNotExists) {
////            // TODO: handle exceptions that were initially from ZK
////            throw new RuntimeException();
////        } else if (output.getExistFlag()) {
////            // TODO: Consider how to support arbitrary byte data in nodes
////            result = new String(output.getData(), StandardCharsets.UTF_8);
////        }
////        return result;
////    }
////
////    public Set<String> getChildren(String path) {
////        CommandOutput output = LsCommand.generateLsCommand(path).executeManager(manager);
////        return Collections.unmodifiableSet(output.getChildrenSet());
////    }
////
////    // TODO: Consider if this needs return boolean for output state
////    public void delete(String path) {
////        DeleteCommand.generateDeleteCommand(path).executeManager(manager);
////    }
////
////    // TODO: Update watchCallables to biconsumers (won't always have access to manager)
////    public boolean subscribeChildChanges(
////            String path,
////            Consumer<CommandOutput> watchCallable) {
//////        BiConsumer<CommandOutput, CSvcManagerAmazonDB_CmdAgnostic> watchCallable) {
////        CommandOutput output = LsCommand.generateLsCommand(
////                path, false, false, true, watchCallable, true)
////                .executeManager(manager);
////        return output.getExistFlag();
////    }
////
////    public boolean subscribeStateChanges(
////            String path,
////            Consumer<CommandOutput> watchCallable) {
//////        BiConsumer<CommandOutput, CSvcManagerAmazonDB_CmdAgnostic> watchCallable) {
////        CommandOutput output = GetCommand.generateGetCommand(
////                        path, false, false, true, watchCallable, true)
////                .executeManager(manager);
////        return output.getExistFlag();
////    }
//
//    // =====
//    // MOVE ABOVE TO ZKServer
//    // =====
//    public String getLeaderNodeData() {
//        return this.readData(ELECTION_MASTER, true);
//    }
//
//    public void electForMaster() {
//        if (!this.exists(ELECTION_NODE)) {
////            this.create(ELECTION_MASTER, "election node", CreateMode.PERSISTENT);
//            this.create(ELECTION_MASTER,
//                    "election node".getBytes(StandardCharsets.UTF_8),
//                    false, false);
//        }
//        try {
////            this.create(
////                    ELECTION_MASTER,
////                    getHostPostOfServer(),
////                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
////                    CreateMode.EPHEMERAL);
//            this.create(ELECTION_MASTER,
//                        getHostPostOfServer().getBytes(StandardCharsets.UTF_8),
//                    true, false);
//            // TODO: handle exception specifics
//        } catch (ZkNodeExistsException e) {
//            log.error("Master already created!!, {}", e);
//        }
//    }
//
//    public boolean masterExists() {
//        return this.exists(ELECTION_MASTER);
//    }
//
//    public void addToLiveNodes(String nodeName, String data) {
//        if (!this.exists(LIVE_NODES)) {
////            this.create(LIVE_NODES, "all live nodes are displayed here", CreateMode.PERSISTENT);
//            this.create(LIVE_NODES,
//                    "all live nodes are displayed here".getBytes(StandardCharsets.UTF_8),
//                    false, false);
//        }
//        String childNode = LIVE_NODES.concat("/").concat(nodeName);
//        if (this.exists(childNode)) {
//            return;
//        }
////        this.create(childNode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//        this.create(childNode, data.getBytes(StandardCharsets.UTF_8),
//                false, false);
//    }
//
//    public List<String> getLiveNodes() {
//        if (!this.exists(LIVE_NODES)) {
//            throw new RuntimeException("No node /liveNodes exists");
//        }
//        return new ArrayList<>(this.getChildren(LIVE_NODES));
//    }
//
//    public void addToAllNodes(String nodeName, String data) {
//        if (!this.exists(ALL_NODES)) {
//            this.create(ALL_NODES, "all live nodes are displayed here".getBytes(StandardCharsets.UTF_8),
//                    false, false);
//        }
//        String childNode = ALL_NODES.concat("/").concat(nodeName);
//        if (this.exists(childNode)) {
//            return;
//        }
////        this.create(childNode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        this.create(childNode, data.getBytes(StandardCharsets.UTF_8),
//                false, false);
//    }
//
//    public List<String> getAllNodes() {
//        if (!this.exists(ALL_NODES)) {
//            throw new RuntimeException("No node /allNodes exists");
//        }
//        return new ArrayList<>(this.getChildren(ALL_NODES));
//    }
//
//    public void deleteNodeFromCluster(String node) {
//        this.delete(ALL_NODES.concat("/").concat(node));
//        this.delete(LIVE_NODES.concat("/").concat(node));
//    }
//
//    public void createAllParentNodes() {
//        if (!this.exists(ALL_NODES)) {
//            this.create(ALL_NODES,
//                    "all live nodes are displayed here".getBytes(StandardCharsets.UTF_8),
//                    false, false);
//        }
//
//        if (!this.exists(LIVE_NODES)) {
//            this.create(LIVE_NODES,
//                    "all live nodes are displayed here".getBytes(StandardCharsets.UTF_8),
//                    false, false);
//        }
//
//        if (!this.exists(ELECTION_NODE)) {
//            this.create(ELECTION_NODE,
//                    "election node".getBytes(StandardCharsets.UTF_8),
//                    false, false);
//        }
//    }
//
//    public String getLeaderNodeData2() {
//        if (!this.exists(ELECTION_NODE_2)) {
//            throw new RuntimeException("No node /election2 exists");
//        }
//        List<String> nodesInElection = new ArrayList<>(this.getChildren(ELECTION_NODE_2));
//        Collections.sort(nodesInElection);
//        String masterZNode = nodesInElection.get(0);
//        return getZNodeData(ELECTION_NODE_2.concat("/").concat(masterZNode));
//    }
//
//    public String getZNodeData(String path) {
//        return this.readData(path, false);
//    }
//
//    public void createNodeInElectionZnode(String data) {
//        if (!this.exists(ELECTION_NODE_2)) {
////            this.create(ELECTION_NODE_2, "election node", CreateMode.PERSISTENT);
//            this.create(ELECTION_NODE_2, "election node".getBytes(StandardCharsets.UTF_8),
//                    false, false);
//        }
////        this.create(ELECTION_NODE_2.concat("/node"), data, CreateMode.EPHEMERAL_SEQUENTIAL);
//        this.create(ELECTION_NODE_2.concat("/node"), data.getBytes(StandardCharsets.UTF_8),
//                true, true);
//    }
//
//    // TODO: Clean up original listeners
////    public void registerChildrenChangeWatcher(String path, IZkChildListener iZkChildListener) {
////        this.subscribeChildChanges(path, iZkChildListener);
////    }
////
////    public void registerZkSessionStateListener(IZkStateListener iZkStateListener) {
////        this.subscribeStateChanges(iZkStateListener);
////    }
//}
