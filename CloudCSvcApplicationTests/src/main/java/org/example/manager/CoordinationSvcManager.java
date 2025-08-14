package org.example.manager;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.client.CSvcClient;
import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.example.util.ZKConfigs.*;

@Slf4j
public class CoordinationSvcManager implements AutoCloseable {


    // TODO: Consider new name
    private CSvcClient cSvcClient;

    public CoordinationSvcManager(CSvcClient.ManagerType managerType) throws InvalidServiceStateException {
//        cSvcClient = new CSvcClient();
        // TODO: Refactor this logic to a more appropriate util class
        try {
            switch (managerType) {
                case DB_VARIANT:
                    cSvcClient = CSvcClient.generateDBManagerClient(null, "/");
                    break;
                case FS_VARIANT:
//                    cSvcClient = CSvcClient.generateFSManagerClient("/mnt/efs/fs1/CSVC_TEST", "/");
                    cSvcClient = CSvcClient.generateFSManagerClient("/mnt/efs/fs2_oz/CSVC_TEST", "/");
                    break;
                default:
                    throw new InvalidServiceStateException("Invalid value");
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new InvalidServiceStateException(e);
        }
//        cSvcClient = new CSvcClient();
    }

    public void start() throws InvalidServiceStateException {
        cSvcClient.startClient();
    }

    public boolean isRunning() {
        return cSvcClient.isRunning();
    }

    // TODO: Remove this and redesign callables to biconsumer
    public DataLayerMgrBase getManager() {
        return cSvcClient.getManager();
    }

    public String getLeaderNodeData() throws InvalidServiceStateException {
        return cSvcClient.readData(ELECTION_MASTER, true);
    }

    public void electForMaster() throws InvalidServiceStateException {
        if (!cSvcClient.exists(ELECTION_NODE)) {
//            cSvcClient.create(ELECTION_MASTER, "election node", CreateMode.PERSISTENT);
            cSvcClient.create(ELECTION_MASTER,
                    "election node".getBytes(StandardCharsets.UTF_8),
                    false, false, true);
        }
        try {
//            cSvcClient.create(
//                    ELECTION_MASTER,
//                    getHostPostOfServer(),
//                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
//                    CreateMode.EPHEMERAL);
            cSvcClient.create(ELECTION_MASTER,
                    getHostPostOfServer().getBytes(StandardCharsets.UTF_8),
                    true, false, true);
            // TODO: handle exception specifics
        } catch (ZkNodeExistsException e) {
            log.error("Master already created!!, {}", e);
        }
    }

    public boolean masterExists() throws InvalidServiceStateException {
        return cSvcClient.exists(ELECTION_MASTER);
    }

    public void addToLiveNodes(String nodeName, String data) throws InvalidServiceStateException {
        if (!cSvcClient.exists(LIVE_NODES)) {
//            cSvcClient.create(LIVE_NODES, "all live nodes are displayed here", CreateMode.PERSISTENT);
            cSvcClient.create(LIVE_NODES,
                    "all live nodes are displayed here".getBytes(StandardCharsets.UTF_8),
                    false, false, true);
        }
        String childNode = LIVE_NODES.concat("/").concat(nodeName);
        if (cSvcClient.exists(childNode)) {
            return;
        }
//        cSvcClient.create(childNode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        cSvcClient.create(childNode, data.getBytes(StandardCharsets.UTF_8),
                true, false, true);
    }

    public List<String> getLiveNodesRegisterWatch(WatchConsumerWrapper<WatchInput> callable)
                                                                                throws InvalidServiceStateException {
        // TODO: This needs to be synchronized, so that a triggered watch doesn't cause issues
//        List<String> nodesInElection = new ArrayList<>(cSvcClient.getChildren(ELECTION_NODE_2));
//        if (!cSvcClient.exists(LIVE_NODES)) {
        // TODO: This is not really needed, subscribe will return null if not exists, and its the same logic
//            throw new RuntimeException("No node /liveNodes exists");
//        }
        Optional<Set<String>> optSet = cSvcClient.subscribeChildChanges(LIVE_NODES, callable);
        if (!optSet.isPresent()) {
            throw new InvalidServiceStateException("Failed to fetch children and register watch");
        }
        return new ArrayList<>(optSet.get());
    }

    public List<String> getLiveNodes() throws InvalidServiceStateException {
        if (!cSvcClient.exists(LIVE_NODES)) {
            throw new InvalidServiceStateException("No node /liveNodes exists");
        }
        return new ArrayList<>(cSvcClient.getChildren(LIVE_NODES));
    }

    public void addToAllNodes(String nodeName, String data) throws InvalidServiceStateException {
        if (!cSvcClient.exists(ALL_NODES)) {
            cSvcClient.create(ALL_NODES, "all live nodes are displayed here".getBytes(StandardCharsets.UTF_8),
                    false, false, true);
        }
        String childNode = ALL_NODES.concat("/").concat(nodeName);
        if (cSvcClient.exists(childNode)) {
            return;
        }
//        cSvcClient.create(childNode, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        cSvcClient.create(childNode, data.getBytes(StandardCharsets.UTF_8),
                false, false, true);
    }

    public List<String> getAllNodesRegisterWatch(WatchConsumerWrapper<WatchInput> callable)
                                                                                throws InvalidServiceStateException {
        // TODO: This needs to be synchronized, so that a triggered watch doesn't cause issues
//        List<String> nodesInElection = new ArrayList<>(cSvcClient.getChildren(ELECTION_NODE_2));
//        if (!cSvcClient.exists(LIVE_NODES)) {
        // TODO: This is not really needed, subscribe will return null if not exists, and its the same logic
//            throw new RuntimeException("No node /liveNodes exists");
//        }
        Optional<Set<String>> optSet = cSvcClient.subscribeChildChanges(ALL_NODES, callable);
        if (!optSet.isPresent()) {
            // TODO: Consider exception vs return null
//            throw new Exception("Failed to fetch children and register watch");
            throw new InvalidServiceStateException("Failed to fetch children and register watch");
//            return null;
        }
        return new ArrayList<>(optSet.get());
    }

    public List<String> getAllNodes() throws InvalidServiceStateException {
        if (!cSvcClient.exists(ALL_NODES)) {
//            throw new RuntimeException("No node /allNodes exists");
            throw new InvalidServiceStateException("No node /allNodes exists");
        }
        return new ArrayList<>(cSvcClient.getChildren(ALL_NODES));
    }

    public void deleteNodeFromCluster(String node) throws InvalidServiceStateException {
        cSvcClient.delete(ALL_NODES.concat("/").concat(node));
        cSvcClient.delete(LIVE_NODES.concat("/").concat(node));
    }

    public void createTempParent() throws InvalidServiceStateException {
//        if (!cSvcClient.exists(PERSISTENCE_NODE)) {
        cSvcClient.create(TEMP_NODE,
                "node used for temp storage".getBytes(StandardCharsets.UTF_8),
                false, false, false);
//        }
    }

    public void addToTemporary(String nodeName, byte[] data) throws InvalidServiceStateException {
//        if (!zkClient.exists(PERSISTENCE_NODE)) {
//            // TODO: Consider if this needs to be checked, can just catch create exception
//        }
        cSvcClient.create(TEMP_NODE.concat("/").concat(nodeName), data,
                true, false, false);
    }

    public void updateTemporary(String nodeName, byte[] data) throws InvalidServiceStateException {
        cSvcClient.writeData(TEMP_NODE.concat("/").concat(nodeName), data,  false);
    }

    public void createPersistenceParent() throws InvalidServiceStateException {
//        if (!cSvcClient.exists(PERSISTENCE_NODE)) {
        cSvcClient.create(PERSISTENCE_NODE,
                            "node used for permanent storage".getBytes(StandardCharsets.UTF_8),
                            false, false, false);
//        }
    }

    public void addToPersistence(String nodeName, byte[] data) throws InvalidServiceStateException {
//        if (!zkClient.exists(PERSISTENCE_NODE)) {
//            // TODO: Consider if this needs to be checked, can just catch create exception
//        }
        cSvcClient.create(PERSISTENCE_NODE.concat("/").concat(nodeName), data,
                false, false, false);
    }

    public void removeFromPersistence(String nodeName) throws InvalidServiceStateException {
        cSvcClient.delete(PERSISTENCE_NODE.concat("/").concat(nodeName));
    }

    public void updatePersistence(String nodeName, byte[] data) throws InvalidServiceStateException {
        cSvcClient.writeData(PERSISTENCE_NODE.concat("/").concat(nodeName), data,  false);
    }

    public void createAllParentNodes() throws InvalidServiceStateException {
        if (!cSvcClient.exists(ALL_NODES)) {
            cSvcClient.create(ALL_NODES,
                    "all live nodes are displayed here".getBytes(StandardCharsets.UTF_8),
                    false, false, true);
        }

        if (!cSvcClient.exists(LIVE_NODES)) {
            cSvcClient.create(LIVE_NODES,
                    "all live nodes are displayed here".getBytes(StandardCharsets.UTF_8),
                    false, false, true);
        }

        if (!cSvcClient.exists(ELECTION_NODE)) {
            cSvcClient.create(ELECTION_NODE,
                    "election node".getBytes(StandardCharsets.UTF_8),
                    false, false, true);
        }
    }

    public String getLeaderNodeData2RegisterWatch(WatchConsumerWrapper<WatchInput> callable)
                                                                                throws InvalidServiceStateException {
        // TODO: This needs to be synchronized, so that a triggered watch doesn't cause issues
//        if (!cSvcClient.exists(ELECTION_NODE_2)) {
        // TODO: This is not really needed, subscribe will return null if not exists, and its the same logic
//            throw new RuntimeException("No node /election2 exists");
//        }
//        List<String> nodesInElection = new ArrayList<>(cSvcClient.getChildren(ELECTION_NODE_2));
        Optional<Set<String>> optSet = cSvcClient.subscribeChildChanges(ELECTION_NODE_2, callable);
        if (!optSet.isPresent()) {
            throw new InvalidServiceStateException("Failed to fetch children and register watch");
//            throw new RuntimeException("Failed to fetch children and register watch");
        }
        List<String> nodesInElection = new ArrayList<>(optSet.get());
        Collections.sort(nodesInElection);
//        System.out.println(String.format("REMOVEME - getLeaderNodeData2 - [nodesInElection=%s]",
//                nodesInElection.toString()));
        String masterZNode = nodesInElection.get(0);
        return getNodeData(ELECTION_NODE_2.concat("/").concat(masterZNode));
    }

    public String getLeaderNodeData2() throws InvalidServiceStateException {
        if (!cSvcClient.exists(ELECTION_NODE_2)) {
            throw new InvalidServiceStateException("No node /election2 exists");
//            throw new RuntimeException("No node /election2 exists");
        }
        List<String> nodesInElection = new ArrayList<>(cSvcClient.getChildren(ELECTION_NODE_2));
        Collections.sort(nodesInElection);
//        System.out.println(String.format("REMOVEME - getLeaderNodeData2 - [nodesInElection=%s]",
//                nodesInElection.toString()));
        String masterZNode = nodesInElection.get(0);
        return getNodeData(ELECTION_NODE_2.concat("/").concat(masterZNode));
    }

    public String getNodeData(String path) throws InvalidServiceStateException {
        return cSvcClient.readData(path, false);
    }

//    public String getNodeDataSync(String path) throws InvalidServiceStateException {
//        return cSvcClient.readDataSync(path, false);
//    }

    public void createNodeInElectionZnode(String data) throws InvalidServiceStateException {
//        System.out.println(String.format("REMOVEME - DEBUG - createNodeInElectionZnode [data=%s]", data));
        if (!cSvcClient.exists(ELECTION_NODE_2)) {
//            cSvcClient.create(ELECTION_NODE_2, "election node", CreateMode.PERSISTENT);
            cSvcClient.create(ELECTION_NODE_2, "election node".getBytes(StandardCharsets.UTF_8),
                    false, false, true);
        }
//        cSvcClient.create(ELECTION_NODE_2.concat("/node"), data, CreateMode.EPHEMERAL_SEQUENTIAL);
//        System.out.println(String.format("REMOVEME - DEBUG - createNodeInElectionZnode [data=%s]", data));
        cSvcClient.create(ELECTION_NODE_2.concat("/node"), data.getBytes(StandardCharsets.UTF_8),
                true, true, true);
    }

    public void registerChildrenChangeWatcher(String path, WatchConsumerWrapper<WatchInput> watchCallable)
                                                                        throws InvalidServiceStateException {
        cSvcClient.subscribeChildChanges(path, watchCallable);
    }

    public void close() throws InterruptedException {
        cSvcClient.close();
    }

//    public void registerZkSessionStateListener(WatchConsumerWrapper<CommandOutput> watchCallable) {
//        cSvcClient.subscribeStateChanges(watchCallable);
//    }
}
