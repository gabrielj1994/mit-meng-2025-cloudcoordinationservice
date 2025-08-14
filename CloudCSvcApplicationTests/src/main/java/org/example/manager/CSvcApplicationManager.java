package org.example.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.client.CSvcClient;
import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import io.javalin.Javalin;
import org.apache.commons.lang3.SerializationUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.example.controller.SimpleController;
import org.example.storage.DataStore;
import org.example.util.HTTPAutocloseWrapper;
import org.example.util.WebAppDemoMetadata;
import org.example.watch.csvc.AllNodesChangeCallable;
import org.example.watch.csvc.LiveNodeChangeCallable;
import org.example.watch.csvc.MasterChangeCallable;
import org.example.watch.csvc.MasterChangeCallableApproach2;

import java.util.Objects;

import static org.example.util.HTTPUtil.*;
import static org.example.util.ZKConfigs.*;

public class CSvcApplicationManager implements WebAppManager {

//    private RestTemplate restTemplate = new RestTemplate();
    // TODO: Properly initialize all autowired fields
//    private ZKSvcManager zkSvcManager;
    private CoordinationSvcManager cSvcManager;

    private DataStore dataStore;

    private Javalin msgApp;

    private SimpleController controller;

//    private IZkChildListener allNodesChangeListener;
//
//    private IZkChildListener liveNodeChangeListener;
//
//    private IZkChildListener masterChangeListener;
//
//    private IZkStateListener connectStateChangeListener;

    private WatchConsumerWrapper<WatchInput> allNodesChangeCallable;
    private WatchConsumerWrapper<WatchInput> liveNodeChangeCallable;
    private WatchConsumerWrapper<WatchInput> masterChangeCallable;
//    private WatchConsumerWrapper<CommandOutput> masterChangeCallableApproach2;

//    private boolean[] isRunning = {false};
    private boolean isRunning = false;

    private int appPort = 7100;

//    private boolean zkFlag = false;

    // TODO: Identify if constructor needs more inputs
//    public CSvcApplicationManager(DataStore dataStore, boolean zkFlag) throws InvalidServiceStateException {
    public CSvcApplicationManager(DataStore dataStore, CSvcClient.ManagerType managerType) throws InvalidServiceStateException {
//        this.zkFlag = zkFlag;
        // TODO: Set system properties at startup
        int leaderMode = Integer.parseInt(System.getProperty("leader.algo"));
        appPort = Integer.parseInt(System.getProperty("msg.port"));
//        if (zkFlag) {
//            String zkHostPort = System.getProperty("zk.url");
//            zkSvcManager = new ZKSvcManager(zkHostPort);
//            allNodesChangeListener = new AllNodesChangeListener();
//            liveNodeChangeListener = new LiveNodeChangeListener();
//            // TODO: add layer of abstraction through initialization helper functions
//            // TODO: add option to choose master listener implementation
//            if (leaderMode == 1) {
//                MasterChangeListener masterListener = new MasterChangeListener();
//                masterListener.setZkService(zkSvcManager);
//                masterChangeListener = masterListener;
//            } else {
//                MasterChangeListenerApproach2 masterListener = new MasterChangeListenerApproach2();
//                masterListener.setZkService(zkSvcManager);
//                masterChangeListener = masterListener;
//            }
//            connectStateChangeListener = new ConnectStateChangeListener();
//        } else {
            cSvcManager = new CoordinationSvcManager(managerType);
            cSvcManager.start();
            allNodesChangeCallable = new AllNodesChangeCallable(this)
                    .generateCallableConsumer();
            liveNodeChangeCallable = new LiveNodeChangeCallable(this)
                    .generateCallableConsumer();
            if (leaderMode == 1) {
                masterChangeCallable = new MasterChangeCallable(this, cSvcManager)
                        .generateCallableConsumer();
            } else {
                masterChangeCallable = new MasterChangeCallableApproach2(this, cSvcManager)
                        .generateCallableConsumer();
            }
//        }

        this.dataStore = dataStore;
        this.controller = new SimpleController(dataStore);
        initializeEndpoints();
//        isRunning[0] = true;
        synchronized (this) {
            isRunning = true;
            this.notifyAll();
        }
    }

    private void initializeEndpoints() {
        msgApp = Javalin.create();
        msgApp.start(appPort);
        msgApp.get(IDX_ENDPOINT, ctx -> ctx.html("Hello world!"));
        msgApp.get(ALL_RECORDS_ENDPOINT, controller.readAllRecordsSerializedHandler());
        msgApp.get(ALL_RECORDS_STRING_ENDPOINT, controller.readAllRecordsStringHandler());
        msgApp.get(GET_RECORD_ENDPOINT, controller.readRecordSimpleSerializeHandler());
        msgApp.get(GET_RECORD_STRING_ENDPOINT, controller.readRecordSimpleStringHandler());
        msgApp.get(KILL_APP_ENDPOINT, ctx -> {
            ctx.html("Bye world!");
            this.stopRunning();
        });

        msgApp.post(UPDATE_RECORD_ENDPOINT, controller.updateRecordSerializeHandler());
        msgApp.post(UPDATE_RECORD_STRING_ENDPOINT, controller.updateRecordSimpleHandler());
//        msgApp.get("/javalindemo", ctx -> ctx.html("Hello world!"));
//        msgApp.get("/javalindemo/records", controller.readAllRecordsHandler());
//        msgApp.get("/javalindemo/records/{key}", controller.readRecordSimpleHandler());
//        msgApp.post("/javalindemo/input", controller.updateRecordSimpleHandler());
//        msgApp.get("/javalindemo/kill", ctx -> {
//            ctx.html("Bye world!");
//            this.close();
//        });
    }
//
//    private void zkInitializationHelper() {
//        zkSvcManager.createAllParentNodes();
//
//        // add this server to cluster by creating znode under /all_nodes, with name as "host:port"
//        zkSvcManager.addToAllNodes(getHostPostOfServer(), "cluster node");
//        ZKMetadata.getClusterInfo().getAllNodes().clear();
//        ZKMetadata.getClusterInfo().getAllNodes().addAll(zkSvcManager.getAllNodes());
//
//        // check which leader election algorithm(1 or 2) need is used
//        String leaderElectionAlgo = System.getProperty("leader.algo");
//
//        // if approach 2 - create ephemeral sequential znode in /election
//        // then get children of  /election and fetch least sequenced znode, among children znodes
//        if (isEmpty(leaderElectionAlgo) || "2".equals(leaderElectionAlgo)) {
//            zkSvcManager.createNodeInElectionZnode(getHostPostOfServer());
//            ZKMetadata.getClusterInfo().setMaster(zkSvcManager.getLeaderNodeData2());
//        } else {
//            if (!zkSvcManager.masterExists()) {
//                zkSvcManager.electForMaster();
//            } else {
//                ZKMetadata.getClusterInfo().setMaster(zkSvcManager.getLeaderNodeData());
//            }
//        }
//
//        // sync person data from master
//        syncDataFromMaster();
//
//        // add child znode under /live_node, to tell other servers that this server is ready to serve
//        // read request
//        zkSvcManager.addToLiveNodes(getHostPostOfServer(), "cluster node");
//        ZKMetadata.getClusterInfo().getLiveNodes().clear();
//        ZKMetadata.getClusterInfo().getLiveNodes().addAll(zkSvcManager.getLiveNodes());
//
//        // register watchers for leader change, live nodes change, all nodes change and zk session
//        // state change
//        if (isEmpty(leaderElectionAlgo) || "2".equals(leaderElectionAlgo)) {
//            zkSvcManager.registerChildrenChangeWatcher(ELECTION_NODE_2, masterChangeListener);
//        } else {
//            zkSvcManager.registerChildrenChangeWatcher(ELECTION_NODE, masterChangeListener);
//        }
//        zkSvcManager.registerChildrenChangeWatcher(LIVE_NODES, liveNodeChangeListener);
//        zkSvcManager.registerChildrenChangeWatcher(ALL_NODES, allNodesChangeListener);
//        zkSvcManager.registerZkSessionStateListener(connectStateChangeListener);
//    }

    public synchronized void cSvcInitializationHelper() throws InvalidServiceStateException {
        // TODO: Make init reads blocking
        cSvcManager.createAllParentNodes();

        // add this server to cluster by creating znode under /all_nodes, with name as "host:port"
        cSvcManager.addToAllNodes(getHostPostOfServer(), "cluster node");
//        ZKMetadata.getClusterInfo().getAllNodes().clear();
//        ZKMetadata.getClusterInfo().getAllNodes().addAll(cSvcManager.getAllNodes());
        tbdGetAllNodesAndWatch();

        // check which leader election algorithm(1 or 2) need is used
        String leaderElectionAlgo = System.getProperty("leader.algo");

        // if approach 2 - create ephemeral sequential znode in /election
        // then get children of  /election and fetch least sequenced znode, among children znodes
        // TODO: Synchronize the section registering watches
//        if (isEmpty(leaderElectionAlgo) || "2".equals(leaderElectionAlgo)) {
//            cSvcManager.createNodeInElectionZnode(getHostPostOfServer());
//            String fetchMaster = cSvcManager.getLeaderNodeData2();
////            System.out.println(String.format("REMOVE ME - cSvcInitializationHelper - [fetchMaster=%s]", fetchMaster));
////            ZKMetadata.getClusterInfo().setMaster(cSvcManager.getLeaderNodeData2());
//            ZKMetadata.getClusterInfo().setMaster(fetchMaster);
//        } else {
//            // TODO: This algorithm is poorly designed; hard to sync; needs addWatch functionality
//            if (!cSvcManager.masterExists()) {
//                cSvcManager.electForMaster();
//            } else {
//                ZKMetadata.getClusterInfo().setMaster(cSvcManager.getLeaderNodeData());
//            }
//        }
        tbdUpdateLeaderAndWatch(leaderElectionAlgo);

        // TODO: Implement my own code that adds watch atomically with leader state update

        // sync person data from master
        syncDataFromMaster();

        // add child znode under /live_node, to tell other servers that this server is ready to serve
        // read request
        cSvcManager.addToLiveNodes(getHostPostOfServer(), "cluster node");
        // TODO: Atomically get and watch liveNodes
//        ZKMetadata.getClusterInfo().getLiveNodes().clear();
//        ZKMetadata.getClusterInfo().getLiveNodes().addAll(cSvcManager.getLiveNodes());
        tbdGetLiveNodesAndWatch();

        // register watchers for leader change, live nodes change, all nodes change and zk session
        // state change
//        if (isEmpty(leaderElectionAlgo) || "2".equals(leaderElectionAlgo)) {
//            cSvcManager.registerChildrenChangeWatcher(ELECTION_NODE_2, masterChangeCallable);
//        } else {
//            cSvcManager.registerChildrenChangeWatcher(ELECTION_NODE, masterChangeCallable);
//        }
//        cSvcManager.registerChildrenChangeWatcher(LIVE_NODES, liveNodeChangeCallable);
//        cSvcManager.registerChildrenChangeWatcher(ALL_NODES, allNodesChangeCallable);
//        cSvcManager.registerZkSessionStateListener(connectStateChangeListener);
    }

    public void tbdGetAllNodesAndWatch() throws InvalidServiceStateException {
        WebAppDemoMetadata.getClusterInfo().getAllNodes().clear();
        WebAppDemoMetadata.getClusterInfo().getAllNodes().addAll(
                cSvcManager.getAllNodesRegisterWatch(allNodesChangeCallable));
    }

    public void tbdGetLiveNodesAndWatch() throws InvalidServiceStateException {
        WebAppDemoMetadata.getClusterInfo().getLiveNodes().clear();
        WebAppDemoMetadata.getClusterInfo().getLiveNodes().addAll(
                cSvcManager.getLiveNodesRegisterWatch(liveNodeChangeCallable));
    }

    public void tbdUpdateLeaderAndWatch(String leaderElectionAlgo) throws InvalidServiceStateException {
        if (isEmpty(leaderElectionAlgo) || "2".equals(leaderElectionAlgo)) {
            // TODO: need a helper function that getleadernodedata2 & sets children watch
            //===
            cSvcManager.createNodeInElectionZnode(getHostPostOfServer());
            String fetchMaster = cSvcManager.getLeaderNodeData2RegisterWatch(masterChangeCallable);
//            System.out.println(String.format("REMOVEME - cSvcInitializationHelper - [fetchMaster=%s]", fetchMaster));
//            ZKMetadata.getClusterInfo().setMaster(cSvcManager.getLeaderNodeData2());
            WebAppDemoMetadata.getClusterInfo().setMaster(fetchMaster);
        } else {
            // TODO: This algorithm is poorly designed; hard to sync; needs addWatch functionality
            if (!cSvcManager.masterExists()) {
                cSvcManager.electForMaster();
            } else {
                WebAppDemoMetadata.getClusterInfo().setMaster(cSvcManager.getLeaderNodeData());
            }
        }
    }

//    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
    // TODO: Rename this function after porting MVP
    public void startApplication() throws InvalidServiceStateException {
//        try {
            // create all parent nodes /election, /all_nodes, /live_nodes
//            if (zkFlag) {
//                zkInitializationHelper();
//            } else {
                cSvcInitializationHelper();
//            }
//        } catch (Exception e) {
//            throw new RuntimeException("Startup failed!!", e);
//        }
    }

    private void syncDataFromMaster() {
        // BKTODO need try catch here for session not found
        if (getHostPostOfServer().equals(WebAppDemoMetadata.getClusterInfo().getMaster())) {
            return;
        }
        // TODO: Properly add data to Data Store
        ObjectMapper mapper = new ObjectMapper();
        HttpClient client = new HttpClient();
//        client.start();
        try (HTTPAutocloseWrapper autocloseWrapper = new HTTPAutocloseWrapper(client)) {
            // TODO: Clean up print statements
            System.out.println(
                    String.format("REMOVEME - DEBUG - syncDataFromMaster [getMaster=%s, get_url=http://%s%s]",
                    WebAppDemoMetadata.getClusterInfo().getMaster(),
                            WebAppDemoMetadata.getClusterInfo().getMaster(), ALL_RECORDS_ENDPOINT));
            ContentResponse response = client.GET(
                    String.format("http://%s%s",
                            WebAppDemoMetadata.getClusterInfo().getMaster(), ALL_RECORDS_ENDPOINT));
//            ZKMetadata.getClusterInfo().getMaster(), appPort, ALL_RECORDS_STRING_ENDPOINT));
            // TODO: Concern that above URL worked for ZK but was different for nonZK
//            List<Record> records = mapper.readValue(
//                    response.getContentAsString(),
//                    new TypeReference<List<Record>>(){});
//            dataStore.replaceRecords(records);
            dataStore.replaceRecords(SerializationUtils.deserialize(response.getContent()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
//        log.info(String.format("testJavalinEndpoints - GET RESPONSE [allRecords_response=%s]", response.getContentAsString()));
//        client.stop();
//        String requestUrl;
//        requestUrl = "http://".concat(ClusterInfo.getClusterInfo().getMaster().concat("/persons"));
//        List<Person> persons = restTemplate.getForObject(requestUrl, List.class);
//        DataStorage.getPersonListFromStorage().addAll(persons);
    }

    public void stopRunning() {
        this.isRunning = false;
    }

    public void close() throws InterruptedException {
        msgApp.close();
        if (!Objects.isNull(cSvcManager)) {
            cSvcManager.close();
        }
//        isRunning[0] = false;
    }

    public boolean isRunning() {
        return isRunning;
    }
}
