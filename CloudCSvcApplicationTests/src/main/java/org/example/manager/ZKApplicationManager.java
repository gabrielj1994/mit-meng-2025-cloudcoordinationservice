package org.example.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import dsg.ccsvc.InvalidServiceStateException;
import io.javalin.Javalin;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.commons.lang3.SerializationUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.example.controller.SimpleController;
import org.example.storage.DataStore;
import org.example.util.HTTPAutocloseWrapper;
import org.example.util.WebAppDemoMetadata;
import org.example.watch.zk.*;

import static org.example.util.HTTPUtil.*;
import static org.example.util.HTTPUtil.UPDATE_RECORD_STRING_ENDPOINT;
import static org.example.util.ZKConfigs.*;
import static org.example.util.ZKConfigs.ALL_NODES;

public class ZKApplicationManager implements WebAppManager {

    private ZKSvcManager zkSvcManager;

    private DataStore dataStore;

    private Javalin msgApp;

    private SimpleController controller;

    private IZkChildListener allNodesChangeListener;

    private IZkChildListener liveNodeChangeListener;

    private IZkChildListener masterChangeListener;

    private IZkStateListener connectStateChangeListener;

    private boolean isRunning = false;

    private int appPort = 7100;

    // TODO: Identify if constructor needs more inputs
    public ZKApplicationManager(DataStore dataStore) throws InvalidServiceStateException {
//        this.zkFlag = zkFlag;
        // TODO: Set system properties at startup
        int leaderMode = Integer.parseInt(System.getProperty("leader.algo"));
        appPort = Integer.parseInt(System.getProperty("msg.port"));
//        if (zkFlag) {
        String zkHostPort = System.getProperty("zk.url");
        zkSvcManager = new ZKSvcManager(zkHostPort);
        allNodesChangeListener = new AllNodesChangeListener();
        liveNodeChangeListener = new LiveNodeChangeListener();
        // TODO: add layer of abstraction through initialization helper functions
        // TODO: add option to choose master listener implementation
        if (leaderMode == 1) {
            MasterChangeListener masterListener = new MasterChangeListener();
            masterListener.setZkService(zkSvcManager);
            masterChangeListener = masterListener;
        } else {
            MasterChangeListenerApproach2 masterListener = new MasterChangeListenerApproach2();
            masterListener.setZkService(zkSvcManager);
            masterChangeListener = masterListener;
        }
        connectStateChangeListener = new ConnectStateChangeListener();
//        }
//        else {
//            cSvcManager = new CoordinationSvcManager();
//            allNodesChangeCallable = new AllNodesChangeCallable(this)
//                    .generateCallableConsumer(cSvcManager.getManager());
//            liveNodeChangeCallable = new LiveNodeChangeCallable(this)
//                    .generateCallableConsumer(cSvcManager.getManager());
//            if (leaderMode == 1) {
//                masterChangeCallable = new MasterChangeCallable(this, cSvcManager)
//                        .generateCallableConsumer(cSvcManager.getManager());
//            } else {
//                masterChangeCallable = new MasterChangeCallableApproach2(this, cSvcManager)
//                        .generateCallableConsumer(cSvcManager.getManager());
//            }
//        }

        this.dataStore = dataStore;
        this.controller = new SimpleController(dataStore);
        initializeEndpoints();
//        isRunning[0] = true;
//        synchronized (this) {
            isRunning = true;
//            this.notifyAll();
//        }
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

    private void zkInitializationHelper() {
        zkSvcManager.createAllParentNodes();

        // add this server to cluster by creating znode under /all_nodes, with name as "host:port"
        zkSvcManager.addToAllNodes(getHostPostOfServer(), "cluster node");
        WebAppDemoMetadata.getClusterInfo().getAllNodes().clear();
        WebAppDemoMetadata.getClusterInfo().getAllNodes().addAll(zkSvcManager.getAllNodes());

        // check which leader election algorithm(1 or 2) need is used
        String leaderElectionAlgo = System.getProperty("leader.algo");

        // if approach 2 - create ephemeral sequential znode in /election
        // then get children of  /election and fetch least sequenced znode, among children znodes
        if (isEmpty(leaderElectionAlgo) || "2".equals(leaderElectionAlgo)) {
            zkSvcManager.createNodeInElectionZnode(getHostPostOfServer());
            WebAppDemoMetadata.getClusterInfo().setMaster(zkSvcManager.getLeaderNodeData2());
        } else {
            if (!zkSvcManager.masterExists()) {
                zkSvcManager.electForMaster();
            } else {
                WebAppDemoMetadata.getClusterInfo().setMaster(zkSvcManager.getLeaderNodeData());
            }
        }

        // sync person data from master
        syncDataFromMaster();

        // add child znode under /live_node, to tell other servers that this server is ready to serve
        // read request
        zkSvcManager.addToLiveNodes(getHostPostOfServer(), "cluster node");
        WebAppDemoMetadata.getClusterInfo().getLiveNodes().clear();
        WebAppDemoMetadata.getClusterInfo().getLiveNodes().addAll(zkSvcManager.getLiveNodes());

        // register watchers for leader change, live nodes change, all nodes change and zk session
        // state change
        if (isEmpty(leaderElectionAlgo) || "2".equals(leaderElectionAlgo)) {
            zkSvcManager.registerChildrenChangeWatcher(ELECTION_NODE_2, masterChangeListener);
        } else {
            zkSvcManager.registerChildrenChangeWatcher(ELECTION_NODE, masterChangeListener);
        }
        zkSvcManager.registerChildrenChangeWatcher(LIVE_NODES, liveNodeChangeListener);
        zkSvcManager.registerChildrenChangeWatcher(ALL_NODES, allNodesChangeListener);
        zkSvcManager.registerZkSessionStateListener(connectStateChangeListener);
    }

    public void startApplication() {
//        try {
        // create all parent nodes /election, /all_nodes, /live_nodes
//        if (zkFlag) {
            zkInitializationHelper();
//        } else {
//            cSvcInitializationHelper();
//        }
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

    public void close() {
        msgApp.close();
//        isRunning[0] = false;
    }

    public boolean isRunning() {
        return isRunning;
    }

}
