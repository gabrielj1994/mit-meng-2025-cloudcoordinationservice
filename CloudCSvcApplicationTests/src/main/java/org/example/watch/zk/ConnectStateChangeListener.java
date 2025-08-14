package org.example.watch.zk;

//import bkatwal.zookeeper.demo.api.ZkService;
//import bkatwal.zookeeper.demo.model.Person;
//import bkatwal.zookeeper.demo.util.ClusterInfo;
//import bkatwal.zookeeper.demo.util.DataStorage;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.example.manager.ZKSvcManager;
import org.example.util.WebAppDemoMetadata;
//import org.springframework.web.client.RestTemplate;

//import static bkatwal.zookeeper.demo.util.ZkDemoUtil.getHostPostOfServer;
//import static bkatwal.zookeeper.demo.util.ZkDemoUtil.isEmpty;
import static org.example.util.ZKConfigs.*;

/** @author "Bikas Katwal" 02/04/19 */
@Slf4j
@Setter
public class ConnectStateChangeListener implements IZkStateListener {

  private ZKSvcManager zkService;
//  private RestTemplate restTemplate = new RestTemplate();

  @Override
  public void handleStateChanged(KeeperState state) throws Exception {
    log.info(state.name()); // 1. disconnected, 2. expired, 3. SyncConnected
  }

  @Override
  public void handleNewSession() throws Exception {
    log.info("connected to zookeeper");

    // sync data from master
    syncDataFromMaster();

    // add new znode to /live_nodes to make it live
    zkService.addToLiveNodes(getHostPostOfServer(), "cluster node");
    WebAppDemoMetadata.getClusterInfo().getLiveNodes().clear();
    WebAppDemoMetadata.getClusterInfo().getLiveNodes().addAll(zkService.getLiveNodes());

    // re try creating znode under /election
    // this is needed, if there is only one server in cluster
    String leaderElectionAlgo = System.getProperty("leader.algo");
    if (isEmpty(leaderElectionAlgo) || "2".equals(leaderElectionAlgo)) {
      zkService.createNodeInElectionZnode(getHostPostOfServer());
      WebAppDemoMetadata.getClusterInfo().setMaster(zkService.getLeaderNodeData2());
    } else {
      if (!zkService.masterExists()) {
        zkService.electForMaster();
      } else {
        WebAppDemoMetadata.getClusterInfo().setMaster(zkService.getLeaderNodeData());
      }
    }
  }

  @Override
  public void handleSessionEstablishmentError(Throwable error) throws Exception {
    log.info("could not establish session");
  }

  // TODO: Properly add data to Data Store
  private void syncDataFromMaster() {
    // BKTODO need try catch here for session not found
    if (getHostPostOfServer().equals(WebAppDemoMetadata.getClusterInfo().getMaster())) {
      return;
    }
    String requestUrl;
    requestUrl = "http://".concat(WebAppDemoMetadata.getClusterInfo().getMaster().concat("/persons"));
//    List<Person> persons = restTemplate.getForObject(requestUrl, List.class);
//    DataStorage.getPersonListFromStorage().clear();
//    DataStorage.getPersonListFromStorage().addAll(persons);
  }
}
