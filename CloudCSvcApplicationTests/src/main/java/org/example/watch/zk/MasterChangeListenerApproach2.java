package org.example.watch.zk;

//import bkatwal.zookeeper.demo.api.ZkService;
//import bkatwal.zookeeper.demo.util.ClusterInfo;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.example.manager.ZKSvcManager;
import org.example.util.WebAppDemoMetadata;

import java.util.Collections;
import java.util.List;

//import static bkatwal.zookeeper.demo.util.ZkDemoUtil.ELECTION_NODE_2;
import static org.example.util.ZKConfigs.ELECTION_NODE_2;

/** @author "Bikas Katwal" 27/03/19 */
@Slf4j
@Setter
public class MasterChangeListenerApproach2 implements IZkChildListener {

  private ZKSvcManager zkService;

  /**
   * listens for deletion of sequential znode under /election znode and updates the
   * clusterinfo
   *
   * @param parentPath
   * @param currentChildren
   */
  @Override
  public void handleChildChange(String parentPath, List<String> currentChildren) {
    if (currentChildren.isEmpty()) {
      throw new RuntimeException("No node exists to select master!!");
    } else {
      //get least sequenced znode
      Collections.sort(currentChildren);
      String masterZNode = currentChildren.get(0);

      // once znode is fetched, fetch the znode data to get the hostname of new leader
      String masterNode = zkService.getZNodeData(ELECTION_NODE_2.concat("/").concat(masterZNode));
      log.info("new master is: {}", masterNode);

      //update the cluster info with new leader
      WebAppDemoMetadata.getClusterInfo().setMaster(masterNode);
    }
  }
}
