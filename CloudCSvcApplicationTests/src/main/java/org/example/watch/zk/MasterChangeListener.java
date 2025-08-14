package org.example.watch.zk;

//import bkatwal.zookeeper.demo.api.ZkService;
//import bkatwal.zookeeper.demo.util.ClusterInfo;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.example.manager.ZKSvcManager;
import org.example.util.WebAppDemoMetadata;

import java.util.List;

/** @author "Bikas Katwal" 27/03/19 */
@Setter
@Slf4j
public class MasterChangeListener implements IZkChildListener {

  private ZKSvcManager zkService;

  /**
   * listens for creation/deletion of znode "master" under /election znode and updates the
   * clusterinfo
   *
   * @param parentPath
   * @param currentChildren
   */
  @Override
  public void handleChildChange(String parentPath, List<String> currentChildren) {
    if (currentChildren.isEmpty()) {
      log.info("master deleted, recreating master!");
      WebAppDemoMetadata.getClusterInfo().setMaster(null);
      try {

        zkService.electForMaster();
      } catch (ZkNodeExistsException e) {
        log.info("master already created");
      }
    } else {
      String leaderNode = zkService.getLeaderNodeData();
      log.info("updating new master: {}", leaderNode);
      WebAppDemoMetadata.getClusterInfo().setMaster(leaderNode);
    }
  }
}
