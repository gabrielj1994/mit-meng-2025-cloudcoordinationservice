package org.example.watch.csvc;

//import bkatwal.zookeeper.demo.api.ZkService;
//import bkatwal.zookeeper.demo.util.ClusterInfo;

import dsg.ccsvc.command.CommandOutput;
import dsg.ccsvc.command.LsCommand;
import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent.WatchType;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.example.manager.CSvcApplicationManager;
import org.example.manager.CoordinationSvcManager;
import org.example.util.WebAppDemoMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.function.BiConsumer;

import static org.example.util.ZKConfigs.ELECTION_NODE_2;

/**
 * @author "Gabriel Jimenez" 2024/10/15
 */
@Slf4j
@Setter
public class MasterChangeCallableApproach2 extends CallableBase {

//    private ApplicationManager owner;
    private CoordinationSvcManager cSvcManager;

    public MasterChangeCallableApproach2 (CSvcApplicationManager owner, CoordinationSvcManager manager) {
        super(owner);
        this.cSvcManager = manager;
    }

//    public WatchConsumerWrapper<WatchInput> generateCallableConsumer(CSvcManagerAmazonDB_CmdAgnostic manager) {
    public WatchConsumerWrapper<WatchInput> generateCallableConsumer() {
        BiConsumer<DataLayerMgrBase, WatchInput> callable = (manager, commandOutput) -> {
            if (!owner.isRunning()) {
                waitOnOwnerInit();
            }
            try {
                log.info("REMOVEME - MasterChangeCallableApproach2 triggered [path={}]", commandOutput.getPath());
//                CommandOutput output = LsCommand.generateLsCommand(commandOutput.getPath(), false,
//                                false, false, null, false,
//                                true, commandOutput.getTriggerTransactionId(), true)
//                        .executeManager(manager);
                CommandOutput output = LsCommand.generateLsCommand(commandOutput.getPath(), false,
                                false, false, null, false,
                                true, true)
                        .executeManager(manager);
                log.info("current live size: {}", output.getChildrenSet().size());
                if (output.getChildrenSet().isEmpty()) {
                    log.error("REMOVEME - MasterChangeCallableApproach2 - childrenSetEmpty!");
                    throw new RuntimeException("No node exists to select master!!");
                } else {
                    // TODO: Consider a better approach than baseline sort (iterate manually once)
                    ArrayList<String> sortedChildren = new ArrayList<>(output.getChildrenSet());
                    Collections.sort(sortedChildren);
                    String masterZNode = sortedChildren.get(0);


                    // once znode is fetched, fetch the znode data to get the hostname of new leader
//                String masterNode = cSvcClient.getZNodeData(ELECTION_NODE_2.concat("/").concat(masterZNode));
                    log.info("Fetching master path: {}", ELECTION_NODE_2.concat("/").concat(masterZNode));
                    String masterNode = cSvcManager.getNodeData(ELECTION_NODE_2.concat("/").concat(masterZNode));
                    log.info("New master is: {}", masterNode);
                    //update the cluster info with new leader
                    WebAppDemoMetadata.getClusterInfo().setMaster(masterNode);
                }
            } catch (Exception e) {
                log.error("REMOVEME - MasterChangeCallableApproach2 - Exception!", e);
            }
//            log.info("REMOVEME - MasterChangeCallableApproach2 getchildren [children={}]", output.getChildrenSet());
        };
        return new WatchConsumerWrapper<>(callable, true, WatchType.CHILDREN);
    }
}
