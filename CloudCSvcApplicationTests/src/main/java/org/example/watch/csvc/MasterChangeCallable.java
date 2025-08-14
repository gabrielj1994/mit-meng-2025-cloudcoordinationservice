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
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.example.manager.CSvcApplicationManager;
import org.example.manager.CoordinationSvcManager;
import org.example.util.WebAppDemoMetadata;

import java.util.function.BiConsumer;

/** @author "Gabriel Jimenez" 2024/10/15 */
@Setter
@Slf4j
public class MasterChangeCallable extends CallableBase {

    //  private ZKSvcManager zkService;
//    private final ApplicationManager owner;
    private CoordinationSvcManager cSvcManager;

    public MasterChangeCallable(CSvcApplicationManager owner, CoordinationSvcManager manager) {
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
                log.info("REMOVEME - MasterChangeCallable triggered [path={}]", commandOutput.getPath());
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
                    log.info("master deleted, recreating master!");
                    WebAppDemoMetadata.getClusterInfo().setMaster(null);
                    try {
                        cSvcManager.electForMaster();
                    } catch (ZkNodeExistsException e) {
                        log.info("master already created");
                    }
                } else {
                    String leaderNode = cSvcManager.getLeaderNodeData();
                    log.info("updating new master: {}", leaderNode);
                    WebAppDemoMetadata.getClusterInfo().setMaster(leaderNode);
                }
            } catch (Exception e) {
                log.error("REMOVEME - MasterChangeCallable - Exception!", e);
            }
//      log.info("REMOVEME - MasterChangeCallable getchildren [children={}]", output.getChildrenSet());
        };
        return new WatchConsumerWrapper<>(callable, true, WatchType.CHILDREN);
    }
}
