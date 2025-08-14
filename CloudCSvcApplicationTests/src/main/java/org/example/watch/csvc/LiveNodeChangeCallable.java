package org.example.watch.csvc;

//import bkatwal.zookeeper.demo.util.ClusterInfo;
import dsg.ccsvc.command.CommandOutput;
import dsg.ccsvc.command.LsCommand;
import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchedEvent.WatchType;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.example.manager.CSvcApplicationManager;
import org.example.util.WebAppDemoMetadata;

import java.util.function.BiConsumer;

/** @author "Gabriel Jimenez" 2024/10/15 */
@Slf4j
public class LiveNodeChangeCallable extends CallableBase {

//    private final ApplicationManager owner;

    public LiveNodeChangeCallable(CSvcApplicationManager owner) {
        super(owner);
    }

//    public WatchConsumerWrapper<WatchInput> generateCallableConsumer(CSvcManagerAmazonDB_CmdAgnostic manager) {
//public WatchConsumerWrapper<WatchInput> generateCallableConsumer(CSvcManagerBase manager) {
public WatchConsumerWrapper<WatchInput> generateCallableConsumer() {
        BiConsumer<DataLayerMgrBase, WatchInput> callable = (manager, commandOutput) -> {
            if (!owner.isRunning()) {
                waitOnOwnerInit();
            }

            try {
                log.info("REMOVEME - LiveNodeChangeCallable triggered [path={}]", commandOutput.getPath());
//                CommandOutput output = LsCommand.generateLsCommand(
//                                commandOutput.getPath(), false, false, false,
//                                null, false, true,
//                                commandOutput.getTriggerTransactionId(), true)
//                        .executeManager(manager);
                CommandOutput output = LsCommand.generateLsCommand(
                                commandOutput.getPath(), false, false, false,
                                null, false, true, true)
                        .executeManager(manager);
                log.info("current live size: {}", output.getChildrenSet().size());
                WebAppDemoMetadata.getClusterInfo().getLiveNodes().clear();
                WebAppDemoMetadata.getClusterInfo().getLiveNodes().addAll(output.getChildrenSet());
            } catch (Exception e) {
                log.error("REMOVEME - LiveNodeChangeCallable - Exception!", e);
            }

//      log.info("REMOVEME - LiveNodeChangeCallable getchildren [children={}]", output.getChildrenSet());
        };
        return new WatchConsumerWrapper<>(callable, true, WatchType.CHILDREN);
    }
}
