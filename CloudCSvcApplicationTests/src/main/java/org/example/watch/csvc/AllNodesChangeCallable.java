package org.example.watch.csvc;

//import bkatwal.zookeeper.demo.util.ClusterInfo;
import dsg.ccsvc.command.CommandOutput;
import dsg.ccsvc.command.LsCommand;
import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent.WatchType;
import lombok.extern.slf4j.Slf4j;
import org.example.manager.CSvcApplicationManager;
import org.example.util.WebAppDemoMetadata;

import java.util.function.BiConsumer;

/** @author "Gabriel Jimenez" 2024/10/15 */
@Slf4j
public class AllNodesChangeCallable extends CallableBase {

//    private final ApplicationManager owner;

    public AllNodesChangeCallable(CSvcApplicationManager owner) {
        super(owner);
    }

    // TODO: Consider other ways to implement permanent watches
    // TODO: Consider non-static for mocking
//    public WatchConsumerWrapper<WatchInput> generateCallableConsumer(CSvcManagerAmazonDB_CmdAgnostic manager) {
//    public WatchConsumerWrapper<WatchInput> generateCallableConsumer(CSvcManagerBase manager) {
    public WatchConsumerWrapper<WatchInput> generateCallableConsumer() {
        BiConsumer<DataLayerMgrBase, WatchInput> callable = (manager, commandOutput) -> {
            if (!owner.isRunning()) {
                waitOnOwnerInit();
            }

            try {
                log.info("REMOVEME - AllNodesChangeCallable triggered [path={}]", commandOutput.getPath());
//                CommandOutput output = LsCommand.generateLsCommand(
//                                commandOutput.getPath(), false, false, false,
//                                null, false, true,
//                                commandOutput.getTriggerTransactionId(), true)
//                        .executeManager(manager);
                CommandOutput output = LsCommand.generateLsCommand(
                                commandOutput.getPath(), false, false, false,
                                null, false, true, true)
                        .executeManager(manager);
                log.info("current all node size: {}", output.getChildrenSet().size());
                WebAppDemoMetadata.getClusterInfo().getAllNodes().clear();
                WebAppDemoMetadata.getClusterInfo().getAllNodes().addAll(output.getChildrenSet());
            } catch (Exception e) {
                log.error("REMOVEME - AllNodesChangeCallable - Exception!", e);
            }
//      CommandOutput output = LsCommand.generateLsCommand(commandOutput.getPath(), false, false, false,
//              null, false, true, commandOutput.getTriggerTransactionId())
//              .executeManager(manager);
//      log.info("REMOVEME - AllNodesChangeCallable getchildren [children={}]", output.getChildrenSet());
        };
//    WatchConsumerWrapper<CommandOutput> callableWrapper =
//            new WatchConsumerWrapper<>(callable, true, WatchType.CHILDREN);
        return new WatchConsumerWrapper<>(callable, true, WatchType.CHILDREN);
    }
}
