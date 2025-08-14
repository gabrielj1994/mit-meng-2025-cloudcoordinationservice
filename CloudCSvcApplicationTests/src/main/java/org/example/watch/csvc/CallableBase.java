package org.example.watch.csvc;

import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import lombok.extern.slf4j.Slf4j;
import org.example.manager.CSvcApplicationManager;

/** @author "Gabriel Jimenez" 2024/10/15 */
@Slf4j
public abstract class CallableBase {

    final CSvcApplicationManager owner;

    public CallableBase(CSvcApplicationManager owner) {
        this.owner = owner;
    }

    void waitOnOwnerInit() {
        synchronized (owner) {
            while (!owner.isRunning()) {
                try {
                    owner.wait();
                } catch (InterruptedException e) {
                    log.error("REMOVEME - AllNodesChangeCallable - Exception!", e);
                }
            }
        }
    }

    // TODO: Consider other ways to implement permanent watches
    // TODO: Consider non-static for mocking
//    public abstract WatchConsumerWrapper<WatchInput> generateCallableConsumer(
//                                                            CSvcManagerAmazonDB_CmdAgnostic manager);
//    public abstract WatchConsumerWrapper<WatchInput> generateCallableConsumer(CSvcManagerBase manager);
    public abstract WatchConsumerWrapper<WatchInput> generateCallableConsumer();
}
