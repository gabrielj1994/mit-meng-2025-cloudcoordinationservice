package org.example.lock;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.client.CSvcClient;
import dsg.ccsvc.command.CommandOutput;
import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

public class CSvcSimpleBarrierUtil {
    /*
    This is a manager style class
    Need:
    - Declare intent to join barrier mechanism
    - First call to engage barrier
    - - This should disable new joins
    - - Engaged barrier will wait for the declared members to reach the barrier
    - - - this is accomplished by a child watch on /parent_node/barrier_status
    - - - one main watch tracking ephemeral numbers,
          creates node under /parent_node/barrier_status/ when enough members
    - - -
    STATES:
    - hashmap to store current barriers, mapped to their state (initialized ; engaged ; triggered)
    - - initialized accepts new members; no other does
    -
    METHODS:
    - initialize barrier
    - engage barrier
    - reset barrier
     */

    public static final String BARRIER_PARENT = "/barrier";

    public static Future<Boolean> engageBarrier(CSvcClient cSvcClient, String barrierName,
                                                int threshold) throws InvalidServiceStateException {
        // TODO: create /barriers/barrierName/node_sequential_ephemeral
//        System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Started Engage Barrier");
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        String barrierPath = BARRIER_PARENT.concat("/").concat(barrierName);
        CommandOutput output;
//        System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Create root");
        try {
            output = cSvcClient.create(BARRIER_PARENT, new byte [0],
                    false, false, false);
        } catch (InvalidServiceStateException e) {
            // NOTE: No-Op ; can already exist
//            e.printStackTrace(System.out);
        } catch (Exception e) {
            System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Hidden Exception");
            e.printStackTrace(System.out);
        }

//        System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Create barrier dir");
        try {
            output = cSvcClient.create(barrierPath, new byte [0],
                    false, false, false);
        } catch (InvalidServiceStateException e) {
            // NOTE: No-Op ; can already exist
//            e.printStackTrace(System.out);
        } catch (Exception e) {
            System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Hidden Exception");
            e.printStackTrace(System.out);
        }
//        System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Prepare Watch");
        BiConsumer<DataLayerMgrBase, WatchInput> callable = (manager, watchInput) -> {
            System.out.println("testSimpleBarrierRunnable - Watch triggered");
            if (future.isDone()) {
                // TODO: Remove watch
                System.out.println("testSimpleBarrierRunnable - Watch removed");
                try {
                    cSvcClient.unsubscribeChildrenChanges(barrierPath);
//                    watchFlag = false;
                } catch (InvalidServiceStateException e) {
                    // No-Op: Watch removed already
//                    System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Watch Error");
//                    throw new RuntimeException(e);
                }
                return;
            }
            try {
                ArrayList<String> children = new ArrayList<>(cSvcClient.getChildren(barrierPath));
                if (children.size() >= threshold) {
                    future.complete(true);
                    cSvcClient.unsubscribeChildrenChanges(barrierPath);
                }
            } catch (InvalidServiceStateException e) {
                e.printStackTrace(System.out);
//                throw new RuntimeException(e);
            } catch (Exception e) {
                System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Hidden Exception");
                e.printStackTrace(System.out);
            }
        };
        WatchConsumerWrapper<WatchInput> watchConsumer = new WatchConsumerWrapper<>(
                callable, true, WatchedEvent.WatchType.CHILDREN);
        cSvcClient.subscribeChildChanges(barrierPath, watchConsumer);

//        System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Create Barrier Member");
        output = cSvcClient.create(barrierPath.concat("/").concat("node_"), new byte [0],
                                            true, true, true);
        if (output.getErrorCode() != 0) {
            cSvcClient.unsubscribeChildrenChanges(barrierPath);
            System.out.println("testSimpleBarrierRunnable - Failed to create barrier member");
            throw new InvalidServiceStateException("Failed to create barrier member");
        }
        return future;
    }
}
