package org.example.lock;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.client.CSvcClient;
import dsg.ccsvc.command.CommandOutput;
import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

@Slf4j
public class CSvcSimpleLock implements AutoCloseable {

    public static final String LOCK_NODE_PARENT = "/lockNodes";
    public static final String LOCK_NODE = "/lock_";
    private CSvcClient cSvcClient;

    private String lockPath = "";
    private boolean watchFlag;
    private boolean inUseFlag;
    private CompletableFuture<Boolean> tbdLockBoolean;

    private CSvcSimpleLock(CSvcClient cSvcClient) throws InvalidServiceStateException {
        this.cSvcClient = cSvcClient;
        this.watchFlag = false;
        this.inUseFlag = false;
    }

    // TODO: Move this to CSvcClient?
    private CompletableFuture<Boolean> queueLockRequest() throws InvalidServiceStateException {
        // TODO: Create ephemeral sequential under lock_nodes.
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CommandOutput output = cSvcClient.create(LOCK_NODE_PARENT.concat(LOCK_NODE), new byte [0],
                                                    true, true, true);
        if (output.getErrorCode() != 0) {
            // TODO: Consider error out / exception
            future.complete(false);
            return future;
        }
        lockPath = output.getPath();
        String lockNodeName = Paths.get(output.getPath()).getFileName().toString();

        // NOTE: add watch
        if (!future.isDone()) {
//            System.out.println("REMOVE ME - DEBUG - ADDING WATCH");
            BiConsumer<DataLayerMgrBase, WatchInput> callable = (manager, watchInput) -> {
//                System.out.println("REMOVE ME - DEBUG - WATCH EXECUTING");
                if (future.isDone()) {
                    // NOTE: Remove watch
                    try {
                        cSvcClient.unsubscribeChildrenChanges(LOCK_NODE_PARENT);
                        watchFlag = false;
                    } catch (InvalidServiceStateException e) {
                        throw new RuntimeException(e);
                    }
//                    System.out.println("REMOVE ME - DEBUG - WATCH DONE 0");
                    return;
                }

                ArrayList<String> sortedChildrenWatch = null;
                try {
                    sortedChildrenWatch = new ArrayList<>(cSvcClient.getChildren(LOCK_NODE_PARENT));
                } catch (InvalidServiceStateException e) {
                    e.printStackTrace(System.out);
//                    System.out.println("REMOVE ME - DEBUG - WATCH EXCEPTION 0");
                    throw new RuntimeException(e);
                }
                if (sortedChildrenWatch.isEmpty()) {
                    log.error("CRITICAL ERROR - SimpleLock Watch - Missing Lock Node!");
                    future.complete(false);
                    // TODO: Remove Watch
                    try {
                        cSvcClient.unsubscribeChildrenChanges(LOCK_NODE_PARENT);
                        watchFlag = false;
                    } catch (InvalidServiceStateException e) {
                        e.printStackTrace(System.out);
//                        System.out.println("REMOVE ME - DEBUG - WATCH EXCEPTION 1");
                        throw new RuntimeException(e);
                    }
                    throw new RuntimeException("CRITICAL ERROR - SimpleLock Watch - Missing Lock Node!");
                } else {
                    Collections.sort(sortedChildrenWatch);
                    if (sortedChildrenWatch.get(0).equals(lockNodeName)) {
                        future.complete(true);
                        // TODO: Remove Watch
                        try {
                            cSvcClient.unsubscribeChildrenChanges(LOCK_NODE_PARENT);
                            watchFlag = false;
                        } catch (InvalidServiceStateException e) {
//                            System.out.println("REMOVE ME - DEBUG - WATCH EXCEPTION 2");
                            throw new RuntimeException(e);
                        }
                    }
                }
//                System.out.println("REMOVE ME - DEBUG - WATCH DONE");
            };

            // NOTE: Get Children, Sort
            WatchConsumerWrapper<WatchInput> watchConsumer = new WatchConsumerWrapper<>(
                    callable, true, WatchedEvent.WatchType.CHILDREN);
            ArrayList<String> sortedChildren = new ArrayList<>(cSvcClient.subscribeChildChanges(LOCK_NODE_PARENT,
                                                                watchConsumer).orElseGet(Collections::emptySet));
            if (sortedChildren.isEmpty()) {
                log.error("CRITICAL ERROR - SimpleLock - Missing Lock Node!");
                future.complete(false);
                throw new InvalidServiceStateException("CRITICAL ERROR - SimpleLock - Missing Lock Node!");
            } else {
                Collections.sort(sortedChildren);
                if (sortedChildren.get(0).equals(lockNodeName)) {
                    future.complete(true);
                }
            }
            watchFlag = true;
        }

        inUseFlag = true;
        return future;
    }

    private void releaseLock() throws InvalidServiceStateException {
        // TODO: Think about where state is preserved.
        //   opt 1) path is stored in the tbdlock instance (after getting a lock)
        //   opt 2) return string instead of the future, the future gets passed in as param
        if (!inUseFlag) {
            return;
        }
//        System.out.printf("REMOVE ME - DEBUG - releaseLock() [thread_name=%s]\n", Thread.currentThread().getName());
        if (!lockPath.isEmpty()) {
            cSvcClient.delete(lockPath);
        }
        if (watchFlag) {
            cSvcClient.unsubscribeChildrenChanges(LOCK_NODE_PARENT);
        }
        inUseFlag = false;
    }

    @Override
    public void close() throws InvalidServiceStateException {
        // NOTE: Release lock
        releaseLock();
//        System.out.println("REMOVE ME - DEBUG - LOCK CLOSED");
//        System.out.printf("REMOVE ME - DEBUG - lock close() [thread_name=%s]\n", Thread.currentThread().getName());
    }

    public static CSvcSimpleLock submitSimpleLockRequest(CSvcClient cSvcClient) throws InvalidServiceStateException {
        CSvcSimpleLock lock = new CSvcSimpleLock(cSvcClient);
        lock.tbdLockBoolean = lock.queueLockRequest();
        return lock;
    }

    public boolean lock() {
        try {
            return tbdLockBoolean.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
