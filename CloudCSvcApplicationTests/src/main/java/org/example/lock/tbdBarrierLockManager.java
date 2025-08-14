//package org.example.lock;
//
//import dsg.tbd.modular.InvalidServiceStateException;
//import dsg.tbd.modular.client.CSvcClient;
//
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.Future;
//
//public class tbdBarrierLockManager {
//    // TODO: ABANDONED, NOT POSSIBLE TO DO A MULTI STAGE BARRIER WITH MULTIPLE CSVCCLIENT OPERATING CONCURRENTLY
//    /*
//    This is a manager style class
//    Need:
//    - Declare intent to join barrier mechanism
//    - First call to engage barrier
//    - - This should disable new joins
//    - - Engaged barrier will wait for the declared members to reach the barrier
//    - - - this is accomplished by a child watch on /parent_node/barrier_status
//    - - - one main watch tracking ephemeral numbers,
//          creates node under /parent_node/barrier_status/ when enough members
//    - - -
//    STATES:
//    - hashmap to store current barriers, mapped to their state (initialized ; engaged ; triggered)
//    - - initialized accepts new members; no other does
//    -
//    METHODS:
//    - initialize barrier
//    - engage barrier
//    - reset barrier
//     */
//
//    public static final String BARRIER_STATUS_PARENT = "/barrier_status";
//    public static final String BARRIER_READY_NODE = BARRIER_STATUS_PARENT.concat("/barrier_status");
//    public static final String BARRIER_STAGING_PARENT = "/barrier_staging";
//    public static final String EXPECTED_MEMBERS_PARENT = "/expected_members";
//
//    private CSvcClient cSvcClient;
//    private ConcurrentHashMap<String, CSvcBarrierState> barrierToStateIndex;
//
//    public tbdBarrierLockManager(CSvcClient cSvcClient) {
//        this.cSvcClient = cSvcClient;
//        this.barrierToStateIndex = new ConcurrentHashMap<>();
//    }
//
//    public void initializeBarrier(String barrierName) throws InvalidServiceStateException {
//        cleanUpBarrier(barrierName);
//        initializeBarrierNodes(barrierName);
//        barrierToStateIndex.put(barrierName, CSvcBarrierState.INITIALIZED);
//    }
//
//    private void initializeBarrierNodes(String barrierName) throws InvalidServiceStateException {
//        // TODO: Create /barrierName/expected_members ; /barrierName/barrier_status ; /barrierName/barrier_staging
//        cSvcClient.create("/".concat(barrierName).concat(BARRIER_STAGING_PARENT),
//                new byte[0], false, false, true);
//        cSvcClient.create("/".concat(barrierName).concat(EXPECTED_MEMBERS_PARENT),
//                new byte[0], false, false, true);
//        cSvcClient.create("/".concat(barrierName).concat(BARRIER_STATUS_PARENT),
//                new byte[0], false, false, true);
//    }
//
//    private void cleanUpBarrier(String barrierName) throws InvalidServiceStateException {
//        // TODO: barrier must be in initialized or triggered state
//        cSvcClient.recursiveDelete("/".concat(barrierName).concat(BARRIER_STAGING_PARENT));
//        cSvcClient.recursiveDelete("/".concat(barrierName).concat(EXPECTED_MEMBERS_PARENT));
//        cSvcClient.recursiveDelete("/".concat(barrierName).concat(BARRIER_STATUS_PARENT));
//    }
//
//    public void engageBarrier(String barrierName) {
//        // TODO: check child count of /barrierName/expected_members & /barrierName/barrier_staging ;
//        //  set the watch on /barrierName/barrier_staging ;
//        //  create /barrierName/barrier_status/ready in watch
//    }
//
//    public void resetBarrier(String barrierName) {
//        // TODO: Only in initialized or triggered state delete /path/barrier_status/ready
//        //  ; delete /path/barrier_staging members ; set inmemory state to initialized
//        //
//    }
//
//    public boolean addExpectedMember(String barrierName, String memberName) {
//        // TODO: If barrier state is initialized state, add memberName
//        if ()
//        return true;
//    }
//
//    public boolean removeExpectedMember(String barrierName, String memberName) {
//        // TODO: If barrier state is initialized state, remove memberName
//        return true;
//    }
//
//    public Future<Boolean> addToEngagedBarrier(String barrierName, String memberName) {
//        // TODO: Require barrier to be engaged, return null or throw exception
//        CompletableFuture<Boolean> future = new CompletableFuture<>();
//        // TODO: add child watch to /path/barrier_status
//        //   create /path/barrier_staging/memberName
//        return future;
//    }
//
//    public CSvcBarrierState getBarrierState(String barrierName) {
//
//    }
//
//    public enum CSvcBarrierState {
//        INITIALIZED,
//        ENGAGED,
//        TRIGGERED;
//    }
//
//}
