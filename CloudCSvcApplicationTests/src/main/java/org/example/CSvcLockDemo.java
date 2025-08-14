package org.example;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.client.CSvcClient;
import dsg.ccsvc.util.DebugConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.example.lock.CSvcSimpleBarrierUtil;
import org.example.lock.CSvcSimpleLock;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.example.lock.CSvcSimpleLock.LOCK_NODE_PARENT;

@Slf4j
public class CSvcLockDemo {

    private static Options options = new Options();
//    private static boolean temp_flag = false;

    static {
        options.addOption(new Option("df", false, "debug flag"));
        options.addOption(new Option("lf", false, "log flag"));
        options.addOption(new Option("mf", false, "metric flag"));
//        options.addOption(new Option("temp", false, "temp flag"));
    }

    public static void main(String[] args) {
        int flag = Integer.parseInt(args[0]);
        CSvcClient.ManagerType type = CSvcClient.ManagerType.fromInteger(Integer.parseInt(args[1]));
        DefaultParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(String.format("CSvcFSStoreDemo - Failed command line options parsing"));
            throw new RuntimeException(e);
        }

        DebugConstants.METRICS_FLAG = cl.hasOption("mf") || DebugConstants.METRICS_FLAG;
        DebugConstants.DEBUG_FLAG = cl.hasOption("df") || DebugConstants.DEBUG_FLAG;
//        temp_flag = cl.hasOption("temp") || temp_flag;

        if (flag == 0) {
            // NOTE: Simple Lock test. 2 single thread executors. Adding elements to shared arraylist.
            //   Each thread adds, then sleeps 5 ms
            //   Validate checks that no intermingled values
            DebugConstants.LOGREADER_FLAG = true;
            testSimpleLock(type);
        } else if (flag == 1) {
            DebugConstants.LOGREADER_FLAG = true;
            testSimpleBarrier(type);
        }
    }

    public static void testSimpleLock(CSvcClient.ManagerType type) {
        System.out.println("Testing testSimpleLock:");
        ExecutorService executorOne = Executors.newSingleThreadExecutor();
        ExecutorService executorTwo = Executors.newSingleThreadExecutor();
        try (CSvcClient client = generateCSvcClient(type)) {
            client.startClient();
            try {
                client.create(LOCK_NODE_PARENT, new byte[0], false, false, true);
            } catch (Exception e) {
                // NOTE: No-Op
            }
            Queue<Integer> sharedQueue = new ConcurrentLinkedQueue<>();
            int insertCount = 50;
            Future<Boolean> execOne = testSimpleLockExecutorHelper(executorOne, client, sharedQueue, 1, insertCount);
            Future<Boolean> execTwo = testSimpleLockExecutorHelper(executorTwo, client, sharedQueue, 2, insertCount);
            assert (execOne.get());
            assert (execTwo.get());
            assert (sharedQueue.size() == insertCount*2);
            assert (testSimpleLockValidationHelper(sharedQueue));
        } catch (InvalidServiceStateException | InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        executorOne.shutdownNow();
        executorTwo.shutdownNow();
//        System.out.println("Active Threads:");
//        for (Thread t : Thread.getAllStackTraces().keySet()) {
//            System.out.println("Thread Name: " + t.getName() + ", State: " + t.getState());
//        }
    }

    private static Future<Boolean> testSimpleLockExecutorHelper(ExecutorService executor, CSvcClient cSvcClient, Queue<Integer> sharedQueue,
                                                                Integer id, int insertCount) {
//        ExecutorService executor = Executors.newSingleThreadExecutor();
        return executor.submit(() -> {
            // NOTE: Submit Lock Claim
            try (CSvcSimpleLock lock = CSvcSimpleLock.submitSimpleLockRequest(cSvcClient)) {
                // NOTE: Wait on lock
                if (lock.lock()) {
                    int counter = 0;
                    // TODO: Consider never returning false, and simply throwing if not true
                    while (counter < insertCount) {
                        sharedQueue.add(id);
                        Thread.sleep(5);
                        counter++;
                    }
                    return true;
                } else {
                    // NOTE: Failed to retrieve lock
                    return false;
                }
            } catch (InterruptedException e) {
//                log.error("testSimpleLockExecutorHelper - Interrupted", e);
                System.out.println("testSimpleLockExecutorHelper - Interrupted");
                e.printStackTrace(System.out);
                throw new RuntimeException(e);
            } catch (InvalidServiceStateException e) {
//                log.error("testSimpleLockExecutorHelper - Unexpected Error", e);
                System.out.println("testSimpleLockExecutorHelper - Unexpected Error");
                e.printStackTrace(System.out);
                throw new RuntimeException(e);
            }
        });
    }

    private static boolean testSimpleLockValidationHelper(Queue<Integer> sharedQueue) {
        if (sharedQueue.isEmpty()) {
            // NOTE: Should not be empty
            return false;
        }
        Integer previousValue = sharedQueue.peek();
        boolean valueSwappedFlag = false;
        for (Integer element : sharedQueue) {
            if (!valueSwappedFlag) {
                valueSwappedFlag = previousValue.intValue() != element.intValue();
            } else if (previousValue.intValue() != element.intValue()) {
                return false;
            }
            previousValue = element;
        }
        return true;
    }

    public static void testSimpleBarrier(CSvcClient.ManagerType type) {
        System.out.println("Testing testSimpleBarrier:");
        // NOTE: Pseudo map-reduce configuration;
        // Simulates counting the number of each alphabet character in a random string.
        //  3 workers, separate characters into buckets, wait for barrier, then count
        try (CSvcClient cSvcClient = generateCSvcClient(type)) {
            cSvcClient.startClient();
            ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Character>> intermediaryBuckets =
                    new ConcurrentHashMap<>();
            intermediaryBuckets.put(0, new ConcurrentLinkedQueue<>());
            intermediaryBuckets.put(1, new ConcurrentLinkedQueue<>());
            intermediaryBuckets.put(2, new ConcurrentLinkedQueue<>());
            ConcurrentHashMap<Integer, Character> bucketMapping = new ConcurrentHashMap<>();
            bucketMapping.put(0, 'h');
            bucketMapping.put(1, 'p');
            bucketMapping.put(2, 'z');
            ConcurrentHashMap<Character, Integer> characterToCountSharedIndex = new ConcurrentHashMap<>();
            HashMap<Character, Integer> expectedCounts = new HashMap<>();
            String randomString;
            testSimpleBarrierRunnable[] runnables = new testSimpleBarrierRunnable[3];
            int runnableIndex = 0;
            int runnableMax = 3;
            int index;
            int maxIndex;
            char currChar;
            while (runnableIndex < runnableMax) {
                randomString = RandomStringUtils.randomAlphabetic(20000).toLowerCase();
                index = 0;
                maxIndex = randomString.length();
                while (index < maxIndex) {
                    currChar = randomString.charAt(index);
                    expectedCounts.merge(currChar, 1, Integer::sum);
                    index++;
                }
                runnables[runnableIndex] = new testSimpleBarrierRunnable(cSvcClient,
                        characterToCountSharedIndex, bucketMapping,
                        intermediaryBuckets, randomString, runnableIndex);
                runnableIndex++;
            }

            runnableIndex = 0;
            Future<?>[] futures = new Future[3];
            ExecutorService executor;
            ArrayList<ExecutorService> executors = new ArrayList<>();
            while (runnableIndex < runnableMax) {
                executor = Executors.newSingleThreadExecutor();
                executors.add(executor);
                futures[runnableIndex] = executor.submit(runnables[runnableIndex]);
                runnableIndex++;
            }

            runnableIndex = 0;
            while (runnableIndex < runnableMax) {
                try {
                    futures[runnableIndex].get();
                } catch (ExecutionException e) {
                    System.out.println("Failed Test - Execution Exception");
                    e.printStackTrace(System.out);
                    throw new RuntimeException(e);
                }
                runnableIndex++;
            }

            assert (testSimpleBarrierValidationHelper(expectedCounts, characterToCountSharedIndex));
            for (ExecutorService executorService : executors) {
                executorService.shutdownNow();
            }
        } catch (InvalidServiceStateException | InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private static boolean testSimpleBarrierValidationHelper(Map<Character, Integer> expectedCounts,
                                                             Map<Character, Integer> calculatedCounts) {
        boolean validationFlag = true;
        for (Map.Entry<Character, Integer> expectedEntry : expectedCounts.entrySet()) {
            if (!expectedEntry.getValue().equals(calculatedCounts.get(expectedEntry.getKey()))) {
                System.out.println(String.format("Failed Validation - testSimpleBarrier" +
                        " [char=%c, expectedCount=%d, calculatedCount=%d]",
                        expectedEntry.getKey(), expectedEntry.getValue(),
                        calculatedCounts.get(expectedEntry.getKey())));
                validationFlag = false;
            }
        }
        return validationFlag;
    }

    private static Integer testSimpleBarrierBucketHelper(Character character,
                                                         ConcurrentHashMap<Integer, Character> bucketMapping) {
        // ASSUMPTION: Assumes buckets assigned 0,1,2 ... N, without gaps
        // ASSUMPTION: Assumes characters are lower case
        int bucketCount = bucketMapping.size();
        int currBucket = 0;
        while (currBucket < bucketCount) {
            if (character.compareTo(bucketMapping.get(currBucket)) <= 0) {
                break;
            }
            currBucket++;
        }
        return currBucket;
    }

    private static class testSimpleBarrierRunnable implements Runnable {

        private CSvcClient cSvcClient;
        private ConcurrentHashMap<Character, Integer> characterToCountSharedIndex;
        private ConcurrentHashMap<Integer, Character> bucketMapping;
        private ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Character>> intermediaryBuckets;
        private String alphabeticString;
        private int assignedBucket;

        private testSimpleBarrierRunnable(CSvcClient cSvcClient,
                              ConcurrentHashMap<Character, Integer> characterToCountSharedIndex,
                              ConcurrentHashMap<Integer, Character> bucketMapping,
                              ConcurrentHashMap<Integer, ConcurrentLinkedQueue<Character>> intermediaryBuckets,
                              String alphabeticString, int assignedBucket) {
            this.cSvcClient = cSvcClient;
            this.characterToCountSharedIndex = characterToCountSharedIndex;
            this.bucketMapping = bucketMapping;
            this.intermediaryBuckets = intermediaryBuckets;
            this.alphabeticString = alphabeticString;
            this.assignedBucket = assignedBucket;
            // VALIDATION: Bucket exists
            if (Objects.isNull(bucketMapping.get(assignedBucket))
                    || Objects.isNull(intermediaryBuckets.get(assignedBucket))) {
                System.out.println("testSimpleBarrierRunnable - Failed Test " +
                        "- Assigned Bucket not contained in bucket mappings");
                throw new RuntimeException("Assigned Bucket not contained in bucket mappings");
            }
        }

        @Override
        public void run() {
            try {
                int index = 0;
                int maxIndex = alphabeticString.length();
                char currChar;
                Integer bucketIndex;
                while (index < maxIndex) {
                    currChar = alphabeticString.charAt(index);
                    bucketIndex = testSimpleBarrierBucketHelper(currChar, bucketMapping);
                    intermediaryBuckets.get(bucketIndex).add(currChar);
                    index++;
                }
                boolean barrierEngagedFlag = false;
                Future<Boolean> barrierTriggeredFlag = null;
                int retryCount = 0;
                int retryMax = 10;
                while (!barrierEngagedFlag && retryCount < retryMax) {
                    try {
                        barrierTriggeredFlag = CSvcSimpleBarrierUtil.engageBarrier(cSvcClient, "testSimpleBarrier", 3);
                        barrierEngagedFlag = true;
                    } catch (InvalidServiceStateException e) {
                        // NOTE: No-Op
                        // TODO: REMOVE
                        e.printStackTrace(System.out);
                    } catch (Exception e) {
                        System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Hidden Exception");
                        e.printStackTrace(System.out);
                    }
                    retryCount++;
                }

                if (!barrierEngagedFlag) {
                    System.out.println("testSimpleBarrierRunnable - Failed Test - Did not engage barrier");
                    throw new RuntimeException("Failed Test - Did not engage barrier");
                }

                try {
                    if (!barrierTriggeredFlag.get()) {
                        System.out.println("testSimpleBarrierRunnable - Failed Test - Did not successfully pass barrier");
                        throw new RuntimeException("Failed Test - Did not successfully pass barrier");
                    }
                } catch (InterruptedException | ExecutionException e) {
                    System.out.println("Failed Test - Exception Waiting for barrier");
                    throw new RuntimeException(e);
                }

                for (Character character : intermediaryBuckets.get(assignedBucket)) {
                    characterToCountSharedIndex.merge(character, 1, Integer::sum);
                }
            } catch (Exception e) {
                System.out.println("\t\tREMOVE ME - testSimpleBarrierRunnable - Hidden Exception");
                e.printStackTrace(System.out);
            }
        }
    }

    private static CSvcClient generateCSvcClient(CSvcClient.ManagerType type) throws InvalidServiceStateException {
        CSvcClient cSvcClient;
        try {
            switch (type) {
                case DB_VARIANT:
                    cSvcClient = CSvcClient.generateDBManagerClient(null, "/");
                    break;
                case FS_VARIANT:
                    cSvcClient = CSvcClient.generateFSManagerClient("/mnt/efs/fs1/CSVC_TEST", "/");
                    break;
                default:
                    throw new InvalidServiceStateException("Invalid value");
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new InvalidServiceStateException(e);
        }
        return cSvcClient;
    }
}
