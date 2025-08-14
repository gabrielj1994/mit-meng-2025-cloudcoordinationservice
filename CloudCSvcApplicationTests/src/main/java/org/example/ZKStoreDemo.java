package org.example;

import io.javalin.Javalin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.zookeeper.*;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.example.manager.ZKSvcManager;
import org.example.model.Record;
import org.example.util.HTTPUtil;
import org.example.util.ZKConfigs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ZKStoreDemo {

    private static Options options = new Options();

    static {
        options.addOption(new Option("mnc", true, "max node count"));
        options.addOption(new Option("bpn", true, "bytes per node"));
    }

    public static void main(String[] args) {
        int flag = Integer.parseInt(args[0]);
//        int maxNodeCount = 20;
        boolean[] isRunning = {true};

        DefaultParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(String.format("CSvcFSStoreDemo - Failed command line options parsing"));
            throw new RuntimeException(e);
        }

        int maxNodeCount = cl.hasOption("mnc") ? Integer.parseInt(cl.getOptionValue("mnc")) : 20;
        int bytesPerNode = cl.hasOption("bpn") ? Integer.parseInt(cl.getOptionValue("bpn")) : 1000;


        if (flag == 0) {
            String zkHostPort = System.getProperty("zk.url");
            ZKSvcManager zkSvcManager = new ZKSvcManager(zkHostPort);
            zkSvcManager.createPersistenceParent();
//            int bytesPerNode = 1000;
            String data = RandomStringUtils.randomAlphanumeric(bytesPerNode);
//            byte[] data = generatedString.getBytes(StandardCharsets.UTF_8);

            testZKCreates(zkSvcManager, data, bytesPerNode);
//            int i = 0;
//            int j = 0;
//            int maxCount = 20;
//            StopWatch watch = new StopWatch();
//            watch.start();
//            for (i = 0; i < maxCount; i++) {
//                zkSvcManager.addToPersistence(String.format("dir_%d", i), data);
//                for (j = 0; j < maxCount; j++) {
//                    zkSvcManager.addToPersistence(String.format("dir_%d/node_%d", i, j), data);
//                }
//            }
//            watch.stop();
//            System.out.println(String.format("ZKStoreDemo CREATEs complete [execution_time_ms=%s," +
//                                                " node_count=%d, bytes_per_node=%d]",
//                                                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1),
//                                                bytesPerNode));

            // NOTE: Mutate nodes
            data = RandomStringUtils.randomAlphanumeric(bytesPerNode);
            testZKSet(zkSvcManager, data, bytesPerNode);
//            data = RandomStringUtils.randomAlphanumeric(bytesPerNode);
////            data = data.getBytes(StandardCharsets.UTF_8);
//            watch.reset();
//            watch.start();
//            for (i = 0; i < maxCount; i++) {
//                zkSvcManager.updatePersistence(String.format("dir_%d", i), data);
//                for (j = 0; j < maxCount; j++) {
//                    zkSvcManager.updatePersistence(String.format("dir_%d/node_%d", i, j), data);
//                }
//            }
//            watch.stop();
//            System.out.println(String.format("ZKStoreDemo SETs complete [execution_time_ms=%s," +
//                                                " node_count=%d, bytes_per_node=%d]",
//                                                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1),
//                                                bytesPerNode));

            // NOTE: Delete nodes
            testZKDelete(zkSvcManager);
//            watch.reset();
//            watch.start();
//            for (i = 0; i < maxCount; i++) {
//                for (j = 0; j < maxCount; j++) {
//                    zkSvcManager.removeFromPersistence(String.format("dir_%d/node_%d", i, j));
//                }
//                zkSvcManager.removeFromPersistence(String.format("dir_%d", i));
//            }
//            watch.stop();
//            System.out.println(String.format("ZKStoreDemo DELETEs complete [execution_time_ms=%s, node_count=%d]",
//                                                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1)));

        } else if (flag == 1) {
            // NOTE: No delete
            String zkHostPort = System.getProperty("zk.url");
            ZKSvcManager zkSvcManager = new ZKSvcManager(zkHostPort);
            zkSvcManager.createPersistenceParent();
//            int bytesPerNode = 1000;
            if (args.length > 2) {
                bytesPerNode = Integer.parseInt(args[2]);
            }
            String data = RandomStringUtils.randomAlphanumeric(bytesPerNode);

            testZKCreates(zkSvcManager, data, bytesPerNode);

            data = RandomStringUtils.randomAlphanumeric(bytesPerNode);
            testZKSet(zkSvcManager, data, bytesPerNode);
        } else if (flag == 2) {
            // NOTE: Deletes only for clean up
            String zkHostPort = System.getProperty("zk.url");
            ZKSvcManager zkSvcManager = new ZKSvcManager(zkHostPort);
            zkSvcManager.createPersistenceParent();

            testZKDelete(zkSvcManager);
        } else if (flag == 3) {
            // NOTE: Test out of the box ZK
            try {
                sandboxZKPerformanceTest(maxNodeCount);
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 4) {
            try {
                String followerTarget = "ec2-3-220-230-216.compute-1.amazonaws.com";
                if (args.length > 1) {
                    followerTarget = args[1];
                }
                int maxRequest = 25;
                if (args.length > 2) {
                    maxRequest = Integer.parseInt(args[2]);
                }

//                int bytesPerNode = 1000;
                if (args.length > 3) {
                    bytesPerNode = Integer.parseInt(args[3]);
                }


                testFollowerTimeDelayRequester(isRunning, followerTarget, maxRequest, bytesPerNode);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 5) {
            testFollowerTimeDelayFollower(isRunning);
        } else if (flag == 6) {
            String targetNode = args[1];
            try {
                testZKDelayWrite(targetNode);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 7) {
            String targetNode = args[1];
            try {
                testZKDelayRead(targetNode);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void testZKDelayWrite(String targetNode) throws InterruptedException {
        Integer zk_port = 2181;
        String connection_string = "127.0.0.1:" + zk_port.toString();
        CountDownLatch connectionLatch = new CountDownLatch(1);
        String generatedString = RandomStringUtils.randomAlphanumeric(1000);
//        byte[] data = generatedString.getBytes(StandardCharsets.UTF_8);
//        ZooKeeper zk = new ZooKeeper(zk_port, 3000, this);
        ZooKeeper zk = null;
        String path = String.format("/retrieveTest/%s", targetNode);
        try {
            zk = connect(connection_string, connectionLatch);
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(System.currentTimeMillis());

        try {
            zk.create(path,
                    buffer.array(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (Exception e) {
            // no-op
        }
        buffer.clear();
        //==
        int count = 0;
        int maxCount = 20;
        int failureThreshold = 5;
        int consecutiveFailureCounter = 0;
        long ownerId = 12345L;
        while (count < maxCount && consecutiveFailureCounter < failureThreshold) {
            try {
//                try (FileChannel fileChannel = FileChannel.open(testFile, StandardOpenOption.WRITE,
//                        StandardOpenOption.CREATE)) {
//                    buffer.putLong(ownerId);
//                    buffer.putLong(System.currentTimeMillis());
//                    buffer.flip();
//                    fileChannel.write(buffer);
//                    buffer.clear();
//                }
                //===
                buffer.clear();
                buffer.putLong(System.currentTimeMillis());
                zk.setData(path, buffer.array(), -1);
            } catch (Exception e) {
                consecutiveFailureCounter++;
                continue;
            }
            count++;
            consecutiveFailureCounter = 0;
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                zk.close();
                throw new RuntimeException(e);
            }
        }
        try {
            zk.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void testZKDelayRead(String targetNode) throws IOException {
        Integer zk_port = 2181;
        String connection_string = "127.0.0.1:" + zk_port.toString();
        CountDownLatch connectionLatch = new CountDownLatch(1);
        String generatedString = RandomStringUtils.randomAlphanumeric(1000);
        byte[] data = generatedString.getBytes(StandardCharsets.UTF_8);
//        ZooKeeper zk = new ZooKeeper(zk_port, 3000, this);
        ZooKeeper zk = null;
        try {
            zk = connect(connection_string, connectionLatch);
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

        // NOTE: Ensure baseline nodes created
        String path;
        try {
            path = String.format("/retrieveTest");
            zk.create(path,
                    data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (Exception e) {
            // no-op
        }
        //===
        int count = 0;
        int maxCount = 20;
        int failureThreshold = 5;
        int consecutiveFailureCounter = 0;
        long fileTimestamp;
        long prevFiletimestamp = -1;
        long processTimestamp;
        long timeDelta;
        ArrayList<ArrayList<Long>> resultLog = new ArrayList<>();
        ByteBuffer buffer;
        MappedByteBuffer mappedBuffer;
        buffer = ByteBuffer.allocate(Long.BYTES);
        while (count < maxCount) {
//            try {
//            mappedBuffer = retrieveMBBuffer(testFile);
            processTimestamp = System.currentTimeMillis();
//                buffer = ByteBuffer.wrap(Files.readAllBytes(testFile));
//                if (buffer.array().length < Long.BYTES) {
//                    continue;
//                }
            try {
//                fileTimestamp = getTimestamp(mappedBuffer);
//                final int RECORD_SIZE = Long.BYTES * 2; // 8 bytes owner ID + 8 bytes timestamp
//                try (FileChannel fileChannel = FileChannel.open(testFile, StandardOpenOption.READ)) {
////                try (SeekableByteChannel fileChannel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
////                    mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, RECORD_SIZE);
////                    fileTimestamp = getTimestamp(mappedBuffer);
//                    fileChannel.position(Long.BYTES);
//                    fileChannel.read(buffer);
//                    buffer.flip();
//                    fileTimestamp = buffer.getLong();
//                    buffer.clear();
//                }
                //===
                byte[] result = zk.getData(String.format("/retrieveTest/%s", targetNode),
                        null, null);
                fileTimestamp = ByteBuffer.wrap(result).getLong();
//            } catch (IndexOutOfBoundsException | IOException e) {
            } catch (IndexOutOfBoundsException e) {
                // NOTE: IOException from unexpected file size
                //       NoSuchFileException from missing file
                continue;
            } catch (Exception e) {
                continue;
            }
            if (fileTimestamp != prevFiletimestamp) {
                count++;
                prevFiletimestamp = fileTimestamp;
                timeDelta = Math.abs(processTimestamp - prevFiletimestamp);
                log.info(String.format("testZKDelayRead - New timestamp" +
                                " [local_timestamp=%d, remote_timestamp=%d, delta=%d]",
                        processTimestamp, prevFiletimestamp, timeDelta));
                resultLog.add(new ArrayList<>(Arrays.asList(processTimestamp, prevFiletimestamp, timeDelta)));
            }
//            }
//            consecutiveFailureCounter = 0;
        }
        log.info("FINISHED TEST - PRINTING TIMESTAMPS");
        for (ArrayList<Long> entry : resultLog) {
            log.info(String.format("testZKDelayRead - Timestamp entry [entry=%s]", entry.toString()));
        }
        try {
            zk.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Summary of current ZK test (testFollowerTimeDelayRequester & testFollowerTimeDelayFollower)
     * 2 nodes, one follower with javalin end points
     * - the nodes should be run on 2 follower zk machines, not the leader
     * ==
     * calculate baseline return trip latency with a known existing key
     * calculate zk baseline create latency by evaluating zk.create latency with no javalin request
     * one node creates in ZK, and requests through javalin for created node to confirm write
     *
     * @param isRunning
     * @param followerTarget
     * @param maxRequest
     * @param bytesPerNode
     * @throws Exception
     */
    private static void testFollowerTimeDelayRequester(boolean[] isRunning, String followerTarget,
                                                       int maxRequest, int bytesPerNode) throws Exception {
        HttpClient client = new HttpClient();
        client.start();
        StopWatch watch = new StopWatch();
        int requestCount = 0;
        int maxBaseRequest = 10;
        while (requestCount < maxBaseRequest) {
            requestCount++;
            watch.reset();
            watch.start();
            ContentResponse response = client.GET("http://"+followerTarget+":9100" +
                    "/zkdemo/records/dummyKey");
//            log.info(String.format("testJavalinEndpoints - GET RESPONSE [getRecord_response=%s]",
//                    SerializationUtils.deserialize(response.getContent()).toString()));
            watch.stop();
            System.out.println(String.format("ZKStoreDemo Baseline Request [execution_time_microseconds=%s]",
                    watch.getTime(TimeUnit.MICROSECONDS)));
        }

        Integer zk_port = 2181;
        String connection_string = "127.0.0.1:" + zk_port.toString();
        CountDownLatch connectionLatch = new CountDownLatch(1);
        ZooKeeper zk = null;
        try {
            zk = connect(connection_string, connectionLatch);
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

        requestCount = 0;
//        maxRequest = 10;
        String path;
        String nodeName;
        String generatedString = RandomStringUtils.randomAlphanumeric(bytesPerNode);
        byte[] data = generatedString.getBytes(StandardCharsets.UTF_8);
        while (requestCount < maxBaseRequest) {
            requestCount++;
            watch.reset();
            nodeName = "node_createBaseline_"+requestCount;
            path = String.format("/retrieveTest/%s", nodeName);
            watch.start();
            // NOTE: subtract ~5 ms
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//            log.info(String.format("testJavalinEndpoints - GET RESPONSE [getRecord_response=%s]",
//                    SerializationUtils.deserialize(response.getContent()).toString()));
            watch.stop();
            System.out.println(String.format("ZKStoreDemo Create Node Baseline [execution_time_microseconds=%s]",
                    watch.getTime(TimeUnit.MICROSECONDS)));
            // NOTE: Clean up
            zk.delete(path, -1);
        }

        requestCount = 0;
//        maxRequest = 25;
        long maxLatency = 0;
        StopWatch midWatch = new StopWatch();
        watch.reset();
        watch.start();
        while (requestCount < maxRequest) {
            requestCount++;
            midWatch.reset();
            nodeName = "node_"+requestCount;
            path = String.format("/retrieveTest/%s", nodeName);
            midWatch.start();
            // NOTE: subtract ~5 ms
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            ContentResponse response = client.GET(String.format("http://"+followerTarget+":9100" +
                    "/zkdemo/records/%s", nodeName));
//            log.info(String.format("testJavalinEndpoints - GET RESPONSE [getRecord_response=%s]",
//                    SerializationUtils.deserialize(response.getContent()).toString()));
            midWatch.stop();
            maxLatency = Math.max(maxLatency, midWatch.getTime(TimeUnit.MILLISECONDS));
//            System.out.println(String.format("ZKStoreDemo New Node Retrieve [execution_time_ms=%s]",
//                    watch.getTime(TimeUnit.MILLISECONDS)));
            // NOTE: Clean up
//            zk.delete(path, -1);
        }
        watch.stop();
        System.out.println(String.format("ZKStoreDemo CREATEs & Replicate complete [execution_time_ms=%s," +
                        " node_count=%d, bytes_per_node=%d]",
                watch.getTime(TimeUnit.MILLISECONDS), maxRequest,
                bytesPerNode));
        requestCount = 0;
        while (requestCount < maxRequest) {
            requestCount++;
            nodeName = "node_"+requestCount;
            path = String.format("/retrieveTest/%s", nodeName);
            // NOTE: Clean up
            zk.delete(path, -1);
        }
        client.stop();

//        while (isRunning[0]) {
//            try {
//                Thread.sleep(25);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
    }

    /**
     * Summary of current ZK test (testFollowerTimeDelayRequester & testFollowerTimeDelayFollower)
     * 2 nodes, one follower with javalin end points
     * - the nodes should be run on 2 follower zk machines, not the leader
     * ==
     * calculate baseline return trip latency with a known existing key
     * calculate zk baseline create latency by evaluating zk.create latency with no javalin request
     * one node creates in ZK, and requests through javalin for created node to confirm write
     *
     * @param isRunning
     */
    private static void testFollowerTimeDelayFollower(boolean[] isRunning) {
        Integer zk_port = 2181;
        String connection_string = "127.0.0.1:" + zk_port.toString();
        CountDownLatch connectionLatch = new CountDownLatch(1);
        String generatedString = RandomStringUtils.randomAlphanumeric(1000);
        byte[] data = generatedString.getBytes(StandardCharsets.UTF_8);
//        ZooKeeper zk = new ZooKeeper(zk_port, 3000, this);
        ZooKeeper zk = null;
        try {
            zk = connect(connection_string, connectionLatch);
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

        // NOTE: Ensure baseline nodes created
        String path;
        try {
            path = String.format("/retrieveTest");
            zk.create(path,
                    data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (Exception e) {
            // no-op
        }

        try {
            path = String.format("/retrieveTest/dummyKey");
            zk.create(path,
                    data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (Exception e) {
            // no-op
        }

        try (Javalin app = Javalin.create()) {
            app.start(9100); // DIFFERENT PORT
//                app.get("/javalindemo", ctx -> ctx.html("Hello world!"));
            ZooKeeper finalZk = zk;

            app.get("/zkdemo/records/{key}", ctx -> {
                String key = Objects.requireNonNull(ctx.pathParam("key"));
                boolean retryFlag = true;
                while (retryFlag) {
                    try {
                        byte[] result = finalZk.getData(String.format("/retrieveTest/%s", key),
                                null, null);
                        if (result.length < 1000) {
                            System.out.println(String.format("CRITICAL ERROR - ZKStoreDemo -" +
                                                " Lower expected data length [length=%d]", result.length));
                        }
                        ctx.result(result);
                        retryFlag = false;
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                    }
                }
            });
            app.get("/zkdemo/kill", ctx -> {
                ctx.html("Bye world!");
                isRunning[0] = false;
            });

            while (isRunning[0]) {
                Thread.sleep(25);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static ZooKeeper connect(String host_port, CountDownLatch connectionLatch)
            throws IOException,
            InterruptedException {
        ZooKeeper zoo = new ZooKeeper(host_port, 2000, new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            }
        });

        connectionLatch.await();
        return zoo;
    }

    public static void sandboxZKPerformanceTest(int maxCount) throws InterruptedException, KeeperException {
        System.out.println("Testing sandboxZKPerformanceTest:");
        String generatedString = RandomStringUtils.randomAlphanumeric(1000);
        byte[] data = generatedString.getBytes(StandardCharsets.UTF_8);
        Integer zk_port = 2181;
//        String connection_string = "ec2-44-192-116-184.compute-1.amazonaws.com:"+zk_port.toString();
//        String connection_string = "ec2-44-198-61-232.compute-1.amazonaws.com:" + zk_port.toString();
        String connection_string = "127.0.0.1:" + zk_port.toString();
        CountDownLatch connectionLatch = new CountDownLatch(1);
//        ZooKeeper zk = new ZooKeeper(zk_port, 3000, this);
        ZooKeeper zk = null;
        try {
            zk = connect(connection_string, connectionLatch);
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

        String testRootPath = "/redesignPerformanceTest";
        String path = String.format("%s", testRootPath);
        StopWatch watch = new StopWatch();
        watch.start();
        try {
            zk.create(
                    path,
                    data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (Exception e) {
            // no op
            e.printStackTrace(System.out);
        }


        // Create 20 subnodes with 20 children each
        int i;
        int j;
        for (i = 0; i < maxCount; i++) {
            path = String.format("%s/%d", testRootPath, i);
            zk.create(
                    path,
                    data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            zk.sync(path, null, null);
            for (j = 0; j < maxCount; j++) {
                path = String.format("%s/%d/%d", testRootPath, i, j);
                zk.create(
                        path,
                        data,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                zk.sync(path, null, null);
            }
        }
        watch.stop();
        System.out.println(String.format("ZKStoreDemo CREATE complete [execution_time_ms=%s, node_count=%d]",
                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1)));

        watch.reset();
        watch.start();
        for (i = 0; i < maxCount; i++) {
            for (j = 0; j < maxCount; j++) {
                path = String.format("%s/%d/%d", testRootPath, i, j);
                zk.delete(
                        path,
                        -1);
            }
            path = String.format("%s/%d", testRootPath, i);
            zk.delete(
                    path,
                    -1);
        }
        watch.stop();
        System.out.println(String.format("ZKStoreDemo DELETEs complete [execution_time_ms=%s, node_count=%d]",
                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1)));
    }

    public static void testZKCreates(ZKSvcManager zkSvcManager, String data, int bytesPerNode) {
        int i;
        int j;
        int maxCount = 20;
        StopWatch watch = new StopWatch();
        watch.start();
        for (i = 0; i < maxCount; i++) {
            zkSvcManager.addToPersistence(String.format("dir_%d", i), data);
            for (j = 0; j < maxCount; j++) {
                zkSvcManager.addToPersistence(String.format("dir_%d/node_%d", i, j), data);
            }
        }
        watch.stop();
        System.out.println(String.format("ZKStoreDemo CREATEs complete [execution_time_ms=%s," +
                        " node_count=%d, bytes_per_node=%d]",
                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1),
                bytesPerNode));
    }

    public static void testZKSet(ZKSvcManager zkSvcManager, String data, int bytesPerNode) {
        int i;
        int j;
        int maxCount = 20;
        StopWatch watch = new StopWatch();
        watch.reset();
        watch.start();
        for (i = 0; i < maxCount; i++) {
            zkSvcManager.updatePersistence(String.format("dir_%d", i), data);
            for (j = 0; j < maxCount; j++) {
                zkSvcManager.updatePersistence(String.format("dir_%d/node_%d", i, j), data);
            }
        }
        watch.stop();
        System.out.println(String.format("ZKStoreDemo SETs complete [execution_time_ms=%s," +
                        " node_count=%d, bytes_per_node=%d]",
                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1),
                bytesPerNode));
    }

    public static void testZKDelete(ZKSvcManager zkSvcManager) {
        int i;
        int j;
        int maxCount = 20;
        StopWatch watch = new StopWatch();
        watch.start();
        for (i = 0; i < maxCount; i++) {
            for (j = 0; j < maxCount; j++) {
                zkSvcManager.removeFromPersistence(String.format("dir_%d/node_%d", i, j));
            }
            zkSvcManager.removeFromPersistence(String.format("dir_%d", i));
        }
        watch.stop();
        System.out.println(String.format("ZKStoreDemo DELETEs complete [execution_time_ms=%s, node_count=%d]",
                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1)));
    }

    public static void testEndpointPerformance() throws Exception {
        HttpClient client = new HttpClient();
        client.start();
        ArrayList<byte[]> inputRecords = new ArrayList<>();
        String baseKey = "performanceKey";
        String baseOwner = "performanceOwner";
        byte[] generatedBytes = RandomStringUtils.randomAlphanumeric(10).getBytes(StandardCharsets.UTF_8);
        int count = 0;
        while (count < 1000) {
            inputRecords.add(SerializationUtils.serialize(
                    new Record(baseKey+count, baseOwner+count, generatedBytes)));
            count++;
        }
        StopWatch watch = new StopWatch();
        watch.start();
        for (byte[] serializedDump : inputRecords) {
            ContentResponse bytesResponse = client.POST(String.format("http://%s%s",
                    ZKConfigs.getHostPostOfServer(),
                    HTTPUtil.UPDATE_RECORD_ENDPOINT)).content(
                    new BytesContentProvider(serializedDump)).send();
//            ContentResponse bytesResponse = client.POST(String.format("http://10.0.0.176:7100%s",
//                    HTTPUtil.UPDATE_RECORD_ENDPOINT)).content(
//                    new BytesContentProvider(serializedDump)).send();
        }
        watch.stop();
        System.out.println(String.format(
                "Test complete [recordCount=%d, dataBytesCount=%d, execution_time_ms=%s]",
                count, generatedBytes.length, watch.getTime(TimeUnit.MILLISECONDS)));
        client.stop();
    }

    public static void testJavalinHelloWorld() {
//        Javalin app = Javalin.create()
//                .port(7000)
//                .start();
        boolean[] isRunning = {true};
        try (Javalin app = Javalin.create()) {
//        Javalin app = Javalin.create();
            app.start(7100);
            app.get("/javalindemo", ctx -> ctx.html("Hello world!"));
            app.get("/javalindemo/kill", ctx -> {
                ctx.html("Bye world!");
                isRunning[0] = false;
            });

            app.post("/javalindemo/input", ctx -> {
                ctx.html("===\n"+
                        ctx.body()+
                        "\n===\n");
                ctx.status(200);
            });


            while (isRunning[0]) {
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static private void testJavalinEndpoints() throws Exception {
        HttpClient client = new HttpClient();
        client.start();
        ContentResponse response = client.GET("http://10.0.0.176:7100/javalindemo/records");
        log.info(String.format("testJavalinEndpoints - GET RESPONSE [allRecords_response=%s]", response.getContentAsString()));

        response = client.GET("http://localhost:7100/javalindemo/records/dummyKey");
        log.info(String.format("testJavalinEndpoints - GET RESPONSE [getRecord_response=%s]", response.getContentAsString()));

        ContentResponse bytesResponse = client.POST("http://10.0.0.176:7100/javalindemo/input").content(
                new BytesContentProvider("appKey|||appOwner|||54321".getBytes(StandardCharsets.UTF_8))).send();

        response = client.GET("http://10.0.0.176:7100/javalindemo/records");
        log.info(String.format("testJavalinEndpoints - GET RESPONSE [allRecords_response=%s]", response.getContentAsString()));
        client.stop();
//        HttpRequest request = new HttpRequest(URI.create("https://example.com/"))
//                .build();
//        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
//        System.out.println("response body: " + response.body());
    }
}
