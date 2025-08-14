package org.example;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZKSandboxUserApp {

    public static void main(String[] args) {

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

    public static void sandboxZKPerformanceTest() throws IOException, InterruptedException, KeeperException {
        System.out.println("Testing sandboxZKPerformanceTest:");
        String generatedString = RandomStringUtils.randomAlphanumeric(1000);
        byte[] data = generatedString.getBytes(StandardCharsets.UTF_8);
        Integer zk_port = 2181;
//        String connection_string = "ec2-44-192-116-184.compute-1.amazonaws.com:"+zk_port.toString();
        String connection_string = "ec2-44-198-61-232.compute-1.amazonaws.com:" + zk_port.toString();
        CountDownLatch connectionLatch = new CountDownLatch(1);
//        ZooKeeper zk = new ZooKeeper(zk_port, 3000, this);
        ZooKeeper zk = null;
        try {
            zk = connect(connection_string, connectionLatch);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        String testRootPath = "/redesignPerformanceTest";
        String path = String.format("%s", testRootPath);
        StopWatch watch = new StopWatch();
        watch.start();
        zk.create(
                path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        // Create 20 subnodes with 20 children each
        int i;
        int j;
        for (i = 0; i < 20; i++) {
            path = String.format("%s/%d", testRootPath, i);
            zk.create(
                    path,
                    data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            for (j = 0; j < 20; j++) {
                path = String.format("%s/%d/%d", testRootPath, i, j);
                zk.create(
                        path,
                        data,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        }
        watch.stop();
        System.out.println(String.format("Test complete [execution_time_ms=%s]", watch.getTime(TimeUnit.MILLISECONDS)));
    }

    public static void sandboxZktest() throws IOException, ParseException, InterruptedException {
//        // NOTE: EFS Test
//        CommandOutput result;
//
//        //CSvcManagerAmazonDB_Sandbox manager = new CSvcManagerAmazonDB_Sandbox(null);
//
//
//        CSvcManagerFileSystem manager = new CSvcManagerFileSystem("ec2-44-198-61-232.compute-1.amazonaws.com",
//                8001, "/mnt/efs/fs1/", Paths.get("/mnt/efs/fs1/csvc.log"), "/");
//        CSvcSandboxParser parser = new CSvcSandboxParser();
////            parser.parseCommand("create /databaseTest contents");
//////            CSvcCommand cmd = parser.processCommand();
////            CSvcCommand cmd = CreateCommand.generateCreateCommand("/databaseTest", "contents");
//        String cmdStr = "create /databaseTest contents";
//        String[] tokens = cmdStr.split(" ");
//        parser.parseOptions(tokens);
//        CSvcCommand cmd = parser.processCommand();
//        cmd.setManager(manager);
//        System.out.println(cmd.getCmdStr());
////            System.out.println(cmd.getArgCount());
//        // TODO: refactor manager population up to the parser level so
//        //  that all parsed command instances have that manager
//        cmd = CreateCommand.generateCreateCommand("/efsTest", "contents");
//        cmd.setManager(manager);
//        System.out.println(cmd.getCmdStr());
//        System.out.println(cmd.getArgCount());
//        result = cmd.executeManager();
//        System.out.println(String.format("[cmd=%s, errorMsg=%s, output=%s]",
//                cmd.getCmdStr(), result.getErrorMsg(), result.getOutput()));
//        Thread.sleep(250);
//        // parser.parseCommand("get /databaseTest");
//        //cmd = GetCommand.generateGetCommand("/databaseTest");
//        cmd = GetCommand.generateGetCommand("/efsTest");
//        cmd.setManager(manager);
//        System.out.println(cmd.getCmdStr());
//        System.out.println(cmd.getArgCount());
//        cmd.setManager(manager);
//        result = cmd.executeManager();
//        System.out.println(String.format("[cmd=%s, errorMsg=%s, output=%s]",
//                cmd.getCmdStr(), result.getErrorMsg(), result.getOutput()));
    }


}
