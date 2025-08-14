package org.example;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.command.CommandOutput;
import dsg.ccsvc.command.LsCommand;
import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent;
import dsg.ccsvc.util.DebugConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.example.manager.CoordinationSvcManager;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static dsg.ccsvc.client.CSvcClient.ManagerType.*;
import static org.example.util.ZKConfigs.TEMP_NODE;

@Slf4j
public class CSvcDBStoreDemo {

    private static Options options = new Options();

    static {
        options.addOption(new Option("df", false, "debug flag"));
        options.addOption(new Option("lf", false, "log flag"));
        options.addOption(new Option("mf", false, "metric flag"));
        options.addOption(new Option("mnc", true, "max node count"));
        options.addOption(new Option("bpn", true, "bytes per node"));
    }

    public static void main(String[] args) {
        int flag = Integer.parseInt(args[0]);
        // TODO: Update with parser
//        DefaultParser parser = new DefaultParser();
//        try {
//            CommandLine cl = parser.parse(options, args);
//        } catch (ParseException e) {
//            System.out.println(String.format("CSvcDBStoreDemo - Failed command line options parsing"));
//            throw new RuntimeException(e);
//        }
        int maxNodeCount = 20;
        if (args.length > 4) {
            // NOTE: Enable all prints
            System.out.println(String.format("DEBUG - CSvcDBStoreDemo - DEBUG LOGGING ENABLED"));
            DebugConstants.DEBUG_FLAG = true;
        }

        if (args.length > 2) {
            // NOTE: Enable logreader
            System.out.println(String.format("DEBUG - CSvcDBStoreDemo - LOGREADER ENABLED"));
            DebugConstants.LOGREADER_FLAG = true;
        }

        if (args.length > 1) {
            maxNodeCount = Integer.parseInt(args[1]);
        }
        int bytesPerNode = 1000;
        if (args.length > 3) {
            bytesPerNode = Integer.parseInt(args[3]);
        }
        String generatedString = RandomStringUtils.randomAlphanumeric(bytesPerNode);
        byte[] data = generatedString.getBytes(StandardCharsets.UTF_8);
        if (flag == 0) {
//            String zkHostPort = System.getProperty("zk.url");
//            ZKSvcManager zkSvcManager = new ZKSvcManager(zkHostPort);
//            zkSvcManager.createPersistenceParent();
            CoordinationSvcManager cSvcManager;
            try {
                cSvcManager = new CoordinationSvcManager(DB_VARIANT);
                cSvcManager.start();
            } catch (InvalidServiceStateException e) {
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                cSvcManager.createPersistenceParent();

//                String generatedString = RandomStringUtils.randomAlphanumeric(bytesPerNode);
//                byte[] data = generatedString.getBytes(StandardCharsets.UTF_8);
                testCSvcCreate(cSvcManager, maxNodeCount, data);
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
    //            System.out.println(String.format("CSvcDBStoreDemo CREATEs complete [execution_time_ms=%s," +
    //                                                " node_count=%d, bytes_per_node=%d]",
    //                                                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1),
    //                                                bytesPerNode));

                // NOTE: Mutate nodes
                generatedString = RandomStringUtils.randomAlphanumeric(bytesPerNode);
                data = generatedString.getBytes(StandardCharsets.UTF_8);
                testCSvcSet(cSvcManager, maxNodeCount, data);
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
    //            System.out.println(String.format("CSvcDBStoreDemo SETs complete [execution_time_ms=%s," +
    //                                                " node_count=%d, bytes_per_node=%d]",
    //                                                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1),
    //                                                bytesPerNode));

                // NOTE: Delete nodes
                testCSvcDelete(cSvcManager, maxNodeCount);
    //            watch.reset();
    //            watch.start();
    //            for (i = 0; i < maxCount; i++) {
    //                for (j = 0; j < maxCount; j++) {
    //                    zkSvcManager.removeFromPersistence(String.format("dir_%d/node_%d", i, j));
    //                }
    //                zkSvcManager.removeFromPersistence(String.format("dir_%d", i));
    //            }
    //            watch.stop();
    //            System.out.println(String.format("CSvcDBStoreDemo DELETEs complete [execution_time_ms=%s, node_count=%d]",
    //                                                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1)));
//                endlessLoop();
            } catch (InvalidServiceStateException e) {
                e.printStackTrace(System.out);
                throw new RuntimeException(e);
            } finally {
                try {
                    cSvcManager.close();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } else if (flag == 1) {
            // NOTE: No delete
            CoordinationSvcManager cSvcManager;
            try {
                cSvcManager = new CoordinationSvcManager(DB_VARIANT);
                cSvcManager.start();
            } catch (InvalidServiceStateException e) {
                throw new RuntimeException(e);
            }
            try {
                cSvcManager.createPersistenceParent();
//                int bytesPerNode = 1000;
//                String data = RandomStringUtils.randomAlphanumeric(bytesPerNode);

                testCSvcCreate(cSvcManager, maxNodeCount, data);

                generatedString = RandomStringUtils.randomAlphanumeric(bytesPerNode);
                data = generatedString.getBytes(StandardCharsets.UTF_8);
                testCSvcSet(cSvcManager, maxNodeCount, data);
            } catch (InvalidServiceStateException e) {
                e.printStackTrace(System.out);
                throw new RuntimeException(e);
            }
        } else if (flag == 2) {
            // NOTE: Deletes only for clean up
            CoordinationSvcManager cSvcManager;
            try {
                cSvcManager = new CoordinationSvcManager(DB_VARIANT);
                cSvcManager.start();

                cSvcManager.createPersistenceParent();

                testCSvcDelete(cSvcManager, maxNodeCount);
            } catch (InvalidServiceStateException e) {
                throw new RuntimeException(e);
            }
            try {
                cSvcManager.close();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 3) {
            // NOTE: Long running "main" node
            DebugConstants.LOGREADER_FLAG = true;
            CoordinationSvcManager cSvcManager;
            try {
                cSvcManager = new CoordinationSvcManager(DB_VARIANT);
                cSvcManager.start();

                try {
                    cSvcManager.createTempParent();
                } catch (InvalidServiceStateException e) {
                    // TODO: No-op for already exists.
                    //   need logic to handle other root causes
                }

                cSvcManager.registerChildrenChangeWatcher(TEMP_NODE,
                        generateGenericLSCallable());

                while (true) {
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException e) {
                        try {
                            cSvcManager.close();
                        } catch (InterruptedException ex) {
                            // NOTE: No-Op
                        }
                        throw new RuntimeException(e);
                    }
                }
            } catch (InvalidServiceStateException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 4) {
            // TODO: Remove this
            if (args.length > 1) {
                // NOTE: Enable all prints
                System.out.println(String.format("DEBUG - CSvcDBStoreDemo - DEBUG LOGGING ENABLED"));
                DebugConstants.DEBUG_FLAG = true;
            }

            // NOTE: Long running "restart" node
            CoordinationSvcManager cSvcManager = null;
            int count = 0;
            while (true) {
                try {
                    cSvcManager = new CoordinationSvcManager(DB_VARIANT);
                    cSvcManager.start();
                    try {
                        cSvcManager.createTempParent();
                    } catch (InvalidServiceStateException e) {
                        // NOTE: No-op
                    }
                    testCSvcCreateEphemeral(cSvcManager, maxNodeCount, data, count);
                    testCSvcSetEphemeral(cSvcManager, maxNodeCount, data, count);
                    count++;
                    Thread.sleep(500);
                } catch (InvalidServiceStateException | InterruptedException e) {
                    System.out.println(String.format("Failed iteration - Continuing"));
                    e.printStackTrace(System.out);
                } finally {
                    if (!Objects.isNull(cSvcManager)) {
                        try {
                            cSvcManager.close();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } else if (flag == 5) {
            CoordinationSvcManager cSvcManager;
            try {
                cSvcManager = new CoordinationSvcManager(FS_VARIANT);
                cSvcManager.start();
            } catch (InvalidServiceStateException e) {
                throw new RuntimeException(e);
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                cSvcManager.createPersistenceParent();
                testCSvcCreate(cSvcManager, maxNodeCount, data);

                // NOTE: Mutate nodes
                generatedString = RandomStringUtils.randomAlphanumeric(bytesPerNode);
                data = generatedString.getBytes(StandardCharsets.UTF_8);
                testCSvcSet(cSvcManager, maxNodeCount, data);

                // NOTE: Delete nodes
                testCSvcDelete(cSvcManager, maxNodeCount);
//                endlessLoop();
            } catch (InvalidServiceStateException e) {
                e.printStackTrace(System.out);
                throw new RuntimeException(e);
            } finally {
                try {
                    cSvcManager.close();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        } else if (flag == 6) {
            // NOTE: Long running "main" node
            DebugConstants.LOGREADER_FLAG = true;
            CoordinationSvcManager cSvcManager;
            try {
                cSvcManager = new CoordinationSvcManager(FS_VARIANT);
                cSvcManager.start();

                try {
                    cSvcManager.createTempParent();
                } catch (InvalidServiceStateException e) {
                    // TODO: No-op for already exists.
                    //   need logic to handle other root causes
                }

                cSvcManager.registerChildrenChangeWatcher(TEMP_NODE,
                        generateGenericLSCallable());

                while (true) {
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException e) {
                        try {
                            cSvcManager.close();
                        } catch (InterruptedException ex) {
                            // NOTE: No-Op
                        }
                        throw new RuntimeException(e);
                    }
                }
            } catch (InvalidServiceStateException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 7) {
            // TODO: Remove this
            if (args.length > 1) {
                // NOTE: Enable all prints
                System.out.println(String.format("DEBUG - CSvcDBStoreDemo - DEBUG LOGGING ENABLED"));
                DebugConstants.DEBUG_FLAG = true;
            }

            // NOTE: Long running "restart" node
            CoordinationSvcManager cSvcManager = null;
            int count = 0;
            while (true) {
                try {
                    cSvcManager = new CoordinationSvcManager(FS_VARIANT);
                    cSvcManager.start();
                    try {
                        cSvcManager.createTempParent();
                    } catch (InvalidServiceStateException e) {
                        // NOTE: No-op
                    }
                    testCSvcCreateEphemeral(cSvcManager, maxNodeCount, data, count);
                    testCSvcSetEphemeral(cSvcManager, maxNodeCount, data, count);
                    count++;
                    Thread.sleep(500);
                } catch (InvalidServiceStateException | InterruptedException e) {
                    System.out.println(String.format("Failed iteration - Continuing"));
                    e.printStackTrace(System.out);
                } finally {
                    if (!Objects.isNull(cSvcManager)) {
                        try {
                            cSvcManager.close();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void endlessLoop() {
        while (true) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void testCSvcCreateEphemeral(CoordinationSvcManager cSvcManager, int maxCount,
                                               byte[] data, int nodeSuffix) throws InvalidServiceStateException {
        int i;
//        int j;
        for (i = 0; i < maxCount; i++) {
            try {
                cSvcManager.addToTemporary(String.format("dir_%d_%d", nodeSuffix, i), data);
            } catch (InvalidServiceStateException e) {
                System.out.println(String.format("\tREMOVE ME - Failed Ephem Create" +
                                                    " [count=%d, idx=%d]", nodeSuffix, i));
            }
        }
    }

    public static void testCSvcSetEphemeral(CoordinationSvcManager cSvcManager, int maxCount,
                                            byte[] data, int nodeSuffix) throws InvalidServiceStateException {
        int i;
//        int j;
        for (i = 0; i < maxCount; i++) {
            cSvcManager.updateTemporary(String.format("dir_%d_%d", nodeSuffix, i), data);
        }

        String dataStr;
        for (i = 0; i < maxCount; i++) {
            try {
                dataStr = cSvcManager.getNodeData(String.format("%s/dir_%d_%d", TEMP_NODE, nodeSuffix, i));
            } catch (InvalidServiceStateException e) {
                System.out.println(String.format("\tREMOVE ME - Failed Data Read [count=%d, idx=%d]", nodeSuffix, i));
                continue;
            }
            assert(Arrays.equals(dataStr.getBytes(StandardCharsets.UTF_8), data));
        }
    }

    public static void testCSvcCreate(CoordinationSvcManager cSvcManager, int maxCount,
                                      byte[] data) throws InvalidServiceStateException {
        int i;
        int j;
        int bytesPerNode = data.length;
//        int maxCount = 20;
//        int maxCount = 2;
        StopWatch watch = new StopWatch();
        watch.start();
        for (i = 0; i < maxCount; i++) {
            cSvcManager.addToPersistence(String.format("dir_%d", i), data);
            for (j = 0; j < maxCount; j++) {
                cSvcManager.addToPersistence(String.format("dir_%d/node_%d", i, j), data);
            }
        }
        watch.stop();
        System.out.println(String.format("CSvcDBStoreDemo CREATEs complete [execution_time_ms=%s," +
                        " node_count=%d, bytes_per_node=%d]",
                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1),
                bytesPerNode));
    }

    public static void testCSvcSet(CoordinationSvcManager cSvcManager, int maxCount,
                                   byte[] data) throws InvalidServiceStateException {
        int i;
        int j;
        int bytesPerNode = data.length;
//        int maxCount = 20;
//        int maxCount = 2;
        StopWatch watch = new StopWatch();
        watch.reset();
        watch.start();
        for (i = 0; i < maxCount; i++) {
            cSvcManager.updatePersistence(String.format("dir_%d", i), data);
            for (j = 0; j < maxCount; j++) {
                cSvcManager.updatePersistence(String.format("dir_%d/node_%d", i, j), data);
            }
        }
        watch.stop();
        System.out.println(String.format("CSvcDBStoreDemo SETs complete [execution_time_ms=%s," +
                        " node_count=%d, bytes_per_node=%d]",
                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1),
                bytesPerNode));
    }

    public static void testCSvcDelete(CoordinationSvcManager cSvcManager,
                                      int maxCount) throws InvalidServiceStateException {
        int i;
        int j;
//        int maxCount = 20;
//        int maxCount = 2;
        StopWatch watch = new StopWatch();
        watch.start();
        for (i = 0; i < maxCount; i++) {
            for (j = 0; j < maxCount; j++) {
                cSvcManager.removeFromPersistence(String.format("dir_%d/node_%d", i, j));
            }
            cSvcManager.removeFromPersistence(String.format("dir_%d", i));
        }
        watch.stop();
        System.out.println(String.format("CSvcDBStoreDemo DELETEs complete [execution_time_ms=%s, node_count=%d]",
                watch.getTime(TimeUnit.MILLISECONDS), maxCount*(maxCount+1)));
    }

    public static WatchConsumerWrapper<WatchInput> generateGenericLSCallable() {
        BiConsumer<DataLayerMgrBase, WatchInput> callable = (manager, commandOutput) -> {
//            if (!owner.isRunning()) {
//                waitOnOwnerInit();
//            }

            try {
                log.info("REMOVEME - generateGenericLSCallable triggered [path={}]", commandOutput.getPath());
                CommandOutput output = LsCommand.generateLsCommand(
                                commandOutput.getPath(), false, false, false,
                                null, false, true, true)
                        .executeManager(manager);
                log.info("current size: {}", output.getChildrenSet().size());
            } catch (Exception e) {
                log.error("REMOVEME - generateGenericLSCallable - Exception!", e);
            }
        };
        return new WatchConsumerWrapper<>(callable, true, WatchedEvent.WatchType.CHILDREN);
    }
}
