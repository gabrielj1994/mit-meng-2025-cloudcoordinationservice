package org.example;

import dsg.ccsvc.datalayer.FSEntry;
import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.command.CommandOutput;
import dsg.ccsvc.command.DeleteCommand;
import dsg.ccsvc.command.LsCommand;
import dsg.ccsvc.command.WatchInput;
import dsg.ccsvc.datalayer.DataLayerMgrBase;
import dsg.ccsvc.log.LogRecordFSHeader;
import dsg.ccsvc.watch.WatchConsumerWrapper;
import dsg.ccsvc.watch.WatchedEvent;
import dsg.ccsvc.util.DebugConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.example.manager.CoordinationSvcManager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import static dsg.ccsvc.client.CSvcClient.ManagerType.DB_VARIANT;
import static dsg.ccsvc.client.CSvcClient.ManagerType.FS_VARIANT;
import static dsg.ccsvc.log.LogRecordFSHeader.HEADER_SIZE;
import static org.example.util.ZKConfigs.PERSISTENCE_NODE;
import static org.example.util.ZKConfigs.TEMP_NODE;

@Slf4j
public class CSvcFSStoreDemo {

    private static Options options = new Options();

    static {
        options.addOption(new Option("df", false, "debug flag"));
        options.addOption(new Option("lf", false, "log flag"));
        options.addOption(new Option("mf", false, "metric flag"));
        options.addOption(new Option("mnc", true, "max node count"));
        options.addOption(new Option("bpn", true, "bytes per node"));
        options.addOption(new Option("ltm", true, "lease threshold multiplier"));
    }

    public static void main(String[] args) {
        int flag = Integer.parseInt(args[0]);
        DefaultParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(String.format("CSvcFSStoreDemo - Failed command line options parsing"));
            throw new RuntimeException(e);
        }

        int maxNodeCount = cl.hasOption("mnc") ? Integer.parseInt(cl.getOptionValue("mnc")) : 20;
        DebugConstants.LOGREADER_FLAG = cl.hasOption("lf") || DebugConstants.LOGREADER_FLAG;
        DebugConstants.DEBUG_FLAG = cl.hasOption("df") || DebugConstants.DEBUG_FLAG;
        DebugConstants.METRICS_FLAG = cl.hasOption("mf") || DebugConstants.METRICS_FLAG;
        int bytesPerNode = cl.hasOption("bpn") ? Integer.parseInt(cl.getOptionValue("bpn")) : 1000;
        int leaseThresholdMultiplier = cl.hasOption("ltm") ? Integer.parseInt(cl.getOptionValue("ltm")) : 1;

        if (DebugConstants.DEBUG_FLAG) {
            log.info("CSvcFSStoreDemo - DEBUG FLAG ON");
            Configurator.setRootLevel(Level.DEBUG);
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
    //            System.out.println(String.format("ZKStoreDemo CREATEs complete [execution_time_ms=%s," +
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
    //            System.out.println(String.format("ZKStoreDemo SETs complete [execution_time_ms=%s," +
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
    //            System.out.println(String.format("ZKStoreDemo DELETEs complete [execution_time_ms=%s, node_count=%d]",
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
                System.out.println(String.format("DEBUG - CSvcFSStoreDemo - DEBUG LOGGING ENABLED"));
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
                DeleteCommand.generateDeleteCommand(PERSISTENCE_NODE).executeManager(cSvcManager.getManager());
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
                // TODO: Consider decoupling start manager
                cSvcManager.getManager().setLeaseMultiplier(leaseThresholdMultiplier);
                cSvcManager.start();

                try {
                    cSvcManager.createTempParent();
                } catch (InvalidServiceStateException e) {
                    // TODO: No-op for already exists.
                    //   need logic to handle other root causes
                }

                cSvcManager.registerChildrenChangeWatcher(TEMP_NODE,
                        generateGenericLSCallable());

                while (cSvcManager.isRunning()) {
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
            // NOTE: Long running "restart" node
            CoordinationSvcManager cSvcManager = null;
            int count = 0;
            while (true) {
                try {
                    cSvcManager = new CoordinationSvcManager(FS_VARIANT);
                    cSvcManager.getManager().setLeaseMultiplier(leaseThresholdMultiplier);
                    cSvcManager.start();

//                    try {
////                        cSvcManager.createTempParent();
//                    } catch (InvalidServiceStateException e) {
//                        // NOTE: No-op
//                    }
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
        } else if (flag == 8) {
            String testPath = args[1];
            testFSDelayWriter(Paths.get(testPath));
        } else if (flag == 9) {
            String testPath = args[1];
            testFSDelayReader(Paths.get(testPath));
        } else if (flag == 10) {
            String testPath = args[1];
            testFSLocalWriteRead(Paths.get(testPath));
        } else if (flag == 11) {
            String testPath = args[1];
            testFSMetadataChanges(Paths.get(testPath));
        } else if (flag == 12) {
            String testPath = args[1];
            try {
                testFSLocalWriteReadByteMap(Paths.get(testPath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 13) {
            String testPath = args[1];
            testFSDelayWriteByteMap(Paths.get(testPath));
        } else if (flag == 14) {
            String testPath = args[1];
            try {
                testFSDelayReadByteMap(Paths.get(testPath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 15) {
            String testPath = args[1];
            try {
                testFSReadPath(Paths.get(testPath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 16) {
            String testPath = args[1];
            try {
                testFSDelayReadFileChannel(Paths.get(testPath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 17) {
            String testPath = args[1];
            testFSDelayWriteFileChannel(Paths.get(testPath));
        } else if (flag == 18) {
            String testPath = args[1];
            int offset = Integer.parseInt(args[2]);
            String outputLeftPath = args[3];
            String outputRightPath = args[4];
            try {
                testFSSplitFile(Paths.get(testPath), offset, Paths.get(outputLeftPath), Paths.get(outputRightPath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 19) {
            String testPath = args[1];
            try {
                testFSConsumeEntries(Paths.get(testPath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 20) {
            testParkNanos();
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

    private static void testParkNanos() {
        int counter = 0;
        ArrayList<Long> timestamps = new ArrayList<>();
        while (counter < 20) {
            counter++;
            LockSupport.parkNanos(1000000);
            timestamps.add(System.currentTimeMillis());
        }
        Long previous = -1L;
        for (Long entry : timestamps) {
            log.info(String.format("testParkNanos - Timestamp entry [entry=%d]", entry));
            if (previous > 0) {
                log.info(String.format("testParkNanos - Timestamp delta [delta=%d]", entry-previous));
            }
            previous = entry;
        }
    }

    private static void testFSConsumeEntries(Path testFile) throws IOException {
        try (RandomAccessFile ignored = new RandomAccessFile(testFile.toFile(), "r");
             FileChannel fileChannel = ignored.getChannel()) {
//            long currentReadOffset = 0;
            ByteBuffer hdrBuffer = ByteBuffer.allocate(HEADER_SIZE);
            ByteBuffer buffer;
            long headerPosition;
            int txId = -1;
            while (fileChannel.position() + HEADER_SIZE < fileChannel.size()) {
                headerPosition = fileChannel.position();
                if (fileChannel.read(hdrBuffer) <= 0) {
                    // NOTE: Restart
                    if (DebugConstants.DEBUG_FLAG) {
                        log.warn("DEBUG - tbdConsumeEntries - Failed to read record header");
                    }
                    throw new RuntimeException("tbdConsumeEntries " +
                            "- Failed to read record header");
                }

                LogRecordFSHeader header = SerializationUtils.deserialize(hdrBuffer.array());
                if (txId != -1 && txId+1 != header.getTransactionId()) {
                    // NOTE: Restart
//                                if (DebugConstants.DEBUG_FLAG) {
                    log.warn(String.format("tbdConsumeEntries - Missing expected transaction ID" +
                                    " [expected_txid=%d, header_txid=%d, log_offset=%d]",
                            txId+1, header.getTransactionId(), headerPosition));
                    log.warn("===\ntbdConsumeEntries - CONTINUING FOR TEST\n===");
//                                }
//                    throw new RuntimeException(String.format("tbdConsumeEntries -" +
//                                    " Missing expected transaction ID [expected_txid=%d, header_txid=%d]",
//                            txId+1, header.getTransactionId()));
                }
                txId = header.getTransactionId();
                hdrBuffer.clear();
                buffer = ByteBuffer.allocate(header.getLogEntryByteSize());

                if (fileChannel.read(buffer) <= 0) {
                    // NOTE: Restart
                    if (DebugConstants.DEBUG_FLAG) {
                        log.warn("tbdConsumeEntries - Failed to read from log read path");
                    }
                    throw new RuntimeException("tbdConsumeEntries -" +
                            " Failed to read from log read path");
                }
                FSEntry entry = SerializationUtils.deserialize(buffer.array());
                //private int transactionId;
                //    private int logEntryByteSize;
                log.info(String.format("testFSConsumeEntries - [log_record_txid=%d, log_record_size=%d]",
                        header.getTransactionId(), header.getLogEntryByteSize()));
                log.info(String.format("testFSConsumeEntries - [log_record=%s]", entry.getRecord()));
                buffer.clear();
            }
        }
    }

    private static void testFSSplitFile(Path testFile, int offset, Path outputLeftPath, Path outputRightPath)
            throws IOException {
        // Open input channel
        try (FileChannel inputChannel = FileChannel.open(testFile, StandardOpenOption.READ);
             FileChannel outputChannel1 = FileChannel.open(outputLeftPath, StandardOpenOption.CREATE,
                     StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel outputChannel2 = FileChannel.open(outputRightPath, StandardOpenOption.CREATE,
                     StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            long fileSize = inputChannel.size();
            if (offset < 0 || offset > fileSize) {
                throw new IllegalArgumentException("Offset is out of bounds: " + offset);
            }
            // Copy part 1 (0 to offset)
            inputChannel.transferTo(0, offset, outputChannel1);
            // Copy part 2 (offset to end)
            inputChannel.transferTo(offset, fileSize - offset, outputChannel2);
        }
    }

    private static void testFSReadPath(Path testFile) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(testFile, StandardOpenOption.READ)) {
            MappedByteBuffer mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 8,
                    fileChannel.size()-8);
//            mappedBuffer.position(8);
            byte[] content = new byte[mappedBuffer.remaining()];
            mappedBuffer.get(content);
            log.info(String.format("testFSReadPath - Content" +
                            " [file_path=%s, content=%d]",
                    testFile, ByteBuffer.wrap(content).getLong()));
        }

        try (FileChannel fileChannel = FileChannel.open(testFile, StandardOpenOption.READ)) {
            MappedByteBuffer mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Long.BYTES);
            log.info(String.format("testFSReadPath - Timestamp" +
                            " [file_path=%s, timestamp=%d]",
                    testFile, mappedBuffer.getLong(0)));
        }
    }

    private static void testFSMetadataChanges(Path testFile) {
        int count = 0;
        int maxCount = 20;
        int failureThreshold = 5;
        int consecutiveFailureCounter = 0;
        long processTimestamp;
        long fileTimestamp;
        long timeDelta;
        ArrayList<ArrayList<Long>> resultLog = new ArrayList<>();
        while (count < maxCount && consecutiveFailureCounter < failureThreshold) {
            try {
                Files.setLastModifiedTime(testFile, FileTime.fromMillis(System.currentTimeMillis()));
                processTimestamp = System.currentTimeMillis();
                fileTimestamp = Files.getLastModifiedTime(testFile).toMillis();
                timeDelta = Math.abs(processTimestamp - fileTimestamp);
                log.info(String.format("testFSLocalWriteRead - New timestamp" +
                                " [process_timestamp=%d, file_timestamp=%d, delta=%d]",
                        processTimestamp, fileTimestamp, timeDelta));
                resultLog.add(new ArrayList<>(Arrays.asList(processTimestamp, fileTimestamp, timeDelta)));
            } catch (IOException e) {
                consecutiveFailureCounter++;
                continue;
            }
            count++;
            consecutiveFailureCounter = 0;
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("FINISHED TEST - PRINTING TIMESTAMPS");
        for (ArrayList<Long> entry : resultLog) {
            log.info(String.format("testFSLocalWriteRead - Timestamp entry [entry=%s]", entry.toString()));
        }
    }

    private static void testFSDelayReadFileChannel(Path testFile) throws IOException {
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
                final int RECORD_SIZE = Long.BYTES * 2; // 8 bytes owner ID + 8 bytes timestamp
                try (FileChannel fileChannel = FileChannel.open(testFile, StandardOpenOption.READ)) {
//                try (SeekableByteChannel fileChannel = Files.newByteChannel(testFile, StandardOpenOption.READ)) {
//                    mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, RECORD_SIZE);
//                    fileTimestamp = getTimestamp(mappedBuffer);
                    fileChannel.position(Long.BYTES);
                    fileChannel.read(buffer);
                    buffer.flip();
                    fileTimestamp = buffer.getLong();
                    buffer.clear();
                }
            } catch (IndexOutOfBoundsException | IOException e) {
                // NOTE: IOException from unexpected file size
                //       NoSuchFileException from missing file
                continue;
            }
            if (fileTimestamp != prevFiletimestamp) {
                count++;
                prevFiletimestamp = fileTimestamp;
                timeDelta = Math.abs(processTimestamp - prevFiletimestamp);
                log.info(String.format("testFSDelayReader - New timestamp" +
                                " [local_timestamp=%d, remote_timestamp=%d, delta=%d]",
                        processTimestamp, prevFiletimestamp, timeDelta));
                resultLog.add(new ArrayList<>(Arrays.asList(processTimestamp, prevFiletimestamp, timeDelta)));
            }
//            }
//            consecutiveFailureCounter = 0;
        }
        log.info("FINISHED TEST - PRINTING TIMESTAMPS");
        for (ArrayList<Long> entry : resultLog) {
            log.info(String.format("testFSDelayReader - Timestamp entry [entry=%s]", entry.toString()));
        }
    }

    private static void testFSDelayReadByteMap(Path testFile) throws IOException {
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
                final int RECORD_SIZE = Long.BYTES * 2; // 8 bytes owner ID + 8 bytes timestamp
                try (FileChannel fileChannel = FileChannel.open(testFile, StandardOpenOption.READ)) {
                    mappedBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, RECORD_SIZE);
                    fileTimestamp = getTimestamp(mappedBuffer);
//                    fileChannel.read(buffer, Long.BYTES);
//                    buffer.flip();
//                    fileTimestamp = buffer.getLong();
                }
            } catch (IndexOutOfBoundsException | IOException e) {
                // NOTE: IOException from unexpected file size
                //       NoSuchFileException from missing file
                continue;
            }
            if (fileTimestamp != prevFiletimestamp) {
                count++;
                prevFiletimestamp = fileTimestamp;
                timeDelta = Math.abs(processTimestamp - prevFiletimestamp);
                log.info(String.format("testFSDelayReader - New timestamp" +
                                " [local_timestamp=%d, remote_timestamp=%d, delta=%d]",
                        processTimestamp, prevFiletimestamp, timeDelta));
                resultLog.add(new ArrayList<>(Arrays.asList(processTimestamp, prevFiletimestamp, timeDelta)));
            }
//            }
//            consecutiveFailureCounter = 0;
        }
        log.info("FINISHED TEST - PRINTING TIMESTAMPS");
        for (ArrayList<Long> entry : resultLog) {
            log.info(String.format("testFSDelayReader - Timestamp entry [entry=%s]", entry.toString()));
        }
    }

    private static void testFSDelayWriteByteMap(Path testFile) {
        int count = 0;
        int maxCount = 20;
        int failureThreshold = 5;
        int consecutiveFailureCounter = 0;
        long ownerId = 12345L;
        MappedByteBuffer[] mappedBuffer = new MappedByteBuffer[]{null};
        while (count < maxCount && consecutiveFailureCounter < failureThreshold) {
            try {
                writeOwnerId(mappedBuffer, testFile, ownerId);
            } catch (IOException e) {
                consecutiveFailureCounter++;
                continue;
            }
            count++;
            consecutiveFailureCounter = 0;
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void testFSDelayWriteFileChannel(Path testFile) {
        int count = 0;
        int maxCount = 20;
        int failureThreshold = 5;
        int consecutiveFailureCounter = 0;
        long ownerId = 12345L;
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES*2);
        while (count < maxCount && consecutiveFailureCounter < failureThreshold) {
            try {
                try (FileChannel fileChannel = FileChannel.open(testFile, StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE)) {
                    buffer.putLong(ownerId);
                    buffer.putLong(System.currentTimeMillis());
                    buffer.flip();
                    fileChannel.write(buffer);
                    buffer.clear();
                }
            } catch (IOException e) {
                consecutiveFailureCounter++;
                continue;
            }
            count++;
            consecutiveFailureCounter = 0;
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void testFSLocalWriteReadByteMap(Path testFile)
            throws IOException {
        int count = 0;
        int maxCount = 20;
        int failureThreshold = 5;
        int consecutiveFailureCounter = 0;
        long processTimestamp;
        long fileTimestamp;
        long timeDelta;
        long ownerId = 12345L;
        ArrayList<ArrayList<Long>> resultLog = new ArrayList<>();
        MappedByteBuffer[] mappedBuffer = new MappedByteBuffer[]{null};
        while (count < maxCount && consecutiveFailureCounter < failureThreshold) {
            try {
                writeOwnerId(mappedBuffer, testFile, ownerId);
                processTimestamp = System.currentTimeMillis();
                fileTimestamp = getTimestamp(mappedBuffer[0]);
                timeDelta = Math.abs(processTimestamp - fileTimestamp);
                log.info(String.format("testFSLocalWriteRead - New timestamp" +
                                " [process_timestamp=%d, file_timestamp=%d, delta=%d]",
                        processTimestamp, fileTimestamp, timeDelta));
                resultLog.add(new ArrayList<>(Arrays.asList(processTimestamp, fileTimestamp, timeDelta)));
            } catch (IOException e) {
                consecutiveFailureCounter++;
                continue;
            }
            count++;
            consecutiveFailureCounter = 0;
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("FINISHED TEST - PRINTING TIMESTAMPS");
        for (ArrayList<Long> entry : resultLog) {
            log.info(String.format("testFSLocalWriteRead - Timestamp entry [entry=%s]", entry.toString()));
        }
    }

    private static MappedByteBuffer retrieveMBBuffer(Path filePath) throws IOException {
        final int RECORD_SIZE = Long.BYTES * 2; // 8 bytes owner ID + 8 bytes timestamp
        try (FileChannel fileChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            return fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, RECORD_SIZE);
        }
    }

    private static void writeOwnerId(MappedByteBuffer[] mappedBuffer, Path filePath, long ownerId) throws IOException {
        final int RECORD_SIZE = Long.BYTES * 2; // 8 bytes owner ID + 8 bytes timestamp
        try (FileChannel fileChannel = FileChannel.open(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            mappedBuffer[0] = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, RECORD_SIZE);
            writeOwnerId(mappedBuffer[0], ownerId);
            updateTimestamp(mappedBuffer[0]);
        }
    }

    private static void updateTimestamp(MappedByteBuffer mappedBuffer) {
        long timestamp = System.currentTimeMillis();
        mappedBuffer.putLong(Long.BYTES, timestamp); // Update only the timestamp (offset 8)
        mappedBuffer.force(); // Ensure it's written to disk
    }

    private static long getOwnerId(MappedByteBuffer mappedBuffer) {
        return mappedBuffer.getLong(0);
    }

    private static long getTimestamp(MappedByteBuffer mappedBuffer) {
        return mappedBuffer.getLong(Long.BYTES);
    }

    private static void writeOwnerId(MappedByteBuffer mappedBuffer, long ownerId) {
        mappedBuffer.putLong(0, ownerId);
        mappedBuffer.force();
    }

    private static void testFSLocalWriteRead(Path testFile) {
        int count = 0;
        int maxCount = 20;
        int failureThreshold = 5;
        int consecutiveFailureCounter = 0;
        long processTimestamp;
        long fileTimestamp;
        long timeDelta;
        ArrayList<ArrayList<Long>> resultLog = new ArrayList<>();
        ByteBuffer buffer;
        buffer = ByteBuffer.allocate(Long.BYTES);
        while (count < maxCount && consecutiveFailureCounter < failureThreshold) {
            buffer.clear();
            buffer.putLong(System.currentTimeMillis());
            try {
                Files.write(testFile, buffer.array(), StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
                processTimestamp = System.currentTimeMillis();
                buffer.clear();
                buffer.put(Files.readAllBytes(testFile));
                buffer.flip();
                fileTimestamp = buffer.getLong();
                timeDelta = Math.abs(processTimestamp - fileTimestamp);
                log.info(String.format("testFSLocalWriteRead - New timestamp" +
                                " [process_timestamp=%d, file_timestamp=%d, delta=%d]",
                        processTimestamp, fileTimestamp, timeDelta));
                resultLog.add(new ArrayList<>(Arrays.asList(processTimestamp, fileTimestamp, timeDelta)));
            } catch (IOException e) {
                consecutiveFailureCounter++;
                continue;
            }
            count++;
            consecutiveFailureCounter = 0;
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        log.info("FINISHED TEST - PRINTING TIMESTAMPS");
        for (ArrayList<Long> entry : resultLog) {
            log.info(String.format("testFSLocalWriteRead - Timestamp entry [entry=%s]", entry.toString()));
        }
    }

    private static void testFSDelayWriter(Path testFile) {
        int count = 0;
        int maxCount = 20;
        int failureThreshold = 5;
        int consecutiveFailureCounter = 0;
        ByteBuffer buffer;
        buffer = ByteBuffer.allocate(Long.BYTES*2);
        while (count < maxCount && consecutiveFailureCounter < failureThreshold) {
            buffer.clear();
            buffer.putLong(12345L);
            buffer.putLong(System.currentTimeMillis());
            try {
                Files.write(testFile, buffer.array(), StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.SYNC);
//                log.info("testFSDelayWriter - New timestamp written [file_timestamp=%d]");
            } catch (IOException e) {
                consecutiveFailureCounter++;
                continue;
//                throw new RuntimeException(e);
            }
            count++;
            consecutiveFailureCounter = 0;
            try {
                Thread.sleep(15);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void testFSDelayReader(Path testFile) {
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
//        buffer = ByteBuffer.allocate(Long.BYTES);
        while (count < maxCount && consecutiveFailureCounter < failureThreshold) {
            try {
                processTimestamp = System.currentTimeMillis();
                buffer = ByteBuffer.wrap(Files.readAllBytes(testFile));
                if (buffer.array().length < Long.BYTES) {
                    continue;
                }
                fileTimestamp = buffer.getLong(Long.BYTES);
                if (fileTimestamp != prevFiletimestamp) {
                    count++;
                    prevFiletimestamp = fileTimestamp;
                    timeDelta = Math.abs(processTimestamp - prevFiletimestamp);
                    log.info(String.format("testFSDelayReader - New timestamp" +
                                    " [local_timestamp=%d, remote_timestamp=%d, delta=%d]",
                            processTimestamp, prevFiletimestamp, timeDelta));
                    resultLog.add(new ArrayList<>(Arrays.asList(processTimestamp, prevFiletimestamp, timeDelta)));
                }
            } catch (IOException e) {
                consecutiveFailureCounter++;
                continue;
//                throw new RuntimeException(e);
            }
            consecutiveFailureCounter = 0;
        }
        log.info("FINISHED TEST - PRINTING TIMESTAMPS");
        for (ArrayList<Long> entry : resultLog) {
            log.info(String.format("testFSDelayReader - Timestamp entry [entry=%s]", entry.toString()));
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
        System.out.println(String.format("ZKStoreDemo CREATEs complete [execution_time_ms=%s," +
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
        System.out.println(String.format("ZKStoreDemo SETs complete [execution_time_ms=%s," +
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
        System.out.println(String.format("ZKStoreDemo DELETEs complete [execution_time_ms=%s, node_count=%d]",
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
                if (Objects.isNull(output) || Objects.isNull(output.getChildrenSet())) {
                    log.warn("generateGenericLSCallable - null output, missing node");
                } else {
                    log.info("current size: {}", output.getChildrenSet().size());
                }
            } catch (Exception e) {
                log.error("generateGenericLSCallable - Unexpected Exception", e);
            }
        };
        return new WatchConsumerWrapper<>(callable, true, WatchedEvent.WatchType.CHILDREN);
    }
}
