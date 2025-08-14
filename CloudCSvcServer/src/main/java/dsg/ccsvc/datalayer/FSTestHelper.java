package dsg.ccsvc.datalayer;

import dsg.ccsvc.datalayer.DataLayerMgrSharedFS;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

// TODO: Remove once refactored into TEST
public class FSTestHelper {
    public static void tbdFSCopyLogToTemp(DataLayerMgrSharedFS manager) throws IOException {
        Files.move(manager.logPath, manager.logTempPath, StandardCopyOption.ATOMIC_MOVE);
    }
}
