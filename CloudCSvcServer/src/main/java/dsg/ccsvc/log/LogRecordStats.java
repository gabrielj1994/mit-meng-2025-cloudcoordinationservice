package dsg.ccsvc.log;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;

public class LogRecordStats implements Serializable {

    private static final long serialVersionUID = 1L;

    /*
    cZxid = 0x2
    ctime = Sat Aug 31 17:39:41 UTC 2024
    mZxid = 0x2
    mtime = Sat Aug 31 17:39:41 UTC 2024
    pZxid = 0x2
    cversion = 0
    dataVersion = 0
    aclVersion = 0
    ephemeralOwner = 0x0
    dataLength = 0
    numChildren = 0
     */
    private int dataLength;
    private final int creationTransactionId;

    private final long creationTimestamp;

    private int modifiedTransactionId;

    private long modifiedTimestamp;

    // pZxid: The transaction ID that last modified the children list of the znode.
    private int childrenTransactionId;

    private int version;
    // cversion: Version 3 ZK - The number of changes to the children list of the znode.
    private int childListVersion;

    // ZK Sequential only tracks created children
    private int childSequentialVersion;
    private int childCount;

    private final long ephemeralOwnerId;

    public LogRecordStats(int creationTransactionId, int modifiedTransactionId, int childrenTransactionId,
                          long creationTimestamp, long modifiedTimestamp, int version, int childListVersion,
                          int childSequentialVersion, int childCount, int dataLength, long ephemeralOwnerId) {
        this.creationTransactionId = creationTransactionId;
        this.modifiedTransactionId = modifiedTransactionId;
        this.childrenTransactionId = childrenTransactionId;
        this.creationTimestamp = creationTimestamp;
        this.modifiedTimestamp = modifiedTimestamp;
        this.version = version;
        this.childListVersion = childListVersion;
        this.ephemeralOwnerId = ephemeralOwnerId;
        this.childSequentialVersion = childSequentialVersion;
        this.childCount = childCount;
        this.dataLength = dataLength;
    }

//    public LogRecordStats(LogRecordStats previousStats, int modifiedTransactionId, int childrenTransactionId, )

    public void setModifiedTimestamp(long modifiedTimestamp) {
        this.modifiedTimestamp = modifiedTimestamp;
    }

    public void setModifiedTransactionId(int modifiedTransactionId) {
        this.modifiedTransactionId = modifiedTransactionId;
    }

    public void setChildrenListMutationTransactionId(int modifiedTransactionId) {
        this.modifiedTransactionId = modifiedTransactionId;
    }

    public void incrementVersion() {
        this.version += 1;
    }

    public void incrementChildrenVersion() {
        this.childListVersion += 1;
    }

    public void incrementChildCount() {
        this.childSequentialVersion += 1;
        this.childCount += 1;
    }

    public void decrementChildCount() {
        this.childCount -= 1;
    }

    public int getCreationTransactionId() {
        return creationTransactionId;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public int getModifiedTransactionId() {
        return modifiedTransactionId;
    }

    public long getModifiedTimestamp() {
        return modifiedTimestamp;
    }

    public int getChildrenTransactionId() {
        return childrenTransactionId;
    }

    public int getVersion() {
        return version;
    }

    public int getChildListVersion() {
        return childListVersion;
    }

    public int getChildSequentialVersion() {
        return childSequentialVersion;
    }

    public int getChildCount() {
        return childCount;
    }

    public long getEphemeralOwnerId() {
        return ephemeralOwnerId;
    }

    public int getDataLength() {
        return dataLength;
    }

    public void setDataLength(int dataLength) {
        this.dataLength = dataLength;
    }

    public LogRecordStats clone() {
        return new LogRecordStats(creationTransactionId, modifiedTransactionId, childrenTransactionId,
                creationTimestamp, modifiedTimestamp, version, childListVersion, childSequentialVersion,
                childCount, dataLength, ephemeralOwnerId);
    }
    @Override
    public String toString() {
        /*
    cZxid = 0x2
    ctime = Sat Aug 31 17:39:41 UTC 2024
    mZxid = 0x2
    mtime = Sat Aug 31 17:39:41 UTC 2024
    pZxid = 0x2
    cversion = 0
    dataVersion = 0
    aclVersion = 0
    ephemeralOwner = 0x0
    dataLength = 0
    numChildren = 0
     */
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("creationTxId:\n\t").append(creationTransactionId).append("\n");
        stringBuilder.append("creationTimestamp:\n\t").append(Timestamp.from(
                                                        Instant.ofEpochMilli(creationTimestamp))).append("\n");
        stringBuilder.append("mutationTxId:\n\t").append(modifiedTransactionId).append("\n");
        stringBuilder.append("mutationTs:\n\t").append(Timestamp.from(
                                                    Instant.ofEpochMilli(modifiedTimestamp))).append("\n");
        stringBuilder.append("dataVersion:\n\t").append(version).append("\n");
        stringBuilder.append("childListVersion:\n\t").append(childListVersion).append("\n");
        stringBuilder.append("ephemeralOwnerId:\n\t").append(ephemeralOwnerId).append("\n");
        stringBuilder.append("dataLength:\n\t").append(dataLength).append("\n");
        stringBuilder.append("numChildren:\n\t").append(childCount).append("\n");
        return stringBuilder.toString();
    }
}
