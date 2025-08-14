package dsg.ccsvc.command;

import dsg.ccsvc.log.LogRecord;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Getter
@Setter
public class WatchInput {

    private String path;
    private int recordVersion;
    private int triggerTransactionId;
    private LogRecord.LogRecordType triggerRecordType;

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("path:\n\t").append(path).append("\n");
        stringBuilder.append("recordVersion:\n\t").append(recordVersion).append("\n");
        stringBuilder.append("triggerTransactionId:\n\t").append(triggerTransactionId).append("\n");
        stringBuilder.append("triggerRecordType:\n\t").append(triggerRecordType).append("\n");
        return stringBuilder.toString();
    }

}
