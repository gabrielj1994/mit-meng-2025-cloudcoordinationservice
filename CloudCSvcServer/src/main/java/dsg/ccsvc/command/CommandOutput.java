package dsg.ccsvc.command;

import dsg.ccsvc.log.LogRecord;
import dsg.ccsvc.log.LogRecordStats;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Set;

// TODO: Consider decoupling watchoutput and commandoutput
public class CommandOutput {

    private String output;
    // TODO: flesh out error codes; currently 0 -> no errors, 1 -> errors
    private int errorCode;
    // TODO: consider only having output
    private String errorMsg;

    // TODO: Consider proper design for outputs
    private String path;

    private byte[] data;

    private int recordVersion;

//    private int triggerTransactionId;

    private LogRecord.LogRecordType triggerRecordType;

    private LogRecordStats recordStats;

    // TODO: Consider how to properly design commandoutput. Consider replicating GetResponse from http
    private Set<String> childrenList;

    private boolean existFlag;

//    public int getTriggerTransactionId() {
//        return triggerTransactionId;
//    }

//    public void setTriggerTransactionId(int triggerTransactionId) {
//        this.triggerTransactionId = triggerTransactionId;
//    }

    public LogRecord.LogRecordType getTriggerRecordType() {
        return triggerRecordType;
    }

    public void setTriggerRecordType(LogRecord.LogRecordType triggerRecordType) {
        this.triggerRecordType = triggerRecordType;
    }

    public int getRecordVersion() {
        return recordVersion;
    }

    public void setRecordVersion(int recordVersion) {
        this.recordVersion = recordVersion;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public byte[] getData() {
        return Objects.isNull(data) ? "null".getBytes(StandardCharsets.UTF_8) : data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public Set<String> getChildrenSet() {
        return childrenList;
    }

    public void setChildrenSet(Set<String> childrenList) {
        this.childrenList = childrenList;
    }

    public boolean getExistFlag() {
        return existFlag;
    }

    public void setExistFlag(boolean existFlag) {
        this.existFlag = existFlag;
    }


    public CommandOutput() {
        this.output = "";
        // NOTE: -1 in-progress; 0 success; 1 error
        this.errorCode = -1;
        this.errorMsg = "";
        this.recordVersion = -1;
        this.existFlag = false;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        // TODO: consider refactor for easier to understand error code flow
        this.errorCode = 0;
        this.output = output;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public void populateError(int errorCode, String errorMsg) {
        this.errorCode = errorCode;
        this.errorMsg = errorMsg;
    }

    public LogRecordStats getRecordStats() {
        return recordStats;
    }

    public void setRecordStats(LogRecordStats recordStats) {
        this.recordStats = recordStats;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("output:\n\t").append(output).append("\n");
        stringBuilder.append("errorCode:\n\t").append(errorCode).append("\n");
        stringBuilder.append("errorMsg:\n\t").append(errorMsg).append("\n");
        stringBuilder.append("path:\n\t").append(path).append("\n");
        stringBuilder.append("data:\n\t").append(data).append("\n");
        stringBuilder.append("recordVersion:\n\t").append(recordVersion).append("\n");
//        stringBuilder.append("triggerTransactionId:\n\t").append(triggerTransactionId).append("\n");
        stringBuilder.append("triggerRecordType:\n\t").append(triggerRecordType).append("\n");
        return stringBuilder.toString();
    }
}
