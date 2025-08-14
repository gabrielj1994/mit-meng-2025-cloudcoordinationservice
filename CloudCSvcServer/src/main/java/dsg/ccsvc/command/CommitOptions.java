package dsg.ccsvc.command;

import lombok.Builder;

//@Getter
@Builder
public class CommitOptions {
    @Builder.Default
    int expectedVersion = -1;
    @Builder.Default
    boolean validateVersionFlag = false;
    @Builder.Default
    boolean sequentialFlag = false;
    @Builder.Default
    boolean dryRun = false;

    public int getExpectedVersion() {
        return expectedVersion;
    }

    public boolean getValidateVersionFlag() {
        return validateVersionFlag;
    }

    public boolean getSequentialFlag() {
        return sequentialFlag;
    }

    public boolean isDryRun() {
        return dryRun;
    }
}
