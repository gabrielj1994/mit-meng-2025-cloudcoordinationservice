package dsg.ccsvc.datalayer;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class FSReadWaitWrapper {
    final Object monitorObject;
    int tbdLogSegmentThreshold;
    long tbdLogSizeThreshold;

    @Override
    public String toString() {
        return String.format("[logSegmentThreshold=%d, logSizeThreshold=%d]",
                                tbdLogSegmentThreshold, tbdLogSizeThreshold);
    }
}
