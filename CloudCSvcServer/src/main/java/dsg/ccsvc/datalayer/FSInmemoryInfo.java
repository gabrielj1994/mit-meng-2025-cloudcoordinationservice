package dsg.ccsvc.datalayer;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class FSInmemoryInfo {
    private int transactionId;
    private int logSegment;
    private long headerStartOffset;
}
