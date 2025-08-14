package dsg.ccsvc.datalayer;

import dsg.ccsvc.log.LogRecord;
import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Builder
@Getter
public class FSEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private LogRecord record;
    private long entryTimestamp;
}
