package dsg.ccsvc.datalayer;

public abstract class ManagerLeaseHandlerBase implements AutoCloseable {
    // TODO: Consider what can be in the base class
    public abstract void close() throws InterruptedException;
}
