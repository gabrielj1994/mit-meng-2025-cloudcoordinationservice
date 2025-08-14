package dsg.ccsvc.watch;

public class WatchWrapperBase {
    String path;
    WatchWrapperBase chainedWatcher;

    public WatchWrapperBase(String path, WatchWrapperBase chainedWatcher) {
        this.path = path;
        this.chainedWatcher = chainedWatcher;
    }

    public void process(WatchedEvent event) {
        if (!this.path.equals(event.getPath())) {
            System.out.println(String.format(
                    "FATAL ERROR - watcher and event path mismatch [path=%s, event.path=%s]",
                    this.path, event.getPath()));
        }
        System.out.println(String.format("Unimplemented base watcher [path=%s]", this.path));
    }


}
