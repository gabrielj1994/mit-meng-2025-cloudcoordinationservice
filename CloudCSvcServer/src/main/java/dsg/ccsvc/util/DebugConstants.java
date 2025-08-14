package dsg.ccsvc.util;

public class DebugConstants {

    public static boolean METRICS_FLAG = false;
    public static boolean DEBUG_FLAG = false;
    public static boolean TESTS_DEBUG_FLAG = false;
    public static boolean LOGREADER_FLAG = false;

    public enum DBState {
        LOCK_NOT_AVAILABLE("55P03");
        private final String state;

        DBState(String state) {
            this.state = state;
        }

        public String getState() {
            return this.state;
        }

    }
}
