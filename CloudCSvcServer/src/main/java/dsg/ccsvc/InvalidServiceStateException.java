package dsg.ccsvc;

public class InvalidServiceStateException extends Exception {

    public InvalidServiceStateException(String message) {
        super(message);
    }

    public InvalidServiceStateException(Throwable cause) {
        super(cause);
    }
    
    public InvalidServiceStateException(String message, Throwable cause) {
        super(message, cause);
    }

}
