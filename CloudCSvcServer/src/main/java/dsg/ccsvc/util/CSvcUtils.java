package dsg.ccsvc.util;

import dsg.ccsvc.InvalidServiceStateException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.function.Consumer;

public class CSvcUtils {

    @FunctionalInterface
    public interface CheckedRunnable {
        // NOTE: Allow any checked exception
        void run() throws Exception;
    }

    @FunctionalInterface
    public interface CheckedConsumer<T> {
        // NOTE: Allow any checked exception
        void accept(T t) throws Exception;
    }

    @FunctionalInterface
    public interface CheckedSupplier<T> {
        // NOTE: Allow any checked exception
        T get() throws Exception;
    }

    @FunctionalInterface
    public interface CheckedFunction<T, R> {
        // NOTE: Allow any checked exception
        R apply(T t) throws Exception;
    }

    // TODO: Clean this up
    public static void unmapMappedBuffer(MappedByteBuffer buffer) {
        if (buffer == null) return;
        try {
            Method cleanerMethod = buffer.getClass().getMethod("cleaner");
            cleanerMethod.setAccessible(true);
            Object cleaner = cleanerMethod.invoke(buffer);
            if (cleaner != null) {
                Method cleanMethod = cleaner.getClass().getMethod("clean");
                cleanMethod.setAccessible(true);
                cleanMethod.invoke(cleaner);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to unmap MappedByteBuffer", e);
        }
    }

    public static void retryOperation(CheckedRunnable operation, Runnable failOverOperation, int retryMax) {
        int retryCount = 0;
        while (retryCount < retryMax) {
            retryCount++;
            try {
                operation.run();
                return;
            } catch (Exception e) {
                if (retryCount >= retryMax) {
                    failOverOperation.run();
                    return;
                }
            }
        }
    }

    public static void retryOperation(CheckedRunnable operation, Consumer<Exception> failOverOperation,
                                      int retryMax) {
        int retryCount = 0;
        while (retryCount < retryMax) {
            retryCount++;
            try {
                operation.run();
                return;
            } catch (Exception e) {
                if (retryCount >= retryMax) {
                    failOverOperation.accept(e);
                    return;
                }
            }
        }
    }

    public static <E> E retryCheckedOperation(CheckedSupplier<E> operation,
                                              CheckedConsumer<Exception> failOverOperation, int retryMax)
            throws InvalidServiceStateException {
        int retryCount = 0;
        while (retryCount < retryMax) {
            retryCount++;
            try {
                return operation.get();
            } catch (Exception e) {
                if (retryCount >= retryMax) {
                    try {
                        failOverOperation.accept(e);
                    } catch (Exception ex) {
//                        Throwable cause = ex;
//                        while (cause instanceof InvalidServiceStateException && !Objects.isNull(cause.getCause())) {
//                            cause = cause.getCause();
//                        }
                        throw new InvalidServiceStateException(unwrapRootCause(ex));
                    }
                }
            }
        }
        throw new InvalidServiceStateException("ERROR - Failed all retries");
    }

    public static <E> E retryCheckedOperation(CheckedSupplier<E> operation,
                                              CheckedFunction<Exception, E> failOverOperation, int retryMax, E failOverValue)
                                                                            throws InvalidServiceStateException {
        int retryCount = 0;
        while (retryCount < retryMax) {
            retryCount++;
            try {
                return operation.get();
            } catch (Exception e) {
                if (retryCount >= retryMax) {
                    try {
                        return failOverOperation.apply(e);
                    } catch (Exception ex) {
//                        Throwable cause = ex;
//                        while (cause instanceof InvalidServiceStateException && !Objects.isNull(cause.getCause())) {
//                            cause = cause.getCause();
//                        }
                        throw new InvalidServiceStateException(unwrapRootCause(ex));
                    }
                }
            }
        }
        return failOverValue;
    }

    public static <E> E retryCheckedOperation(CheckedSupplier<E> operation,
                                              CheckedFunction<Exception, E> failOverOperation, int retryMax,
                                              InvalidServiceStateException failOverException)
            throws InvalidServiceStateException {
        int retryCount = 0;
        while (retryCount < retryMax) {
            retryCount++;
            try {
                return operation.get();
            } catch (Exception e) {
                if (retryCount >= retryMax) {
                    try {
                        return failOverOperation.apply(e);
                    } catch (Exception ex) {
//                        Throwable cause = ex;
//                        while (cause instanceof InvalidServiceStateException && !Objects.isNull(cause.getCause())) {
//                            cause = cause.getCause();
//                        }
                        throw new InvalidServiceStateException(unwrapRootCause(ex));
                    }
                }
            }
        }
        throw failOverException;
    }

    public static <E> E retryCheckedOperation(CheckedSupplier<E> operation,
                                              CheckedConsumer<Exception> failOverOperation, int retryMax,
                                              InvalidServiceStateException failOverException)
            throws InvalidServiceStateException {
        int retryCount = 0;
        while (retryCount < retryMax) {
            retryCount++;
            try {
                return operation.get();
            } catch (Exception e) {
                if (retryCount >= retryMax) {
                    try {
                        failOverOperation.accept(e);
                    } catch (Exception ex) {
//                        Throwable cause = ex;
//                        while (cause instanceof InvalidServiceStateException && !Objects.isNull(cause.getCause())) {
//                            cause = cause.getCause();
//                        }
                        throw new InvalidServiceStateException(unwrapRootCause(ex));
                    }
                }
            }
        }
        throw failOverException;
    }

    public static void retryCheckedOperation(CheckedRunnable operation, CheckedConsumer<Exception> failOverOperation,
                                      int retryMax) throws InvalidServiceStateException {
        int retryCount = 0;
        while (retryCount < retryMax) {
            retryCount++;
            try {
                operation.run();
                return;
            } catch (Exception e) {
                if (retryCount >= retryMax) {
                    try {
                        failOverOperation.accept(e);
                    } catch (Exception ex) {
//                        Throwable cause = ex;
//                        while (cause instanceof InvalidServiceStateException && !Objects.isNull(cause.getCause())) {
//                            cause = cause.getCause();
//                        }
                        throw new InvalidServiceStateException(unwrapRootCause(ex));
                    }
                    return;
                }
            }
        }
    }

    public static boolean tbdCustomFileExists(Path path) throws InvalidServiceStateException {
        try (RandomAccessFile ignoredFile = new RandomAccessFile(path.toFile(), "r");
             FileChannel ignoredChannel = ignoredFile.getChannel()) {
            return true;
        } catch (NoSuchFileException | FileNotFoundException e) {
            return false;
        } catch (IOException e) {
            throw new InvalidServiceStateException(e);
        }
    }

    public static Throwable unwrapRootCause(Throwable cause) {
        while (cause instanceof InvalidServiceStateException && !Objects.isNull(cause.getCause())) {
            cause = cause.getCause();
        }
        return cause;
    }

//    public static <E extends Exception> void retryOperation(
//            Runnable operation,   // The operation to retry
//            int maxRetries,          // Maximum number of retries
//            Class<E> retryOnException // Exception type to retry on
//    ) throws E {
//        int retryCount = 0;
//        while (true) {
//            try {
//                operation.run();  // Execute the operation
//                return;
//            } catch (Exception e) {
//                if (!retryOnException.isInstance(e)) {
//                    throw e;  // If it's not the specified exception, rethrow it
//                }
//                retryCount++;
//                if (retryCount >= maxRetries) {
//                    throw retryOnException.cast(e); // Rethrow the last caught exception if max retries reached
//                }
//            }
//        }
//    }

    public static class PrioritizedTask<V> extends FutureTask<V> implements Comparable<PrioritizedTask<V>> {
//    public static class PrioritizedTask<V> extends FutureTask<V> implements Callable<V>, Comparable<CSvcUtils.PrioritizedTask<V>> {
        private final int priority;
//        private final Callable<V> task;

        public PrioritizedTask(int priority, Callable<V> task) {
            super(task);
            this.priority = priority;
//            this.task = task;
        }
//
//        @Override
//        public V call() throws Exception {
//            return task.call();
//        }

        @Override
        public int compareTo(PrioritizedTask other) {
            // NOTE: lower value -> higher priority
            return Integer.compare(this.priority, other.priority);
        }
    }

}
