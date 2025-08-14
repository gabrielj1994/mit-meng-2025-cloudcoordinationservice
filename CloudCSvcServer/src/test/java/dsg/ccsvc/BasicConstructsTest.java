package dsg.ccsvc;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.concurrent.*;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.*;

@Slf4j
public class BasicConstructsTest {

    static class TestResourceClaimer implements Callable<Resource> {
        public Resource call() {
            // Simulate error
            throwException(true);
            return new Resource(false);
        }

    }

    static class Resource implements AutoCloseable {
        private boolean valid;

        public Resource(boolean valid) {
            if (!valid) {
                throw new IllegalStateException("Invalid resource");
            }
            this.valid = valid;
        }

        @Override
        public void close() {
            System.out.println("Resource closed");
        }
    }


    private static void throwException(boolean throwFlag) {
        if (throwFlag) {
            throw new RuntimeException("Test");
        }
    }

    @Test
    private void testSystemTimeDrift() {

        // Run 'date +%s%3N' to get system time in milliseconds
        try {
            StopWatch watch = new StopWatch();
            watch.start();
            Process process = Runtime.getRuntime().exec("date +%s%3N");
            process.waitFor();
            long javaTime = System.currentTimeMillis();
            watch.stop();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String systemTimeStr = reader.readLine();
            long systemTime = Long.parseLong(systemTimeStr.trim());

            long drift = Math.abs(javaTime - systemTime);

            log.info("Java Time: " + Instant.ofEpochMilli(javaTime));
            log.info("System Time: " + Instant.ofEpochMilli(systemTime));
            log.info("Drift (ms): " + drift);
            log.info("In-Process Stopwatch (ms): " + watch.getTime(TimeUnit.MILLISECONDS));
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    private void testFutureTryExceptions() {
        ExecutorService executor = Executors.newFixedThreadPool(1);

//        Future<Resource> future = executor.submit(() -> {
//            // Simulate error
//            throwException(true);
//            return new Resource(false);
//        });

        Future<Resource> future = executor.submit(new TestResourceClaimer());

        try {
            try (Resource resource = future.get()) { // Blocking call
                log.info("Using resource...");
            } catch (ExecutionException e) {
                log.error("Internal catch", e);
                throw e;
            } catch (RuntimeException e) {
                log.error("Internal catch runtime", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag
            log.error("Thread was interrupted", e);
        } catch (ExecutionException e) {
            log.error("Failed to acquire resource: " + e.getCause(), e);
        }

        future = executor.submit(() -> {
            // Simulate error
            return new Resource(true);
        });

        try {
            try (Resource resource = future.get()) { // Blocking call
                log.error("Using resource...");
            } catch (ExecutionException e) {
                log.error("Internal catch", e);
                throw e;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt flag
            log.error("Thread was interrupted", e);
        } catch (ExecutionException e) {
            log.error("Failed to acquire resource: " + e.getCause(), e);
        }

        executor.shutdown();
    }

}
