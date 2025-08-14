package org.example;

import dsg.ccsvc.InvalidServiceStateException;
import dsg.ccsvc.client.CSvcClient;
import dsg.ccsvc.util.DebugConstants;
import io.javalin.Javalin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.example.manager.CSvcApplicationManager;
import org.example.manager.ZKApplicationManager;
import org.example.model.Record;
import org.example.storage.SimpleStore;
import org.example.util.HTTPUtil;
import org.example.util.ZKConfigs;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WebAppDemo {
//public class ZKDemo {
    // NOTE: Used to be called ZKDemo
    // TODO: Rename class

    private static Options options = new Options();

    static {
        options.addOption(new Option("df", false, "debug flag"));
        options.addOption(new Option("lf", false, "log flag"));
        options.addOption(new Option("mf", false, "metric flag"));
//        options.addOption(new Option("temp", false, "temp flag"));
    }

    public static void main(String[] args) {
        //org.eclipse.jetty.util.log.Slf4jLog
//        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "Info");
//        System.setProperty("org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog");
//        System.setProperty("org.eclipse.jetty.LEVEL", "OFF");
//        System.setProperty("org.eclipse.jetty.LEVEL", "INFO");
//        System.setProperty("log4j.category.org.eclipse.jetty", "error");
//        Logger LOGGER = LoggerFactory.getLogger(ZKDemo.class);
//        LOGGER
//        org.apache.log4j.Logger logger4j = org.apache.log4j.Logger.getRootLogger();
//        System.out.println("Hello world!");
        int flag = Integer.parseInt(args[0]);
        CSvcClient.ManagerType type = CSvcClient.ManagerType.fromInteger(Integer.parseInt(args[1]));
//        boolean[] isRunning = {true};
//        app.get("/javalindemo/kill", ctx -> {
//            ctx.html("Bye world!");
//            isRunning[0] = false;
//        });
//        if (args.length > 1) {
//            // NOTE: Enable all prints
//            DebugConstants.DEBUG_FLAG = true;
//        }
        DefaultParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(String.format("CSvcFSStoreDemo - Failed command line options parsing"));
            throw new RuntimeException(e);
        }
        DebugConstants.METRICS_FLAG = cl.hasOption("mf") || DebugConstants.METRICS_FLAG;
        DebugConstants.DEBUG_FLAG = cl.hasOption("df") || DebugConstants.DEBUG_FLAG;

        if (flag == 0) {
            // TODO: Basic node with CSvc
            DebugConstants.LOGREADER_FLAG = true;
            SimpleStore dataStore = new SimpleStore();
            dataStore.updateRecord(new Record("dummyKey", "dummyOwner",
                    "dummyData".getBytes(StandardCharsets.UTF_8)));
            boolean appStartedFlag = false;
            while (!appStartedFlag) {
                try (CSvcApplicationManager mgr = new CSvcApplicationManager(dataStore, type)) {
                    mgr.startApplication();
                    appStartedFlag = true;
                    while (mgr.isRunning()) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } catch (InvalidServiceStateException | InterruptedException e) {
                    System.out.println(String.format("CRITICAL ERROR - ZKDemo - Retrying app start"));
                    e.printStackTrace(System.out);
                }
            }

        } else if (flag == 1) {
            // TODO: Basic node with ZK
            SimpleStore dataStore = new SimpleStore();
            dataStore.updateRecord(new Record("dummyKey", "dummyOwner",
                    "dummyData".getBytes(StandardCharsets.UTF_8)));
//            try (CSvcApplicationManager mgr = new CSvcApplicationManager(dataStore, true)) {
            try (ZKApplicationManager mgr = new ZKApplicationManager(dataStore)) {
                mgr.startApplication();
                while (mgr.isRunning()) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            } catch (InvalidServiceStateException e) {
                // TODO: split up zk and non zk apps
                // NOTE: Should never happen
                throw new RuntimeException(e);
            }
        } else if (flag == 2) {
            // TODO: Test endpoints
            try {
                testEndpointPerformance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (flag == 3) {
            // TODO: Test a client that keeps restarting
            DebugConstants.LOGREADER_FLAG = false;
            boolean[] isRunning = {true};
            SimpleStore dataStore = new SimpleStore();
            dataStore.updateRecord(new Record("dummyKey", "dummyOwner",
                    "dummyData".getBytes(StandardCharsets.UTF_8)));
            try (Javalin app = Javalin.create()) {
//        Javalin app = Javalin.create();
                app.start(8100); // DIFFERENT PORT
//                app.get("/javalindemo", ctx -> ctx.html("Hello world!"));
                app.get("/javalindemorestart/kill", ctx -> {
                    ctx.html("Bye world!");
                    isRunning[0] = false;
                });
                while (isRunning[0]) {
//                    boolean appStartedFlag = false;
//                    while (!appStartedFlag && isRunning[0]) {
                    try (CSvcApplicationManager mgr =
                                 new CSvcApplicationManager(dataStore, type)) {
                        mgr.startApplication();
//                            appStartedFlag = true;

                        try {
//                            Thread.sleep(150);
                            Thread.sleep(550);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } catch (InvalidServiceStateException | InterruptedException e) {
                        System.out.println(String.format("CRITICAL ERROR - ZKDemo - Retrying app start"));
                        e.printStackTrace(System.out);
                    }
//                    }

                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                app.stop();
            }
        } else if (flag == 4) {
            // NOTE: Test client that performs actions and restarts
            DebugConstants.LOGREADER_FLAG = false;
            boolean[] isRunning = {true};
            SimpleStore dataStore = new SimpleStore();
            dataStore.updateRecord(new Record("dummyKey", "dummyOwner",
                    "dummyData".getBytes(StandardCharsets.UTF_8)));
            try (Javalin app = Javalin.create()) {
//        Javalin app = Javalin.create();
                app.start(8100); // DIFFERENT PORT
//                app.get("/javalindemo", ctx -> ctx.html("Hello world!"));
                app.get("/javalindemorestart/kill", ctx -> {
                    ctx.html("Bye world!");
                    isRunning[0] = false;
                });
                while (isRunning[0]) {
//                    boolean appStartedFlag = false;
//                    while (!appStartedFlag && isRunning[0]) {
                    try (CSvcApplicationManager mgr =
                                 new CSvcApplicationManager(dataStore, type)) {
                        mgr.startApplication();
//                            appStartedFlag = true;

                        try {
//                            Thread.sleep(150);
                            Thread.sleep(550);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } catch (InvalidServiceStateException | InterruptedException e) {
                        System.out.println(String.format("CRITICAL ERROR - ZKDemo - Retrying app start"));
                        e.printStackTrace(System.out);
                    }
//                    }

                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                app.stop();
            }

        }
    }

    public static void testEndpointPerformance() throws Exception {
        HttpClient client = new HttpClient();
        client.start();
        ArrayList<byte[]> inputRecords = new ArrayList<>();
        String baseKey = "performanceKey";
        String baseOwner = "performanceOwner";
        byte[] generatedBytes = RandomStringUtils.randomAlphanumeric(10).getBytes(StandardCharsets.UTF_8);
        int count = 0;
        while (count < 1000) {
            inputRecords.add(SerializationUtils.serialize(
                    new Record(baseKey+count, baseOwner+count, generatedBytes)));
            count++;
        }
        StopWatch watch = new StopWatch();
        watch.start();
        for (byte[] serializedDump : inputRecords) {
            ContentResponse bytesResponse = client.POST(String.format("http://%s%s",
                    ZKConfigs.getHostPostOfServer(),
                    HTTPUtil.UPDATE_RECORD_ENDPOINT)).content(
                    new BytesContentProvider(serializedDump)).send();
//            ContentResponse bytesResponse = client.POST(String.format("http://10.0.0.176:7100%s",
//                    HTTPUtil.UPDATE_RECORD_ENDPOINT)).content(
//                    new BytesContentProvider(serializedDump)).send();
        }
        watch.stop();
        System.out.println(String.format(
                "Test complete [recordCount=%d, dataBytesCount=%d, execution_time_ms=%s]",
                count, generatedBytes.length, watch.getTime(TimeUnit.MILLISECONDS)));
        client.stop();
    }

    public static void testJavalinHelloWorld() {
//        Javalin app = Javalin.create()
//                .port(7000)
//                .start();
        boolean[] isRunning = {true};
        try (Javalin app = Javalin.create()) {
//        Javalin app = Javalin.create();
            app.start(7100);
            app.get("/javalindemo", ctx -> ctx.html("Hello world!"));
            app.get("/javalindemo/kill", ctx -> {
                ctx.html("Bye world!");
                isRunning[0] = false;
            });

            app.post("/javalindemo/input", ctx -> {
                ctx.html("===\n"+
                        ctx.body()+
                        "\n===\n");
                ctx.status(200);
            });


            while (isRunning[0]) {
                Thread.sleep(50);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static private void testJavalinEndpoints() throws Exception {
        HttpClient client = new HttpClient();
        client.start();
        ContentResponse response = client.GET("http://10.0.0.176:7100/javalindemo/records");
        log.info(String.format("testJavalinEndpoints - GET RESPONSE [allRecords_response=%s]", response.getContentAsString()));

        response = client.GET("http://localhost:7100/javalindemo/records/dummyKey");
        log.info(String.format("testJavalinEndpoints - GET RESPONSE [getRecord_response=%s]", response.getContentAsString()));

        ContentResponse bytesResponse = client.POST("http://10.0.0.176:7100/javalindemo/input").content(
                new BytesContentProvider("appKey|||appOwner|||54321".getBytes(StandardCharsets.UTF_8))).send();

        response = client.GET("http://10.0.0.176:7100/javalindemo/records");
        log.info(String.format("testJavalinEndpoints - GET RESPONSE [allRecords_response=%s]", response.getContentAsString()));
        client.stop();
//        HttpRequest request = new HttpRequest(URI.create("https://example.com/"))
//                .build();
//        HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
//        System.out.println("response body: " + response.body());
    }
}
