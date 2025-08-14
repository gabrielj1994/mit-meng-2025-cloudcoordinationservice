package org.example;

import io.javalin.Javalin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.example.controller.SimpleController;
import org.example.model.Record;
import org.example.storage.DataStore;
import org.example.storage.SimpleStore;

import java.nio.charset.StandardCharsets;

@Slf4j
public class Main {
    public static void main(String[] args) {
//        System.out.println("Hello world!");
        int flag = Integer.parseInt(args[0]);
        if (flag == 0) {
            // NOTE: Try to insert and wait
            testJavalinHelloWorld();
        } else if (flag == 1) {
            testJavalinDatastore();
        }
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
                Thread.sleep(25);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void testJavalinDatastore() {
        DataStore store = new SimpleStore();
        store.updateRecord(new Record("dummyKey", "dummyOwner",
                "dummyData".getBytes(StandardCharsets.UTF_8)));
        SimpleController controller = new SimpleController(store);

        boolean[] isRunning = {true};
        try (Javalin app = Javalin.create()) {
//            app.start(7100);
//            app.get("/javalindemo", ctx -> ctx.html("Hello world!"));
//            app.get("/javalindemo/records", ctx ->
//                    ctx.html("Records:\n===\n"+
//                    store.getAllRecords().toString()+
//                    "\n==="));
//            app.get("/javalindemo/records/{key}", controller.readRecordHandler());
//            app.get("/javalindemo/kill", ctx -> {
//                ctx.html("Bye world!");
//                isRunning[0] = false;
//            });
//
//            app.post("/javalindemo/input", controller.updateRecordHandler());
            initializeEndpoints(app, store, controller, isRunning);

            try {
                testJavalinEndpoints();
            } catch (Exception e) {
                log.error("====\nFAILED TEST:\n\ttestJavalinEndpoints\n====");
                log.error(e.toString());
            }

            while (isRunning[0]) {
                Thread.sleep(25);
            }


        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static private void initializeEndpoints(Javalin app, DataStore store, SimpleController controller, boolean[] isRunning) {
        app.start(7100);
        app.get("/javalindemo", ctx -> ctx.html("Hello world!"));
        app.get("/javalindemo/records", ctx ->
                ctx.html("Records:\n===\n"+
                        store.getAllRecords().toString()+
                        "\n==="));
        app.get("/javalindemo/records/{key}", controller.readRecordSimpleSerializeHandler());
        app.get("/javalindemo/records/string/{key}", controller.readRecordSimpleStringHandler());
        app.get("/javalindemo/kill", ctx -> {
            ctx.html("Bye world!");
            isRunning[0] = false;
        });

        app.post("/javalindemo/input", controller.updateRecordSimpleHandler());
    }

    static private void testJavalinEndpoints() throws Exception {
        HttpClient client = new HttpClient();
        client.start();
        ContentResponse response = client.GET("http://10.0.0.176:7100/javalindemo/records");
        log.info(String.format("testJavalinEndpoints - GET RESPONSE [allRecords_response=%s]", response.getContentAsString()));

        response = client.GET("http://localhost:7100/javalindemo/records/string/dummyKey");
        log.info(String.format("testJavalinEndpoints - GET RESPONSE [getRecord_response=%s]", response.getContentAsString()));

        response = client.GET("http://localhost:7100/javalindemo/records/dummyKey");
        log.info(String.format("testJavalinEndpoints - GET RESPONSE [getRecord_response=%s]",
                SerializationUtils.deserialize(response.getContent()).toString()));

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
