package org.example.controller;

import io.javalin.http.Handler;
import org.apache.commons.lang3.SerializationUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.example.model.Record;
import org.example.storage.DataStore;
import org.example.util.HTTPAutocloseWrapper;
import org.example.util.WebAppDemoMetadata;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import static org.example.util.ZKConfigs.getHostPostOfServer;

//@RestController
public class SimpleController {

//  private RestTemplate restTemplate = new RestTemplate();

    private DataStore dataStore;

    public SimpleController (DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public Handler updateRecordSimpleHandler() {
        return ctx -> {
            Record record = parseRecord(ctx.bodyAsBytes());
            dataStore.updateRecord(record);
        };
    }

    public Handler updateRecordSerializeHandler() {
        return ctx -> {
            Record record = SerializationUtils.deserialize(ctx.bodyAsBytes());
            dataStore.updateRecord(record);
        };
    }

    public Handler updateRecordZKHandler() {
        // TODO: Consider serialization
        return ctx -> {
//    String requestFrom = request.getHeader("request_from");
//    String leader = ClusterInfo.getClusterInfo().getMaster();
//    if (!isEmpty(requestFrom) && requestFrom.equalsIgnoreCase(leader)) {
//      Person person = new Person(id, name);
//      DataStorage.setPerson(person);
//      return ResponseEntity.ok("SUCCESS");
//    }
//    // If I am leader I will broadcast data to all live node, else forward request to leader
            String leader = WebAppDemoMetadata.getClusterInfo().getMaster();
            Record record;
            if (leader.equalsIgnoreCase(String.valueOf(ctx.formParam("request_source")))) {
                record = parseRecord(ctx.bodyAsBytes());
//                record = SerializationUtils.deserialize(ctx.bodyAsBytes());
                dataStore.updateRecord(record);
            }

//    if (amILeader()) {
//      List<String> liveNodes = ClusterInfo.getClusterInfo().getLiveNodes();
//
//      int successCount = 0;
//      for (String node : liveNodes) {
//
//        if (getHostPostOfServer().equals(node)) {
//          Person person = new Person(id, name);
//          DataStorage.setPerson(person);
//          successCount++;
//        } else {
            HttpClient client = new HttpClient();
//            client.start();
            try (HTTPAutocloseWrapper closableClient = new HTTPAutocloseWrapper(client)) {
                if (amILeader()) {
                    List<String> liveNodes = WebAppDemoMetadata.getClusterInfo().getLiveNodes();

                    int successCount = 0;
                    for (String node : liveNodes) {
                        if (getHostPostOfServer().equals(node)) {
                            record = parseRecord(ctx.bodyAsBytes());
                            dataStore.updateRecord(record);
                            successCount++;
                        } else {
                            // TODO: Create request to other node
                            // TODO: Consider what to do with response
                            // TODO: Consider non happy-path execution (needing retry)
                            ContentResponse bytesResponse =
                                    client.POST(String.format("http://%s:7100/javalindemo/input", node))
                                            .content(new BytesContentProvider(ctx.bodyAsBytes()))
                                            .param("request_source", leader)
                                            .send();
                            successCount++;
//              String requestUrl =
//              "http://"
//                  .concat(node)
//                  .concat("person")
//                  .concat("/")
//                  .concat(String.valueOf(id))
//                  .concat("/")
//                  .concat(name);
//          HttpHeaders headers = new HttpHeaders();
//          headers.add("request_from", leader);
//          headers.setContentType(MediaType.APPLICATION_JSON);
//
//          HttpEntity<String> entity = new HttpEntity<>(headers);
//          restTemplate.exchange(requestUrl, HttpMethod.PUT, entity, String.class).getBody();
//          successCount++;
                        }
                    }
                    ctx.status(200);
                } else {
                    // TODO: Forward request to leader
                    // TODO: Consider what to do with response
                    // TODO: Consider non happy-path execution (needing retry)
                    ContentResponse bytesResponse =
                            client.POST(String.format("http://%s:7100/javalindemo/input", leader))
                                    .content(new BytesContentProvider(ctx.bodyAsBytes()))
                                    .param("request_source", getHostPostOfServer())
                                    .send();
//      String requestUrl =
//          "http://"
//              .concat(leader)
//              .concat("person")
//              .concat("/")
//              .concat(String.valueOf(id))
//              .concat("/")
//              .concat(name);
//      HttpHeaders headers = new HttpHeaders();
//
//      headers.setContentType(MediaType.APPLICATION_JSON);
//
//      HttpEntity<String> entity = new HttpEntity<>(headers);
//      return restTemplate.exchange(requestUrl, HttpMethod.PUT, entity, String.class);
                }
            }
        };
    }

    public Handler readRecordSimpleSerializeHandler() {
        return ctx -> {
            String key = Objects.requireNonNull(ctx.pathParam("key"));
            Optional<Record> record = dataStore.getRecord(key);
            if(record.isPresent()) {
                ctx.result(SerializationUtils.serialize(record.get()));
            } else {
                ctx.html("404: Record Not Found");
                ctx.status(404);
            }
        };
    }

    public Handler readRecordSimpleStringHandler() {
        return ctx -> {
            String key = Objects.requireNonNull(ctx.pathParam("key"));
            Optional<Record> record = dataStore.getRecord(key);
            if(record.isPresent()) {
                ctx.html(record.get().toString());
            } else {
                ctx.html("404: Record Not Found");
                ctx.status(404);
            }
        };
    }

    public Handler readAllRecordsSerializedHandler() {
        //store.getAllRecords()
        return ctx -> {
//            String key = Objects.requireNonNull(ctx.pathParam("key"));
//            List<Record> record = dataStore.getAllRecords();
//            if(record.isPresent()) {
//            ctx.json(dataStore.getAllRecords());
            ctx.result(SerializationUtils.serialize((Serializable) dataStore.getAllRecords()));
            ctx.status(200);
//            } else {
//                ctx.html("404: Record Not Found");
//                ctx.status(404);
//            }
        };
    }

    public Handler readAllRecordsStringHandler() {
        //store.getAllRecords()
        return ctx -> {
//            String key = Objects.requireNonNull(ctx.pathParam("key"));
//            List<Record> record = dataStore.getAllRecords();
//            if(record.isPresent()) {
//            ctx.json(dataStore.getAllRecords());
            ctx.html("Records:\n===\n"+
                    dataStore.getAllRecords().toString()+
                    "\n===");
            ctx.status(200);
//            } else {
//                ctx.html("404: Record Not Found");
//                ctx.status(404);
//            }
        };
    }

    // TODO: Consider what this is used for
//    @GetMapping("/clusterInfo")
//    public ResponseEntity<ZKMetadata> getClusterinfo() {
    public Handler clusterInfoHandler() {
//        return ResponseEntity.ok(ClusterInfo.getClusterInfo());
        return ctx -> ctx.json(WebAppDemoMetadata.getClusterInfo());
    }

    private boolean amILeader() {
        String leader = WebAppDemoMetadata.getClusterInfo().getMaster();
        return getHostPostOfServer().equals(leader);
    }

    private Record parseRecord(byte[] byteLine) {
        String delimiter = "|||";
        List<byte[]> tokens = tokenizeByteArr(byteLine, delimiter);
        // TODO: Validations. For demo assume happy path?
        Record returnRecord = new Record(
                new String(tokens.get(0), StandardCharsets.UTF_8),
                new String(tokens.get(1), StandardCharsets.UTF_8),
                tokens.get(2));
        return returnRecord;
    }

    public static List<byte[]> tokenizeByteArr(byte[] rawByte, String tokenDelimiter) {
        Pattern pattern = Pattern.compile(tokenDelimiter, Pattern.LITERAL);
        String[] parts = pattern.split(new String(rawByte, StandardCharsets.UTF_8), 3);
        List<byte[]> ret = new ArrayList<byte[]>();
        for (String part : parts)
            ret.add(part.getBytes(StandardCharsets.UTF_8));
        return ret;
    }

//  @PutMapping("/person/{id}/{name}")
//  public ResponseEntity<String> savePerson(
//      HttpServletRequest request,
//      @PathVariable("id") Integer id,
//      @PathVariable("name") String name) {
//
//    String requestFrom = request.getHeader("request_from");
//    String leader = ClusterInfo.getClusterInfo().getMaster();
//    if (!isEmpty(requestFrom) && requestFrom.equalsIgnoreCase(leader)) {
//      Person person = new Person(id, name);
//      DataStorage.setPerson(person);
//      return ResponseEntity.ok("SUCCESS");
//    }
//    // If I am leader I will broadcast data to all live node, else forward request to leader
//    if (amILeader()) {
//      List<String> liveNodes = ClusterInfo.getClusterInfo().getLiveNodes();
//
//      int successCount = 0;
//      for (String node : liveNodes) {
//
//        if (getHostPostOfServer().equals(node)) {
//          Person person = new Person(id, name);
//          DataStorage.setPerson(person);
//          successCount++;
//        } else {
//          String requestUrl =
//              "http://"
//                  .concat(node)
//                  .concat("person")
//                  .concat("/")
//                  .concat(String.valueOf(id))
//                  .concat("/")
//                  .concat(name);
//          HttpHeaders headers = new HttpHeaders();
//          headers.add("request_from", leader);
//          headers.setContentType(MediaType.APPLICATION_JSON);
//
//          HttpEntity<String> entity = new HttpEntity<>(headers);
//          restTemplate.exchange(requestUrl, HttpMethod.PUT, entity, String.class).getBody();
//          successCount++;
//        }
//      }
//
//      return ResponseEntity.ok()
//          .body("Successfully update ".concat(String.valueOf(successCount)).concat(" nodes"));
//    } else {
//      String requestUrl =
//          "http://"
//              .concat(leader)
//              .concat("person")
//              .concat("/")
//              .concat(String.valueOf(id))
//              .concat("/")
//              .concat(name);
//      HttpHeaders headers = new HttpHeaders();
//
//      headers.setContentType(MediaType.APPLICATION_JSON);
//
//      HttpEntity<String> entity = new HttpEntity<>(headers);
//      return restTemplate.exchange(requestUrl, HttpMethod.PUT, entity, String.class);
//    }
//  }
}
