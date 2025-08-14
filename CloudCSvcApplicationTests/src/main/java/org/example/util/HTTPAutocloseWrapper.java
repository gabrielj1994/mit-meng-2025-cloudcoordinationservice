package org.example.util;

import org.eclipse.jetty.client.HttpClient;

public class HTTPAutocloseWrapper implements AutoCloseable {

    private HttpClient client;

    public HTTPAutocloseWrapper(HttpClient client) throws Exception {
        this.client = client;
        client.start();
    }

    public void close() {
        try {
            client.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
