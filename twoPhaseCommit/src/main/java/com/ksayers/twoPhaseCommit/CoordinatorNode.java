package com.ksayers.twoPhaseCommit;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;


public class CoordinatorNode {
    Map<String, InetSocketAddress> nodeList = new HashMap<String, InetSocketAddress>();
    ThreadPoolExecutor serverThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
    HttpServer server;
    InetSocketAddress address = new InetSocketAddress("localhost", 8000);

    public CoordinatorNode() throws IOException {
        server = HttpServer.create(address, 10);
        server.createContext("/register", new CoordinatorNodeHttpHandler());
        server.setExecutor(serverThreadPool);
        server.start();
        System.out.println(String.format("Listening on %s", address));
    }

    public void sendReady(Integer message) {
        sendMessageToNodes("ready", message);
    }

    public void sendCommit(Integer message) {
        sendMessageToNodes("commit", message);
    }

    public void sendAbort(Integer message) {
        sendMessageToNodes("abort", message);
    }

    private void sendMessageToNodes(String path, Integer message) {
        for (Map.Entry<String, InetSocketAddress> entry : nodeList.entrySet()) {
            String nodeId = entry.getKey();
            InetSocketAddress address = entry.getValue();
            
            try {
                URL url = new URI(String.format(
                    "http://%s:%s/%s",
                    address.getHostName(),
                    address.getPort(),
                    path
                )).toURL();
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("PUT");
                connection.setDoOutput(true);
                
                // send commit message
                OutputStream outputStream = connection.getOutputStream();
                outputStream.write(String.valueOf(message).getBytes());
                outputStream.flush();
                outputStream.close();
                
                // check response code
                int responseCode = connection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    System.out.println(String.format("Commit message sent to node %s at $s", nodeId, url));
                } else {
                    System.err.println(String.format("Failed to send commit message node %s at $s", nodeId, url));
                }
                
                connection.disconnect();

            } catch (Exception exception) {
                System.err.println(String.format("Failed to send % message node %s", path, nodeId));
                exception.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        @SuppressWarnings("unused")
        CoordinatorNode node = new CoordinatorNode();
    }

    /*
     * Handle register requests
     */
    private class CoordinatorNodeHttpHandler implements HttpHandler {
        @Override    
        public void handle(HttpExchange httpExchange) {
            try {
                if (!httpExchange.getRequestMethod().equals("POST")) {
                    CoordinatorNode.sendErrorResponse(httpExchange);
                    return;
                }

                registerNode(httpExchange);
            
            } catch (IOException | JSONException exception) {
                exception.printStackTrace();
                CoordinatorNode.sendErrorResponse(httpExchange);
            }
        }

        private void registerNode(HttpExchange httpExchange) throws IOException {
            // parse request body
            InputStream requestBodyStream = httpExchange.getRequestBody();
            byte[] requestBodyBytes = requestBodyStream.readNBytes(100);
            String requestBody = new String(requestBodyBytes);
            JSONObject requestJson;
            try {
                requestJson = new JSONObject(requestBody);
            }
            catch (JSONException exception) {
                exception.printStackTrace();
                CoordinatorNode.sendErrorResponse(httpExchange);
                return;
            }

            // input validation
            if (!requestJson.has("nodeId") || !requestJson.has("hostname") || !requestJson.has("port")) {
                CoordinatorNode.sendErrorResponse(httpExchange);
                return;
            }

            // update node list
            nodeList.put(
                requestJson.getString("nodeId"),
                new InetSocketAddress(requestJson.getString("hostname"), requestJson.getInt("port"))
            );
            System.out.println(nodeList);

            // send okay response
            OutputStream outputStream = httpExchange.getResponseBody();
            httpExchange.sendResponseHeaders(200, 0);
            outputStream.flush();
            outputStream.close();
        }
    }

    private static void sendErrorResponse(HttpExchange httpExchange) {
        System.out.println("Invalid request");

        OutputStream outputStream = httpExchange.getResponseBody();
        try {
            httpExchange.sendResponseHeaders(400, 0);
            outputStream.flush();
            outputStream.close();
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }
}
