package com.ksayers.twoPhaseCommit;

import java.util.Map;
import java.util.Scanner;
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

import com.ksayers.twoPhaseCommit.utils.Pair;

import org.json.JSONException;
import org.json.JSONObject;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;


public class CoordinatorNode implements AutoCloseable {
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

    public boolean doTransaction(String message) {
        return doTransaction(message, null);
    }

    public boolean doTransaction(String message, Scanner userInput) {
        // perform ready phase
        Map<String, Pair<Integer, String>> readyResponses = sendMessageToNodes("ready", message, false);

        // check if any nodes are not ready
        boolean readySuccess = true;
        for (Map.Entry<String, Pair<Integer, String>> entry : readyResponses.entrySet()) {
            Integer responseCode = entry.getValue().first;
            if (responseCode == null || responseCode != HttpURLConnection.HTTP_OK) {
                readySuccess = false;
                break;
            }
        }
        if (readySuccess) {
            System.out.println("All nodes are ready");
        } else {
            System.out.println("Not all nodes are ready, abort");
        }

        if (userInput != null) {
            System.out.println("Press enter to continue with commit/abort phase");
            userInput.nextLine();
        }

        // if not all nodes are ready, abort
        if (!readySuccess) {
            sendMessageToNodes("abort", message, true);
            return false;
        }

        // perform commit phase
        sendMessageToNodes("commit", message, true);
        return true;
    }

    private Map<String, Pair<Integer, String>> sendMessageToNodes(String path, String message, boolean retryForever) {
        Map<String, Pair<Integer, String>> responses = new HashMap<String, Pair<Integer, String>>();
        for (Map.Entry<String, InetSocketAddress> entry : nodeList.entrySet()) {
            String nodeId = entry.getKey();
            
            Pair<Integer, String> nodeResponse = sendMessageToNode(nodeId, path, message);
            Integer responseCode = nodeResponse.first;
            while (retryForever && responseCode == null) {
                nodeResponse = sendMessageToNode(nodeId, path, message);
                responseCode = nodeResponse.first;
            }

            responses.put(nodeId, nodeResponse);
        }

        return responses;
    }

    private Pair<Integer, String> sendMessageToNode(String nodeId, String path, String message) {
        InetSocketAddress address = nodeList.get(nodeId);

        try {
            URL url = new URI(String.format(
                "http://%s:%s/%s",
                address.getHostName(),
                address.getPort(),
                path
            )).toURL();
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setConnectTimeout(1000);
            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            
            // send message
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(message.getBytes());
            outputStream.flush();
            outputStream.close();
            
            // check response code
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                System.out.println(String.format("Successfully sent %s message to %s at %s", path, nodeId, url));
            } else {
                System.err.println(String.format("Failed to send %s message node %s at %s", path, nodeId, url));
                return new Pair<Integer,String>(responseCode, null);
            }

            // get response body
            InputStream inputStream = connection.getInputStream();
            byte[] responseBodyBytes = inputStream.readAllBytes();
            String responseBody = new String(responseBodyBytes);
            
            connection.disconnect();

            return new Pair<Integer,String>(responseCode, responseBody);

        } catch (Exception exception) {
            System.err.println(String.format("Failed to send %s message node %s", path, nodeId));

            return new Pair<Integer,String>(null, null);
        }
    }

    public static void main(String[] args) throws IOException {
        CoordinatorNode node = new CoordinatorNode();

        Scanner userInput = new Scanner(System.in);
        while (userInput.hasNext()) {
            String input = userInput.nextLine();

            if (input.equals("exit")) {
                break;
            }

            if (input.equals("t")) {
                System.out.println("Enter message to send to nodes");
                String message = userInput.nextLine();
                node.doTransaction(message, userInput);
            }
        }
        userInput.close();
        node.close();
        System.exit(0);
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
            System.out.println(String.format("Added new node: %s", nodeList));

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

    public void close() {
        server.stop(0);
    }
}
