package com.ksayers.twoPhaseCommit;

import java.util.Map;
import java.util.Scanner;
import java.util.ArrayList;
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
        Pair<ArrayList<Integer>, ArrayList<String>> readyResponses = sendMessageToNodes("ready", message);
        System.out.println(readyResponses);

        return true;
    }

    /**
     * Sends a message to all nodes in the nodeList.
     * 
     * @param path The path to send the message to.
     * @param message The message to send.
     * @return A Pair object containing the response codes and response bodies from each node.
     */
    private Pair<ArrayList<Integer>, ArrayList<String>> sendMessageToNodes(String path, String message) {
        ArrayList<Integer> responseCodes = new ArrayList<Integer>();
        ArrayList<String> responseBodies = new ArrayList<String>();

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
                outputStream.write(message.getBytes());
                outputStream.flush();
                outputStream.close();
                
                // check response code
                int responseCode = connection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    System.out.println(String.format("Commit message sent to node %s at $s", nodeId, url));
                } else {
                    System.err.println(String.format("Failed to send commit message node %s at $s", nodeId, url));
                }

                // get response body
                InputStream inputStream = connection.getInputStream();
                byte[] responseBodyBytes = inputStream.readAllBytes();
                String responseBody = new String(responseBodyBytes);
                
                connection.disconnect();

                responseCodes.add(responseCode);
                responseBodies.add(responseBody);

            } catch (Exception exception) {
                System.err.println(String.format("Failed to send % message node %s", path, nodeId));
                exception.printStackTrace();

                responseCodes.add(null);
                responseBodies.add(null);
            }
        }

        return new Pair<ArrayList<Integer>, ArrayList<String>>(responseCodes, responseBodies);
    }

    public static void main(String[] args) throws IOException {
        CoordinatorNode node = new CoordinatorNode();

        Scanner userInput = new Scanner(System.in);
        while (userInput.hasNext()) {
            String input = userInput.nextLine();

            if (input.equals("asdf")) {
                break;
            }

            if (input.equals("transaction")) {
                System.out.println("Enter message to send to nodes");
                String message = userInput.nextLine();
                node.doTransaction(message);
            }
        }
        userInput.close();
        node.close();
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

    public void close() {
        server.stop(0);
    }
}
