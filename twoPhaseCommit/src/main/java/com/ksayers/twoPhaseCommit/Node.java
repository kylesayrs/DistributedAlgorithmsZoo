package com.ksayers.twoPhaseCommit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;


public class Node {
    InetSocketAddress coordinatorAddress = new InetSocketAddress("localhost", 8000);
    ArrayList<String> readyLedger = new ArrayList<String>();
    ArrayList<String> commitLedger = new ArrayList<String>();

    ThreadPoolExecutor serverThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    HttpServer server = null;

    String nodeId;
    InetSocketAddress address = null;

    public Node(String _nodeId, Integer port) throws Exception {
        nodeId = _nodeId;
        address = new InetSocketAddress("localhost", port);
        goOnline();

        // register with coordinator
        registerNode();
    }

    public void goOnline() throws Exception {
        if (server == null) {
            server = HttpServer.create(address, 5);
            server.createContext("/ready", new ReadyHttpHandler());
            server.createContext("/commit", new CommitHttpHandler());
            server.createContext("/abort", new AbortHttpHandler());
            server.setExecutor(serverThreadPool);
            server.start();
        }

        System.out.println(String.format("Node %s online", nodeId));
    }

    public void goOffline() {
        if (server != null) {
            server.stop(1);
            server = null;
        }

        System.out.println(String.format("Node %s offline", nodeId));
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Must specify nodeId and port");
            return;
        }

        Node node = new Node(args[0], Integer.parseInt(args[1]));

        Scanner userInput = new Scanner(System.in);
        while (userInput.hasNext()) {
            String input = userInput.nextLine();

            if (input.equals("exit")) {
                break;
            }

            if (input.equals("online")) {
                node.goOnline();
            }

            if (input.equals("offline")) {
                node.goOffline();
            }
        }
        userInput.close();
        System.exit(0);
    }

    private void registerNode() throws Exception {
        // construct url
        URL url = new URI(String.format(
            "http://%s:%s/register",
            coordinatorAddress.getHostName(),
            coordinatorAddress.getPort()
        )).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        System.out.println(String.format("Sending register request to %s", url));

        // construct payload
        Map<String, Object> jsonMap = new HashMap<String, Object>();
        if (address != null) {
            jsonMap.put("nodeId", nodeId);
            jsonMap.put("hostname", address.getHostName());
            jsonMap.put("port", address.getPort());
        }
        JSONObject jsonObject = new JSONObject(jsonMap);
        String payload = jsonObject.toString();

        // send request
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Content-Length", Integer.toString(payload.getBytes().length));
        DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream());
        outputStream.writeBytes(payload);
        outputStream.close();

        // get response
        if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
            System.err.println("Could not register with coordinator node");
            return;
        } else {
            System.out.println("Successfully registered with coordinator");
        }

        connection.disconnect();
    }

    private class ReadyHttpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) {
            if (!httpExchange.getRequestMethod().equals("PUT")) {
                Node.sendErrorResponse(httpExchange);
                return;
            }

            // extract request body
            String requestBody = getRequestBody(httpExchange);
            if (requestBody == null) {
                Node.sendErrorResponse(httpExchange);
                return;
            }

            // check if can ready
            if (readyLedger.contains(requestBody) || commitLedger.contains(requestBody)) {
                System.out.println(String.format("Cannot ready %s %s", readyLedger, commitLedger));
                Node.sendErrorResponse(httpExchange);
                return;
            }

            // mark as ready
            boolean addSuccess = readyLedger.add(requestBody);
            assert addSuccess;
            System.out.println(String.format("Readied %s %s", readyLedger, commitLedger));

            // send okay response
            try {
                OutputStream outputStream = httpExchange.getResponseBody();
                httpExchange.sendResponseHeaders(200, 0);
                outputStream.flush();
                outputStream.close();
            } catch (Exception exception) {
                exception.printStackTrace();
                Node.sendErrorResponse(httpExchange);
            }
        }
    }

    private class CommitHttpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) {
            if (!httpExchange.getRequestMethod().equals("PUT")) {
                Node.sendErrorResponse(httpExchange);
                return;
            }

            // extract request body
            String requestBody = getRequestBody(httpExchange);
            if (requestBody == null) {
                Node.sendErrorResponse(httpExchange);
                return;
            }

            // check if can commit
            if (!readyLedger.contains(requestBody) || commitLedger.contains(requestBody)) {
                System.out.println(String.format("Cannot commit %s %s", readyLedger, commitLedger));
                Node.sendErrorResponse(httpExchange);
                return;
            }

            // perform commit
            boolean commitSuccess = commitLedger.add(requestBody);
            assert commitSuccess;
            boolean readySuccess = readyLedger.remove(requestBody);
            assert readySuccess;
            System.out.println(String.format("Committed %s %s", readyLedger, commitLedger));

            // send okay response
            try {
                OutputStream outputStream = httpExchange.getResponseBody();
                httpExchange.sendResponseHeaders(200, 0);
                outputStream.flush();
                outputStream.close();
            } catch (Exception exception) {
                exception.printStackTrace();
                Node.sendErrorResponse(httpExchange);
            }
        }
    }

    private class AbortHttpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) {
            if (!httpExchange.getRequestMethod().equals("PUT")) {
                Node.sendErrorResponse(httpExchange);
                return;
            }

            // extract request body
            String requestBody = getRequestBody(httpExchange);
            if (requestBody == null) {
                Node.sendErrorResponse(httpExchange);
                return;
            }

            // abort commit
            readyLedger.remove(requestBody);  // do not check return value
            System.out.println(String.format("Aborted %s", requestBody));

            // send okay response
            try {
                OutputStream outputStream = httpExchange.getResponseBody();
                httpExchange.sendResponseHeaders(200, 0);
                outputStream.flush();
                outputStream.close();
            } catch (Exception exception) {
                exception.printStackTrace();
                Node.sendErrorResponse(httpExchange);
            }
        }
    }

    private static String getRequestBody(HttpExchange httpExchange) {
        try {
            InputStream requestBodyStream = httpExchange.getRequestBody();
            byte[] requestBodyBytes = requestBodyStream.readNBytes(100);

            return new String(requestBodyBytes);

        } catch (IOException exception) {
            exception.printStackTrace();
            return null;
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
