package com.ksayers.twoPhaseCommit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
import java.net.HttpURLConnection;


public class Node {
    InetSocketAddress coordinatorAddress = new InetSocketAddress("localhost", 8000);
    ArrayList<Integer> ledger;

    ThreadPoolExecutor serverThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    HttpServer server;

    String nodeId;
    InetSocketAddress address = null;

    public Node(String _nodeId, Integer port) throws Exception {
        nodeId = _nodeId;
        address = new InetSocketAddress("localhost", port);
        server = HttpServer.create(address, 5);
        server.createContext("/ready", new ReadyHttpHandler());
        server.setExecutor(serverThreadPool);
        server.start();

        // register with coordinator
        registerNode();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Must specify nodeId and port");
            return;
        }

        @SuppressWarnings("unused")
        Node node = new Node(args[0], Integer.parseInt(args[1]));
    }

    private void registerNode() throws Exception {
        // construct url
        URL url = new URI(String.format(
            "http://%s:%s/register",
            coordinatorAddress.getHostName(),
            coordinatorAddress.getPort()
        )).toURL();

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
        System.out.println(String.format("Sending register request to %s", url));
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Content-Length", Integer.toString(payload.getBytes().length));
        DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream());
        outputStream.writeBytes(payload);
        outputStream.close();

        // get response
        if (connection.getResponseCode() != 200) {
            System.err.println("Could not register with coordinator node");
            return;
        }

        System.out.println("Successfully registered with coordinator");
    }

    private class ReadyHttpHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange httpExchange) {

        }
    }
}
