package com.ksayers.twoPhaseCommit;

import java.util.HashMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;

import org.json.JSONException;
import org.json.JSONObject;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;


public class CoordinatorNode {
    HashMap<String, InetSocketAddress> nodeList = new HashMap<String, InetSocketAddress>();
    ThreadPoolExecutor serverThreadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
    HttpServer server;

    public CoordinatorNode() throws IOException{
        server = HttpServer.create(new InetSocketAddress("localhost", 8001), 10);
        server.createContext("/test", new CoordinatorNodeHttpHandler());
        server.setExecutor(serverThreadPool);
        server.start();
    }
    
    static void sendReady() {
        System.out.println("Sending ready");
    }

    public static void main(String[] args) throws IOException {
        System.out.println("main");

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
                    sendErrorResponse(httpExchange);
                    return;
                }

                registerNode(httpExchange);
            
            } catch (IOException | JSONException exception) {
                exception.printStackTrace();
                sendErrorResponse(httpExchange);
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
                sendErrorResponse(httpExchange);
                return;
            }

            // input validation
            if (!requestJson.has("nodeId") || !requestJson.has("address") || !requestJson.has("port")) {
                sendErrorResponse(httpExchange);
                return;
            }

            // update node list
            nodeList.put(
                requestJson.getString("nodeId"),
                new InetSocketAddress(requestJson.getString("address"), requestJson.getInt("port"))
            );
            System.out.println(nodeList);

            // send okay response
            OutputStream outputStream = httpExchange.getResponseBody();
            httpExchange.sendResponseHeaders(200, 0);
            outputStream.flush();
            outputStream.close();
        }

        private void sendErrorResponse(HttpExchange httpExchange) {
            OutputStream outputStream = httpExchange.getResponseBody();
            try {
                httpExchange.sendResponseHeaders(400, 0);
                outputStream.flush();
                outputStream.close();
            } catch (IOException exception) {
                exception.printStackTrace();;
            }
        }
    }
}
