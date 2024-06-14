package com.ksayers.twoPhaseCommit;

import java.util.HashMap;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;


public class CoordinatorNode {
    HashMap<String, String> nodeList = new HashMap<String, String>();
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

        CoordinatorNode node = new CoordinatorNode();
    }

    private class CoordinatorNodeHttpHandler implements HttpHandler {
        @Override    
        public void handle(HttpExchange httpExchange) {
            try {
                if (!httpExchange.getRequestMethod().equals("POST")) {
                    sendErrorResponse(httpExchange);
                    return;
                }

                sendErrorResponse(httpExchange);

                if (false) {
                    throw new IOException();
                }
            
            } catch (IOException exception) {
                exception.printStackTrace();
                sendErrorResponse(httpExchange);
                //httpExchange.getRequestMethod()
            }
        }

        private void sendErrorResponse(HttpExchange httpExchange) {
            System.err.println("Error response");
            OutputStream outputStream = httpExchange.getResponseBody();
            try {
                httpExchange.sendResponseHeaders(400, 0);
                outputStream.flush();
                outputStream.close();
            } catch (IOException exception) {
                exception.printStackTrace();
                System.err.println("Failed to send error response");
            }
        }
    }
}
