package com.ksayers.twoPhaseCommit;

import java.util.HashMap;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;
import org.apache.http.protocol.HttpRequestHandlerRegistry;


public class CoordinatorNode {
    HashMap<String, String> nodeList = new HashMap<String, String>();

    Thread serverThread = new Thread(() -> {
        System.out.println("Hello thread");
    });

    public CoordinatorNode() {
        serverThread.start();
    }
    
    static void sendReady() {
        System.out.println("Sending ready");
    }

    public static void main(String[] args) {
        System.out.println("main");

        CoordinatorNode node = new CoordinatorNode();
    }
}
