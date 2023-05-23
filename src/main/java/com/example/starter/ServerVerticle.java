package com.example.starter;

import io.vertx.core.AbstractVerticle;

public class ServerVerticle extends AbstractVerticle {

      @Override
      // websocketサーバーを起動する
      public void start() throws Exception {
          System.out.println("ServerVerticle started!");
      }

      @Override
      public void stop() throws Exception {
          System.out.println("ServerVerticle stopped!");
      }
}
