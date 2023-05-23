package com.example.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.*;

public class MainVerticle extends AbstractVerticle {
  Map<String, String> config = new HashMap<>();
  KafkaConsumer<String, String> kafkaConsumer;
  KafkaProducer<String, String> kafkaProducer;
  KafkaAdminClient adminClient;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    config.put("bootstrap.servers", "192.168.86.141:9092");
    config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    config.put("group.id", "my_group");
    config.put("auto.offset.reset", "earliest");
    config.put("enable.auto.commit", "false");
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    config.put("acks", "1");
    // use consumer for interacting with Apache Kafka
    adminClient = KafkaAdminClient.create(vertx, config);
    kafkaConsumer = KafkaConsumer.create(vertx, config);
    kafkaProducer = KafkaProducer.create(vertx, config);

    adminClient.createTopics(Collections.singletonList(new NewTopic("test", 1, (short) 1)), done -> {
      if (done.succeeded()) {
        System.out.println("adminClient.createTopics: Topic created");
      } else {
        System.out.println("adminClient.createTopics: Topic creation failed " + done.cause());
      }
    });


    kafkaConsumer.handler(record -> {
      System.out.println("Processing key=" + record.key() + ",value=" + record.value() +
        ",partition=" + record.partition() + ",offset=" + record.offset());
    });
    kafkaConsumer.subscribe("test");



    vertx.createHttpServer().requestHandler(req -> {
      Long t = System.currentTimeMillis();
      KafkaProducerRecord<String, String> record =
        KafkaProducerRecord.create("test", "arrive", t.toString());
//      kafkaProducer.write(record);
      kafkaProducer.write(record, done -> {
        if (done.succeeded()) {
          System.out.println("kafkaProducer.write: done");
        } else {
          System.out.println("kafkaProducer.write: fail" + done.cause());
        }
      });

      req.response()
        .putHeader("content-type", "text/plain")
        .end("Hello from Vert.x!");
    }).listen(8888, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 8888");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }
}
