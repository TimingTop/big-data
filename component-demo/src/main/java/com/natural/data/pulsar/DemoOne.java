package com.natural.data.pulsar;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;

import java.util.concurrent.TimeUnit;

public class DemoOne {

    public static void main(String[] args) throws PulsarClientException {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://192.168.56.104:6650,192.168.56.105:6650,192.168.56.106:6650")
                .build();


        Producer<byte[]> producer = client.newProducer()
                .topic("topic-one")
                .create();
        producer.send("message one".getBytes());
        
        producer.close();
        producer.closeAsync()
                .thenRun(() -> System.out.println("Producer closed"))
                .exceptionally((ex) -> {
                   System.out.println("Failed to close producer: " + ex);
                    Throwable ex1 = ex;
                    return null;
                });

        Producer<String> producer1 = client.newProducer(Schema.STRING)
                    .topic("topic-two")
                    .create();
        producer1.send("message two");


        Producer<byte[]> producer2 = client.newProducer()
                .topic("my-topic")
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .sendTimeout(10, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .create();

        producer2.sendAsync("message three".getBytes())
                .thenAccept(msgId -> {
                   System.out.println("aa" + msgId);
                });


        Consumer consumer = client.newConsumer()
                .topic("topic-one")
                .subscriptionName("my-subscription")
                .subscribe();

//        while (true) {
//            Message msg = consumer.receive();
//
//
//            consumer.acknowledge(msg);
//        }

        client.newConsumer()
                .topic("my-topic")
                .subscriptionName("subscription")
                .ackTimeout(10, TimeUnit.SECONDS)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        consumer.receiveAsync();

//        Authentication tlsAuth = AuthenticationFactory
//                .create(AuthenticationTls.class.getName(), null);


        PulsarClient.builder()
                .enableTlsHostnameVerification(true)
                .serviceUrl("http://")
                ;













    }
}
