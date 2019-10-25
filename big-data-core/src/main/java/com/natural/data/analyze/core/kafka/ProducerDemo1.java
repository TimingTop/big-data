package com.natural.data.analyze.core.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class ProducerDemo1 {

    public static void main(String[] args) throws InterruptedException {
        Properties p = new Properties();
        Random random = new Random();
        String topic = "topic-test-one";
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.104:9092,192.168.56.105:9092,192.168.56.106:9092");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(p);

        kafkaProducer.initTransactions();



        kafkaProducer.beginTransaction();
        for (int i = 0; i < 9; i++) {
            String msg = "Hello, " + random.nextInt(100);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);

            kafkaProducer.send(record);

            Thread.sleep(500);
        }

        kafkaProducer.commitTransaction();

        kafkaProducer.close();
    }
}
