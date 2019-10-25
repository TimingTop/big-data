package com.natural.data.analyze.core.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

// https://kafka.apache.org/23/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
// 去官方文档看
public class ConsumerDemo1 {
    public static void main(String[] args) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.104:9092,192.168.56.105:9092,192.168.56.106:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "aa_group_one");

        // ##############     自动 commit #########################
        // 自动commit
        p.put("enable.auto.commit", "true");
        // 自动 commit 的时间间隔
        p.put("auto.commit.interval.ms", "1000");
        // #################   手工 commit ########################
        p.put("enable.auto.commit", "false");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(p);
        // 订阅 topic
        kafkaConsumer.subscribe(Collections.singletonList("topic-test-one"));

        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

//        while(true) {
//            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
//            for (ConsumerRecord<String, String> record: records) {
//                record.topic();
//                record.offset();
//                record.value();
//                buffer.add(record);
//            }
//            if (buffer.size() >= minBatchSize) {
//                // 处理 所有的 record， 用于手工 commit ，这个 commit 会 commit 全部 partition，为了不丢数据，需要更细的commit 粒度
//                kafkaConsumer.commitAsync();
//                buffer.clear();
//            }
//        }

        // 根据每个 partition  去 commit offset
        try {
            while(true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (TopicPartition partition: records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        record.value();
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
