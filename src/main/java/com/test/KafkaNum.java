package com.test;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;


// 计算 kafka topic新写入数据的个数
public class KafkaNum {

    public static void main(String[] args) {

        String topic = args[1];
        String groupId = args[0];

        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka35:9092");
        props.put("group.id", groupId);
        // 自动提交offset
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("auto.offset.reset","earliest");
        props.put("auto.offset.reset","latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        long count = 0L;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            if (records.count() > 0) {
                count += records.count();
                System.out.println(count);
            }
        }
    }
}

