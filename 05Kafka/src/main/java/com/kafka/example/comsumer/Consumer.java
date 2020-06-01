package com.kafka.example.comsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {

        Properties prop = new Properties();
        // kafka集群
        prop.put("bootstrap.servers", "hadoop101:9092");
        // 消费者组id
        prop.put("group.id", "test");
        // 设置是否自动提交offset
        prop.put("enable.auto.commit", "true");
        // 提交延迟（自动确认offset的时间间隔）
        prop.put("auto.commit.interval.ms", "1000");
        // KV的反序列化
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(prop);
        // 消费者订阅的topic，可同时订阅多个
        consumer.subscribe(Arrays.asList("first"));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                //System.out.println(record.partition() + "--" + record.offset() + "--" + record.value());
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

    }
}
