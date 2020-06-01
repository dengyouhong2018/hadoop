package com.kafka.example.produce;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        Properties prop = new Properties();

        // Kafka服务端的主机名和端口号
        prop.put("bootstrap.servers", "hadoop101:9092");
        // 等待所有副本节点的应答级别
        //prop.put("acks", "all");
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        // 消息发送最大重试次数
        prop.put("retries", 1);
        // 一批消息处理大小
        prop.put("batch.size", 16384);
        // 提交延时
        prop.put("linger.ms", 1);
        // 发送缓存区内存大小
        prop.put("buffer.memory", 33554432);
        // KV序列化的类
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 关联自定已的分区类
        //prop.setProperty("partitioner.class", "com.kafka.example.customizePartitioner");

        KafkaProducer producer = new KafkaProducer<String,String>(prop);

        // 带回调函数的生产者
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord("first", String.valueOf(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println(metadata.partition() + "--" + metadata.offset());
                    } else {
                        System.out.println("发送失败");
                    }
                }
            });
        }

        producer.close();

    }

}
