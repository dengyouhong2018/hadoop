package com.kafka.example.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义时间拦截器
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {

        // 1、取出数据
        String value = record.value();

        // 2、创建一个新的ProduceRecord对象，并返回
        return new ProducerRecord(record.topic(), record.partition(), record.key(), System.currentTimeMillis()
                + "" + value);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
