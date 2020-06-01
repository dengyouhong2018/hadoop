package com.kafka.example.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义计数拦截器
 */
public class CountInterceptor implements ProducerInterceptor<String, String> {

    int success; // 成功数
    int error; // 错误数

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        if (metadata != null) {
            success++;
        } else {
            error++;
        }

    }

    @Override
    public void close() {
        System.out.println("success" + success);
        System.out.println("error" + error);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
