package com.ct.producer.consumer;

import com.ct.producer.consumer.bean.CalllogConsumer;

/**
 * 启动消费者
 * 使用kafka消费者获取Flume采集数据
 * 将数据存储到HBase中
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception {

        // 创建消费者
        CalllogConsumer consumer = new CalllogConsumer();

        // 消费数据
        consumer.consume();

        // 关闭资源
        consumer.close();

    }
}
