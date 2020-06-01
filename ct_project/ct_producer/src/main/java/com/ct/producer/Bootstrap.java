package com.ct.producer;

import com.ct.producer.bean.LocalFileProducer;
import com.ct.producer.io.LocalFileDataIn;
import com.ct.producer.io.LocalFileDataOut;

/**
 * 引导类，启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws Exception {
        // 构建生产者对象
        LocalFileProducer producer = new LocalFileProducer();

        producer.setIn(new LocalFileDataIn(""));
        producer.setOut(new LocalFileDataOut(""));

        // 生产数据
        producer.produce();

        // 关闭生产者对象
        producer.close();
    }
}
