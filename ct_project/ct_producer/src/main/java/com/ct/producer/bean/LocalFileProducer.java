package com.ct.producer.bean;


import com.ct.common.bean.DataIn;
import com.ct.common.bean.DataOut;
import com.ct.common.bean.Producer;

import java.io.IOException;

/**
 * 本地数据文件生产者
 */
public class LocalFileProducer implements Producer {

    private DataIn in;
    private DataOut out;
    private volatile Boolean flag = true;

    public void setIn(DataIn in) {
        this.in = in;
    }

    public void setOut(DataOut out) {
        this.out = out;
    }

    /**
     * 真正生产数据的方法
     */
    public void produce() {
        // 读取通讯录数据

        // 从通讯录中随机查找2个电话号码（主叫、被叫）

        // 生成随机通话时间

        // 生产通话记录

        // 将通话记录刷写到数据文件
    }

    /**
     * 关闭生产者
     */
    public void close() throws IOException {
        if(in != null){
            in.close();
        }

        if(out != null){
            out.close();
        }
    }
}
