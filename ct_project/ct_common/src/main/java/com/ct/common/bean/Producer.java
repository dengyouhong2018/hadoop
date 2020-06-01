package com.ct.common.bean;

import java.io.Closeable;

/**
 * 生产者接口
 */
public interface Producer extends Closeable {

    void setIn(DataIn in);

    void setOut(DataOut out);

    /**
     * 生产数据
     */
    void produce();
}
