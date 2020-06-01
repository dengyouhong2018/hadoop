package com.ct.producer.consumer.dao;


import com.ct.common.bean.BaseDao;
import com.ct.common.constant.Names;
import com.ct.common.constant.ValueConstant;
import com.ct.producer.consumer.bean.Calllog;

import java.io.IOException;

/**
 * HBase数据访问对象
 */
public class HBaseDao extends BaseDao {

    public void init() throws IOException {
        setup();

        createNamespaceNX(Names.NAMESPACE.getValue());

        createTableXX(Names.TABLE.getValue(), ValueConstant.REGION_COUNT, Names.CF_CALLER.getValue());

        cleanup();

    }

    /**
     * 插入对象
     *
     * @param calllog
     */
    public void insertData(Calllog calllog) throws IOException, IllegalAccessException {

       String rowkey = genRegionNum(calllog.getCall1(), calllog.getCalltime()) + "_"
                        + calllog.getCall1() + calllog.getCalltime()
                        + calllog.getCall2() + calllog.getDuration();

        calllog.setRowkey(rowkey);

        putObj(calllog);
    }


    /**
     * 插入数据
     *
     * @param value
     */
    public void insertData(String value) throws IOException {

        //将通话日志保存到日志表中
        //Put put = new Put();

        // 1、获取通话日志数据

        // 2、创建数据对象

        /**
         * rowkey设计原则
         * 1、长度原则
         *      最大值64KB,推荐长度为 10 ~ 100 byte
         *      最好8的倍数，能短则短，rowkey如果太长会影响性能
         * 2、唯一原则
         *      rowkey应该具备唯一性
         * 3、散列原则
         *      1)盐值散列，不能使用时间戳直接作为rowkey
         *         在rowkey前增加随机数
         *      2)字符串反转：13123132345 13123135678
         *         电话号码
         *      3)计算分区号：hashMap
         */

        // rowkey = regionNum + call1 + time + call2 + duration

        //getRegionNum();

        // 3、保存数据


        //putData(Names.TABLE.getValue(),put);

    }

}
