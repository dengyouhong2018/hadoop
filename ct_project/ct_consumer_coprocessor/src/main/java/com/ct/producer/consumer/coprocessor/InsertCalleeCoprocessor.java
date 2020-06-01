package com.ct.producer.consumer.coprocessor;

import com.ct.common.constant.Names;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 使用协处理器保存被叫用户的数据
 */
public class InsertCalleeCoprocessor extends BaseRegionObserver {

    /**
     * 保存主叫用户数据之后，由HBase自动保存被叫用户数据
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        // 获取表
        e.getEnvironment().getTable(TableName.valueOf(Names.TABLE.getValue()));

        // 主叫用户的rowkey
        String rowkey = Bytes.toString(put.getRow());


        String[] values = rowkey.split("_");

        // 保存数据


    }
}
