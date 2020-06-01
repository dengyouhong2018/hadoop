package com.hadoop.hbase.mr1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 目标：将fruit表中的一部分数据，通过MR迁入到fruit_mr表中
 */
public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        // 构建Put对象
        Put put = new Put(key.get());

        // 遍历数据
        for (Cell cell : value.rawCells()) {
            byte[] qualifier = CellUtil.cloneQualifier(cell);

            if ("name".equals(Bytes.toString(qualifier))) {

                // 向该列cell加入到put对象中
                put.add(cell);

            }
        }

        // 写出去
        context.write(key, put);

    }
}
